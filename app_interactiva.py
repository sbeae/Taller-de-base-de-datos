import redis
from pymongo import MongoClient
import json
import unicodedata
import sys
import os
import time

# =============================================================================
# 0. CONFIGURACIÓN GLOBAL
# =============================================================================
ANIO_ANALISIS = "2024"

# =============================================================================
# 1. CONFIGURACIÓN Y CONEXIÓN
# =============================================================================
def conectar_db():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        client = MongoClient('mongodb://localhost:27017/')
        db = client['crimenes_db']
        collection = db['delitos']

        r.ping()
        client.server_info()

        return r, collection
    except Exception as e:
        print(f"\nError de conexión: {e}")
        print("Verifique que Redis y MongoDB estén en ejecución.")
        sys.exit()

r, collection = conectar_db()

# =============================================================================
# 2. FUNCIONES UTILITARIAS
# =============================================================================
def normalizar(texto):
    if not isinstance(texto, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto.lower())
                   if unicodedata.category(c) != 'Mn')

def limpiar_pantalla():
    os.system('cls' if os.name == 'nt' else 'clear')

def obtener_gravedad(tasa):
    if tasa > 2000: return "CRÍTICO"
    if tasa > 1000: return "ALTO"
    if tasa > 500:  return "MEDIO"
    return "BAJO"

# =============================================================================
# 3. CONSULTA POR COMUNA O REGIÓN
# =============================================================================
def ejecutar_consulta(tipo_ubicacion):
    limpiar_pantalla()

    titulo = "Comuna" if tipo_ubicacion == "comuna" else "Región"
    print(f"\n--- Análisis por {titulo} (Periodo: {ANIO_ANALISIS}) ---\n")

    nombre_lugar = input(f"Ingrese nombre de la {titulo.lower()}: ")
    delito_buscado = input("Ingrese delito (o escriba 'todos'): ")

    lugar_clean = normalizar(nombre_lugar)
    delito_clean = normalizar(delito_buscado)

    print("\nObteniendo datos demográficos...")

    cuts_encontrados = []
    poblacion_total = 0
    nombre_oficial = nombre_lugar

    for key in r.keys('*'):
        try:
            data = json.loads(r.get(key))
            campo = data.get(tipo_ubicacion, "")

            if lugar_clean in normalizar(campo):
                cuts_encontrados.append(int(key))
                poblacion_total += int(data.get('poblacion', 0))
                nombre_oficial = campo
        except:
            continue

    if not cuts_encontrados:
        print("No se encontró la ubicación indicada.")
        input("Presione Enter para volver...")
        return

    print("Consultando información delictual...")

    match_stage = {"cut_comuna": {"$in": cuts_encontrados}}
    match_stage["fecha"] = {"$regex": ANIO_ANALISIS}

    if delito_clean != "todos":
        match_stage["delito"] = {"$regex": delito_buscado, "$options": "i"}

    pipeline = [
        {"$match": match_stage},
        {"$group": {"_id": None, "total_delitos": {"$sum": "$delito_n"}}}
    ]

    resultado = list(collection.aggregate(pipeline))
    total_crimenes = resultado[0]['total_delitos'] if resultado else 0

    tasa = (total_crimenes / poblacion_total) * 100000 if poblacion_total > 0 else 0

    print("\n" + "="*50)
    print("REPORTE DE ANÁLISIS")
    print("="*50)
    print(f"Periodo:            {ANIO_ANALISIS}")
    print(f"Ubicación:          {nombre_oficial}")
    print(f"Población:          {poblacion_total:,} habitantes")
    print("-" * 50)
    print(f"Delito analizado:   {delito_buscado}")
    print(f"Total de casos:     {total_crimenes}")
    print("-" * 50)
    print(f"Tasa anual x 100k:  {tasa:.2f}")
    print(f"Nivel estimado:     {obtener_gravedad(tasa)}")
    print("="*50)

    input("\nPresione Enter para continuar...")

# =============================================================================
# 4. RANKING NACIONAL POR COMUNAS
# =============================================================================
def generar_top_peligrosidad():
    limpiar_pantalla()
    print(f"\nGenerando ranking nacional ({ANIO_ANALISIS}). Esto puede tardar unos segundos...\n")

    ranking = []
    keys = r.keys('*')
    total_keys = len(keys)
    procesados = 0

    for key in keys:
        procesados += 1
        if procesados % 50 == 0:
            print(f"Procesando {procesados}/{total_keys}...", end="\r")

        try:
            data_redis = json.loads(r.get(key))
            nombre = data_redis.get('comuna')
            poblacion = int(data_redis.get('poblacion', 0))
            cut = int(key)

            if poblacion < 5000:
                continue

            delitos_agg = collection.aggregate([
                {
                    "$match": {
                        "cut_comuna": cut,
                        "fecha": {"$regex": ANIO_ANALISIS}
                    }
                },
                {"$group": {"_id": None, "count": {"$sum": "$delito_n"}}}
            ])

            delitos_lista = list(delitos_agg)
            total = delitos_lista[0]['count'] if delitos_lista else 0

            if total > 0:
                tasa = (total / poblacion) * 100000
                ranking.append({
                    "comuna": nombre,
                    "tasa": tasa,
                    "total": total,
                    "poblacion": poblacion
                })
        except:
            continue

    ranking_ordenado = sorted(ranking, key=lambda x: x['tasa'], reverse=True)[:10]

    print("\n" + "="*60)
    print(f"TOP 10 COMUNAS MÁS PELIGROSAS ({ANIO_ANALISIS})")
    print("(Normalizado por población - Tasa Anual)")
    print("="*60)
    print(f"{'#':<3} {'Comuna':<20} {'Tasa x 100k':<15} {'Casos':<10}")
    print("-" * 60)

    for i, item in enumerate(ranking_ordenado, 1):
        print(f"{i:<3} {item['comuna']:<20} {item['tasa']:<15.2f} {item['total']:<10}")

    print("-" * 60)
    print("* No se incluyen comunas con menos de 5.000 habitantes.")
    input("\nPresione Enter para volver...")

# =============================================================================
# 5. NUEVO: RANKING NACIONAL POR REGIONES (TOP 5)
# =============================================================================
def generar_top_regiones():
    limpiar_pantalla()
    print(f"\nGenerando ranking regional ({ANIO_ANALISIS}). Esto puede tardar unos segundos...\n")

    regiones = {}  # { "Nombre región": {"poblacion": X, "delitos": Y} }

    keys = r.keys('*')

    for key in keys:
        try:
            data = json.loads(r.get(key))

            region = data.get("region")
            poblacion = int(data.get("poblacion", 0))
            cut = int(key)

            if not region:
                continue

            if region not in regiones:
                regiones[region] = {"poblacion": 0, "delitos": 0}

            regiones[region]["poblacion"] += poblacion

            # delitos por comuna → suman a la región
            delitos_agg = collection.aggregate([
                {
                    "$match": {
                        "cut_comuna": cut,
                        "fecha": {"$regex": ANIO_ANALISIS}
                    }
                },
                {"$group": {"_id": None, "count": {"$sum": "$delito_n"}}}
            ])

            lista = list(delitos_agg)
            total = lista[0]["count"] if lista else 0
            regiones[region]["delitos"] += total

        except:
            continue

    ranking = []
    for region, datos in regiones.items():
        if datos["poblacion"] == 0:
            continue
        tasa = (datos["delitos"] / datos["poblacion"]) * 100000
        ranking.append({
            "region": region,
            "tasa": tasa,
            "delitos": datos["delitos"]
        })

    top5 = sorted(ranking, key=lambda x: x["tasa"], reverse=True)[:5]

    print("\n" + "="*60)
    print(f"TOP 5 REGIONES MÁS PELIGROSAS ({ANIO_ANALISIS})")
    print("="*60)
    print(f"{'#':<3} {'Región':<25} {'Tasa x 100k':<15} {'Casos':<10}")
    print("-" * 60)

    for i, item in enumerate(top5, 1):
        print(f"{i:<3} {item['region']:<25} {item['tasa']:<15.2f} {item['delitos']:<10}")

    print("-" * 60)
    input("\nPresione Enter para volver...")

# =============================================================================
# 6. MENÚ PRINCIPAL
# =============================================================================
def main():
    while True:
        limpiar_pantalla()
        print(f"""
=========================================
 SISTEMA DE ANÁLISIS 
 Periodo de Análisis: {ANIO_ANALISIS}
=========================================
1. Consultar por comuna
2. Consultar por región
3. Ver ranking nacional (Top 10 comunas)
4. Ver ranking nacional de regiones (Top 5)
5. Salir
""")
        opcion = input("Seleccione una opción: ")

        if opcion == '1':
            ejecutar_consulta("comuna")
        elif opcion == '2':
            ejecutar_consulta("region")
        elif opcion == '3':
            generar_top_peligrosidad()
        elif opcion == '4':
            generar_top_regiones()
        elif opcion == '5':
            break

if __name__ == "__main__":
    main()
