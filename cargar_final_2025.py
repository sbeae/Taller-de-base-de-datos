import pandas as pd
import redis
from pymongo import MongoClient
import json
import sys

print("🚀 INICIANDO CARGA MAESTRA (DATOS 2025)...")

# =============================================================================
# 1. CONEXIÓN Y LIMPIEZA
# =============================================================================
try:
    # Conectamos a Redis
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    r.flushall() # Borramos datos viejos o inventados
    print(" -> Redis limpio.")

    # Conectamos a MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['crimenes_db']
    collection = db['delitos']
    collection.drop() # Borramos historial viejo
    print(" -> MongoDB limpio.")
    
except Exception as e:
    print(f"❌ Error de conexión: {e}")
    sys.exit()

# =============================================================================
# 2. PROCESAR POBLACIÓN 2025 (Tu nuevo archivo)
# =============================================================================
print("📂 Leyendo 'poblacion.csv' (Proyecciones INE)...")

try:
    # Leemos con separador de punto y coma ';'
    path_archivo = r'C:\Users\sebas\OneDrive\Desktop\Final base de datos\poblacion.csv'

    df_pob = pd.read_csv(path_archivo, sep=';', encoding='utf-8')
    
    # Filtramos solo el año 2025
    df_2025 = df_pob[df_pob['año'] == 2025]
    
    # Creamos un "Diccionario Rápido" { codigo_comuna : población }
    # Esto nos permite buscar la población de cualquier comuna en 0.0001 segundos
    dict_poblacion = pd.Series(df_2025['población'].values, index=df_2025['cut_comuna']).to_dict()
    
    print(f" -> ¡Éxito! Se cargaron datos de población 2025 para {len(dict_poblacion)} comunas.")
    print(f"    (Ejemplo: Iquique tiene {dict_poblacion.get(1101, 'No encontrado')} habitantes)")

except Exception as e:
    print(f"❌ Error leyendo el archivo de población: {e}")
    sys.exit()

# =============================================================================
# 3. CARGAR REDIS (La Fusión: Comuna + Región + Población 2025)
# =============================================================================
print("📂 Procesando archivo de Delitos 'output.csv'...")
df_delitos = pd.read_csv('output.csv')

# Obtenemos la lista única de comunas que existen en tus delitos
df_geo = df_delitos[['cut_comuna', 'comuna', 'region']].drop_duplicates()

print(f" -> Cruzando datos y subiendo a Redis...")

contador = 0
for index, row in df_geo.iterrows():
    codigo = int(row['cut_comuna'])
    
    # Buscamos la población 2025 en nuestro diccionario
    # Si por alguna razón no está (comuna nueva), ponemos 0.
    pob_real = dict_poblacion.get(codigo, 0)
    
    # Creamos el objeto JSON para Redis
    # ensure_ascii=False permite que los tildes se guarden bien (Tarapacá)
    datos = json.dumps({
        "comuna": row['comuna'],
        "region": row['region'],
        "poblacion": int(pob_real),
        "anio_poblacion": 2025
    }, ensure_ascii=False)
    
    r.set(str(codigo), datos)
    contador += 1

print(f" -> {contador} comunas actualizadas en Redis.")

# =============================================================================
# 4. CARGAR MONGODB (El Historial de Delitos)
# =============================================================================
print(" -> Subiendo historial delictivo a MongoDB...")

# Seleccionamos solo las columnas necesarias
df_mongo = df_delitos[['fecha', 'delito', 'delito_n', 'cut_comuna']]
registros = df_mongo.to_dict(orient='records')

collection.insert_many(registros)

print("\n✅ ¡PROCESO FINALIZADO CON ÉXITO!")
print(" -> Redis: Catálogo Geográfico + Población 2025 (Real)")
print(" -> MongoDB: Historial de Delitos (2018-2025)")