import matplotlib.pyplot as plt
import redis
from pymongo import MongoClient
import json
import numpy as np

# Configuración
ANIO = "2024"  # Ajusta al año que tengas en tus datos

# -------------------------
# 1. CONEXIONES
# -------------------------
try:
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    client = MongoClient("mongodb://localhost:27017/")
    db = client["crimenes_db"]
    collection = db["delitos"]
    r.ping()
except Exception as e:
    print(f"Error de conexión: {e}")
    exit()

# -------------------------
# 2. PROCESAMIENTO DE DATOS
# -------------------------
print("Calculando Top 5 Regiones más peligrosas...")

datos_regionales = {}
keys = r.keys('*')

# A. Agrupar Población y CUTs por Región desde Redis
for key in keys:
    try:
        data = json.loads(r.get(key))
        region = data.get('region')
        pob = int(data.get('poblacion', 0))
        
        if pob == 0: continue
        
        if region not in datos_regionales:
            datos_regionales[region] = {'pob': 0, 'cuts': []}
        
        datos_regionales[region]['pob'] += pob
        datos_regionales[region]['cuts'].append(int(key))
    except: continue

# B. Consultar Delitos en Mongo y Calcular Tasas
ranking_final = []

for region, info in datos_regionales.items():
    cuts = info['cuts']
    poblacion = info['pob']
    
    # Sumar delitos de toda la región
    pipeline = [
        {"$match": {"cut_comuna": {"$in": cuts}, "fecha": {"$regex": ANIO}}},
        {"$group": {"_id": None, "total": {"$sum": "$delito_n"}}}
    ]
    
    res = list(collection.aggregate(pipeline))
    delitos = res[0]['total'] if res else 0
    
    if poblacion > 0:
        tasa = (delitos / poblacion) * 100000
        ranking_final.append({
            "region": region,
            "poblacion": poblacion,
            "delitos": delitos,
            "tasa": tasa
        })

# C. Ordenar por TASA (La más peligrosa primero)
top_5 = sorted(ranking_final, key=lambda x: x['tasa'], reverse=True)[:5]

# -------------------------
# 3. PREPARAR GRÁFICO
# -------------------------
regiones = [d['region'] for d in top_5]
poblaciones = [d['poblacion'] for d in top_5]
delitos = [d['delitos'] for d in top_5]
tasas = [d['tasa'] for d in top_5]

x = np.arange(len(regiones))  # Posiciones en eje X
width = 0.35  # Ancho de las barras

fig, ax1 = plt.subplots(figsize=(14, 7))

plt.title(f"TOP 5 REGIONES MÁS PELIGROSAS ({ANIO})\n(Comparativa: Población vs Delitos vs Tasa Real)", fontsize=14, pad=20)

# --- BARRAS (EJE IZQUIERDO) ---
# Barra 1: Población (Gris)
rects1 = ax1.bar(x - width/2, poblaciones, width, label='Población Total', color='#bdc3c7', alpha=0.7)
# Barra 2: Delitos (Azul)
rects2 = ax1.bar(x + width/2, delitos, width, label='Total Delitos', color='#2980b9')

# Configuración Eje Izquierdo
ax1.set_ylabel('Cantidad de Personas / Delitos', fontsize=12, fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(regiones, rotation=15, ha="right")
ax1.legend(loc='upper left')

# Formatear Eje Y para que no use notación científica (opcional, números grandes)
ax1.ticklabel_format(style='plain', axis='y')

# Función para poner etiquetas encima de las barras
def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax1.annotate(f'{int(height):,}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8, rotation=90)

autolabel(rects1) # Etiquetas Población
autolabel(rects2) # Etiquetas Delitos

# --- LÍNEA (EJE DERECHO) ---
ax2 = ax1.twinx()
linea = ax2.plot(x, tasas, color='#c0392b', marker='o', linewidth=3, markersize=10, label='Tasa (x100k hab)')
ax2.set_ylabel('Tasa de Peligrosidad (x100k hab)', color='#c0392b', fontsize=12, fontweight='bold')
ax2.tick_params(axis='y', labelcolor='#c0392b')

# Etiquetas para la línea roja
for i, v in enumerate(tasas):
    ax2.text(i, v + (max(tasas)*0.03), f"{v:.1f}", color='#c0392b', fontweight='bold', ha='center', fontsize=11, backgroundcolor='white')

# Leyenda combinada (opcional, aunque ya están separadas por eje)
# ax1 tiene su leyenda a la izquierda, la línea roja se explica sola por el color y eje derecho.

plt.tight_layout()
print("✅ Gráfico generado.")
plt.show()