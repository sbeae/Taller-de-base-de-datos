import pandas as pd

def parquet_a_csv(archivo_parquet, archivo_salida):
    try:
        # 1. Leer el archivo Parquet
        print(f"📖 Leyendo {archivo_parquet}...")
        df = pd.read_parquet(archivo_parquet, engine='pyarrow')

        # 2. Guardar como CSV
        # index=False evita que se guarde el índice numérico de pandas (0, 1, 2...)
        # encoding='utf-8-sig' es RECOMENDADO si tienes tildes o Ñ (datos de Chile) para que Excel lo lea bien
        df.to_csv(archivo_salida, index=False, sep=',', encoding='utf-8-sig')
        
        print(f"✅ Conversión exitosa! Guardado en: {archivo_salida}")
        print(f"📊 Filas procesadas: {len(df)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

# --- Ejecución ---
if __name__ == "__main__":
    # Cambia estos nombres por tus archivos reales
    input_file = "tus_datoscead_delincuencia_chile (1).parquet" 
    output_file = "output.csv"
    
    parquet_a_csv(input_file, output_file)