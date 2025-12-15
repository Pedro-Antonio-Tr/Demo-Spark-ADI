import sys
import time
import random
from pyspark.sql import SparkSession

n = 1

def limpiar_pantalla():
    print("\n" * 2)

def pausa_explicativa(mensaje):
    global n
    print(f"\n>>> PASO {n}: {mensaje}")
    input("    [Presiona ENTER para continuar...]")
    print("-" * 60)
    n += 1

# --- PASO 1: INICIO DEL DRIVER Y SPARK UI ---
print("============================================================")
print("   DEMOSTRACION TECNICA: APACHE SPARK (CLUSTER LOCAL)   ")
print("============================================================")

print("... Inicializando SparkSession y JVM ...")

spark = SparkSession.builder \
    .appName("DemoADI_Presentacion") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("[INFO] Driver iniciado correctamente.")
print(f"[INFO] Spark UI disponible en: {spark.sparkContext.uiWebUrl}")
print("       (Abre este enlace en tu navegador para ver los DAGs y Executors)")

pausa_explicativa("Infraestructura iniciada. Abre el navegador.")


# --- PASO 2: INGESTA Y VISUALIZACION DE PARTICIONES ---
# ETIQUETA PARA LA UI: Todo lo que pase aquí se llamará "Paso 2"
spark.sparkContext.setJobGroup("Paso 2", "Ingesta y Visualizacion de Particiones")

data_pequena = [
    ("Stranger Things", "Ciencia Ficcion"), ("The Crown", "Drama"),
    ("La Casa de Papel", "Accion"), ("Black Mirror", "Ciencia Ficcion"),
    ("Narcos", "Drama"), ("Breaking Bad", "Drama"),
    ("Rick and Morty", "Animacion"), ("Dark", "Ciencia Ficcion")
]

# Forzamos 4 particiones
rdd_distribuido = spark.sparkContext.parallelize(data_pequena, numSlices=4)

print(f"[INFO] Dataset cargado. Numero de particiones (nodos virtuales): {rdd_distribuido.getNumPartitions()}")

datos_por_nodo = rdd_distribuido.glom().collect() # Esto generará un Job en la UI con el nombre nuevo

print("\n[VISUALIZACION DE ALMACENAMIENTO DISTRIBUIDO]")
for i, particion in enumerate(datos_por_nodo):
    print(f"  -> Nodo/Particion {i}: Contiene {len(particion)} elementos: {particion}")

pausa_explicativa("Distribucion fisica de datos (RDDs) observada.")


# --- PASO 3: TRANSFORMACIONES (LAZY EVALUATION) ---
# ETIQUETA PARA LA UI
spark.sparkContext.setJobGroup("Paso 3", "Definicion de Transformaciones (Lazy)")

df = rdd_distribuido.toDF(["Titulo", "Genero"])
print("... Declarando transformacion: Filtrar por 'Ciencia Ficcion' ...")
ciencia_ficcion_df = df.filter(df.Genero == "Ciencia Ficcion")

print("[INFO] Transformacion registrada en el DAG. Ningun dato procesado aun (Lazy).")
# Nota: Como aquí no hay acción, no saldrá nada nuevo en la lista de Jobs,
# pero si abres la pestaña "SQL/DataFrame" verás la operación registrada.

pausa_explicativa("Plan de ejecucion creado. Procediendo a la Accion.")


# --- PASO 4: ACCION Y EJECUCION ---
# ETIQUETA PARA LA UI
spark.sparkContext.setJobGroup("Paso 4", "Ejecucion de Accion .show()")

print("... Ejecutando accion .show() ...")
ciencia_ficcion_df.show() # Esto disparará un Job con la etiqueta "Paso 4"

pausa_explicativa("Accion completada. Resultados devueltos al Driver.")


# --- PASO 5: PRUEBA DE RENDIMIENTO (CACHE VS DISCO) ---
print("\n[PRUEBA DE RENDIMIENTO BIG DATA]")
print("... Generando dataset masivo (5.000.000 registros) ...")

df_masivo = spark.range(0, 5000000)

# ETIQUETA PARA LA UI: Prueba SIN Cache
spark.sparkContext.setJobGroup("Paso 5.1", "Rendimiento SIN Cache")

print("... Ejecutando conteo SIN memoria cache ...")
inicio = time.time()
conteo = df_masivo.count() 
fin = time.time()
print(f"[RESULTADO] Tiempo sin cache: {fin - inicio:.4f} segundos")

# ETIQUETA PARA LA UI: Carga en Memoria
spark.sparkContext.setJobGroup("Paso 5.2", "Carga en Memoria (.cache)")

print("... Guardando dataset en Memoria RAM (.cache) ...")
df_masivo.cache() 
df_masivo.count() 

# ETIQUETA PARA LA UI: Prueba CON Cache
spark.sparkContext.setJobGroup("Paso 5.3", "Rendimiento CON Cache")

print("... Ejecutando conteo CON memoria cache ...")
inicio = time.time()
conteo = df_masivo.count() 
fin = time.time()
print(f"[RESULTADO] Tiempo con cache: {fin - inicio:.4f} segundos")

pausa_explicativa("Comparativa de rendimiento finalizada.")


# --- CIERRE ---
print("... Deteniendo SparkContext ...")
spark.stop()
print("[INFO] Sesion finalizada.")