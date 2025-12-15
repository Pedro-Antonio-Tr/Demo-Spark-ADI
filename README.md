# Demo: Procesamiento de Big Data con Apache Spark (PySpark)

Este proyecto es una demostración práctica para la asignatura de **Aplicaciones Distribuidas en Internet**. Muestra el funcionamiento de RDDs, Transformaciones y Acciones utilizando Apache Spark en modo local.

## Requisitos Previos

Para ejecutar este script necesitas tener instalado:

1.  **Python 3.x**: [Descargar aquí](https://www.python.org/downloads/)
2.  **Java (JDK 8, 11 o 17)**: Spark corre sobre la JVM (Java Virtual Machine).
    * *Windows/Mac/Linux*: Asegúrate de tener Java instalado y configurado en las variables de entorno (`JAVA_HOME`).
    * Prueba: Ejecuta `java -version` en tu terminal. Si responde, estás listo.

## Instalación

1. Clona este repositorio o descarga los archivos.
2. Instala la librería `pyspark` usando pip en un entorno virtual:

```
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark
```

## Ejecución

```
python demo_spark.py
```

Sigue las instrucciones en pantalla (pulsa ENTER para avanzar en la demostración).