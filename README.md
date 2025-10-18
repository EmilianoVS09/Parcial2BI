#Proyecto ETL – Airbnb Ciudad de México

Este proyecto implementa un proceso ETL (Extracción, Transformación y Carga) utilizando los datasets de Airbnb Ciudad de México.
El objetivo es construir un flujo automatizado que permita limpiar, transformar y cargar los datos desde una fuente MongoDB hacia una base de datos SQLite y un archivo Excel (.xlsx) para análisis posteriores o visualización en herramientas como Power BI o Tableau.

El proceso registra todos los eventos mediante logs automáticos, garantizando trazabilidad y control de calidad de los datos.

#Instrucciones de Instalación
Crear el entorno virtual

En la raíz del proyecto, abre una terminal y ejecuta:
python -m venv venv

Luego actívalo:
En Windows:
venv\Scripts\activate

En Linux/Mac:
source venv/bin/activate

Instalar las dependencias
Ejecuta:
pip install -r requirements.txt


#Contenido recomendado del archivo requirements.txt:
pandas
openpyxl
pymongo


(SQLite y logging vienen integrados con Python, no necesitan instalación)
Ejecutar el proceso ETL
Una vez activado el entorno, corre el flujo principal:
python main.py
Esto realizará:
Extracción desde MongoDB
Transformación y limpieza de datos
Carga en SQLite (data/airbnb.db) y Excel (data/airbnb_limpio.xlsx)
Generación de logs (logs/etl.log)

| Nombre                       | Rol / Responsabilidad                                                                                                            |
| -----------------------------| ---------------------------------------------------------------------------------------------------------------------------------|
| **Emiliano Velez suarez**    | Desarrollo de la clase `Transformacion` , normalización de datos, desarrollo de la clase `Extraccion` y conexión con MongoDB     |
| **Yenifer Gonzalez Quirama** | Implementación de la clase `Carga` , manejo de logs , documentación, pruebas y README.md                                         |

#Ejemplo de Ejecución del ETL:

from extraccion import Extraccion
from transformacion import Transformacion
from carga import Carga

Extracción
ext = Extraccion(db_name="bi_mx")
ext.conectar()
df_calendar = ext.obtener_coleccion("calendar")
ext.cerrar()

Transformación
trans = Transformacion(df_calendar)
df_limpio = trans.ejecutar_transformaciones()

Carga
carga = Carga(df_limpio, sqlite_path="data/airbnb.db")
carga.ejecutar_carga_completa(table_name="calendar_limpio", excel_path="data/airbnb_limpio.xlsx")

