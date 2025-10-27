# main_etl.py
from extraction import Extraction
from transformacion import Transformation
from carga import Carga
from logs import Logs
from pymongo import MongoClient

# --------- Configuración ---------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME   = "bi_mx"

def contar_mongo(coleccion: str) -> int:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    n = db[coleccion].count_documents({})
    client.close()
    return n

def main():
    logs = Logs()
    logs.log("=== INICIO ETL (main_etl.py) ===", "info")

    # --------- 1) EXTRACCIÓN ---------
    ex = Extraction()
    db = ex.mongodb_connection(MONGO_URI, DB_NAME)

    # Conteos esperados en origen (para verificación)
    expected = {
        "listings": contar_mongo("listings_mx"),
        "calendar": contar_mongo("calendar_mx"),
        "reviews":  contar_mongo("reviews_mx")
    }
    logs.log(f"[Origen Mongo] Esperados -> {expected}", "info")

    df_listings = ex.load_mongodb_datasets(db, "listings_mx")
    df_calendar = ex.load_mongodb_datasets(db, "calendar_mx")
    df_reviews  = ex.load_mongodb_datasets(db, "reviews_mx")
    ex.close_mongodb_connection(MONGO_URI)

    # --------- 2) TRANSFORMACIÓN ---------
    tf = Transformation(df_listings,df_calendar,df_reviews)

    # pipeline típico (ajusta al nombre real de tus métodos)
    tf.run()

    # DataFrame final para carga (ajusta al nombre que tu clase expone)
    # Ej: tf.flat_sheet o tf.listings_clean; usa el que defina tu clase como “listo para carga”
    df_final = getattr(tf, "flat_sheet", None)
    if df_final is None:
        logs.log("No se encontró el DataFrame final listo para carga en Transformacion.", "error")
        raise RuntimeError("Transformacion no expuso df final (e.g., tf.flat_sheet).")

    logs.log(f"[Transformacion] DF final con {len(df_final)} filas y {len(df_final.columns)} columnas.", "info")

    # --------- 3) CARGA ---------
    cg = Carga(df_final)
    # a) Carga completa con SQLite + Excel siempre; SQL Server solo si pasas parámetros
    cg.ejecutar_carga_completa(
        table_name="airbnb_limpio",
        excel_path="data/airbnb_limpio.xlsx",
    )

    logs.log("=== FIN ETL (main_etl.py) ===", "info")

if __name__ == "__main__":
    main()
