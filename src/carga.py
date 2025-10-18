import sqlite3
import pandas as pd
import logging
import os


class Carga:
    """
    Clase para realizar la carga de datos transformados en SQLite y Excel.
    """

    def __init__(self, df, sqlite_path="data/airbnb.db"):
        """
        Inicializa la clase con el DataFrame transformado y la ruta de la base SQLite.
        """
        self.df = df.copy()
        self.sqlite_path = sqlite_path
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        logging.info(f"Carga inicializada con {len(self.df)} registros.")

    def _connect_sqlite(self):
        """
        Crea la conexión a la base de datos SQLite.
        """
        try:
            conn = sqlite3.connect(self.sqlite_path)
            logging.info(f"Conectado a SQLite: {self.sqlite_path}")
            return conn
        except Exception as e:
            logging.error(f"Error al conectar con SQLite: {e}")
            raise

    def insertar_en_sqlite(self, table_name="airbnb_limpio", if_exists="replace"):
        """
        Inserta el DataFrame transformado en una tabla SQLite.
  
        """
        conn = self._connect_sqlite()
        try:
            self.df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            conn.commit()
            logging.info(f"Datos insertados en la tabla '{table_name}' correctamente.")
        except Exception as e:
            logging.error(f"Error al insertar en SQLite: {e}")
        finally:
            conn.close()

    def verificar_carga_sqlite(self, table_name="airbnb_limpio"):
        """
        Verifica la cantidad de registros cargados en la tabla SQLite.
        """
        conn = self._connect_sqlite()
        try:
            query = f"SELECT COUNT(*) FROM {table_name};"
            count = conn.execute(query).fetchone()[0]
            logging.info(f"Verificación: {count} registros encontrados en '{table_name}'.")
            return count
        except Exception as e:
            logging.error(f"Error al verificar registros en SQLite: {e}")
        finally:
            conn.close()

    def exportar_a_excel(self, output_path="data/airbnb_limpio.xlsx"):
        """
        Exporta el DataFrame a un archivo Excel.
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            self.df.to_excel(output_path, index=False)
            logging.info(f"Datos exportados correctamente a {output_path}")
        except Exception as e:
            logging.error(f"Error al exportar a Excel: {e}")
            raise

    def ejecutar_carga_completa(self, table_name="airbnb_limpio", excel_path="data/airbnb_limpio.xlsx"):
        """
        Ejecuta todas las tareas de carga y verificación.
        """
        logging.info("=== INICIO DE CARGA DE DATOS ===")
        self.insertar_en_sqlite(table_name)
        registros_sql = self.verificar_carga_sqlite(table_name)
        if registros_sql != len(self.df):
            logging.warning("La cantidad de registros en SQLite no coincide con el DataFrame original.")
        self.exportar_a_excel(excel_path)
        logging.info("=== FIN DE CARGA DE DATOS ===")
