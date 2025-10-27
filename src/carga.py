import sqlite3
import pandas as pd
import os
from logs import Logs


class Carga:
    """
    Clase para realizar la carga de datos transformados en SQLite y Excel.
    """

    def __init__(self, df, sqlite_path="data/airbnb.db"):
        """
        Inicializa la clase con el DataFrame transformado y la ruta de la base SQLite.
        """
        self.df = df.copy()
        self.logs = Logs()
        self.sqlite_path = sqlite_path
        os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
        self.logs.log(f"Carga inicializada con {len(self.df)} registros.", "info")

    def _connect_sqlite(self):
        """
        Crea la conexión a la base de datos SQLite.
        """
        try:
            conn = sqlite3.connect(self.sqlite_path)
            self.logs.log(f"Conectado a SQLite: {self.sqlite_path}","info")
            return conn
        except Exception as e:
            self.logs.log(f"Error al conectar con SQLite: {e}","error")
            raise

    def insertar_en_sqlite(self, table_name="airbnb_limpio", if_exists="replace"):
        """
        Inserta el DataFrame transformado en una tabla SQLite.
  
        """
        conn = self._connect_sqlite()
        try:
            self.df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            conn.commit()
            self.logs.log(f"Datos insertados en la tabla '{table_name}' correctamente.","info")
        except Exception as e:
            self.logs.log(f"Error al insertar en SQLite: {e}","error")
            raise
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
            self.logs.log(f"Verificación: {count} registros encontrados en '{table_name}'.","info")
            return count
        except Exception as e:
            self.logs.log(f"Error al verificar registros en SQLite: {e}","error")
            raise
        finally:
            conn.close()

    def exportar_a_excel_particionado(self, df=None, base_path="data/airbnb_limpio", max_rows_per_file=200_000, truncate_text_cols=True, max_text_len=500):
        """
        Exporta el DataFrame en varios .xlsx sin reventar:
        - Streaming con openpyxl (write_only) → baja RAM
        - ZIP64 activado explícitamente
        - Partición en N archivos (por defecto 200k filas por archivo)
        - (Opcional) Trunca cadenas muy largas para evitar inflar el ZIP
        """
        import os, gc
        import pandas as pd
        from openpyxl import Workbook

        df = self.df if df is None else df
        if df is None or len(df) == 0:
            self.logs.log("[ExcelPart] DataFrame vacío; no se genera archivo.", "warning")
            return []

        # Límite duro de Excel: 1,048,576 filas por hoja (reservamos header)
        EXCEL_HARD_LIMIT = 1_048_576 - 1
        if max_rows_per_file is None:
            max_rows_per_file = 200_000
        max_rows_per_file = int(min(max_rows_per_file, EXCEL_HARD_LIMIT))

        # Preparar carpeta
        base_dir = os.path.dirname(base_path) or "."
        os.makedirs(base_dir, exist_ok=True)

        n = len(df)
        cols = list(df.columns)
        file_paths = []

        self.logs.log(f"[ExcelPart] Inicio | filas={n:,} | máx/archivo={max_rows_per_file:,} | cols={len(cols)}", "info")

        # (Opcional) truncar textos para reducir tamaño del ZIP
        if truncate_text_cols:
            obj_cols = [c for c in df.columns if df[c].dtype == 'object']
            if obj_cols:
                self.logs.log(f"[ExcelPart] Truncando columnas de texto a {max_text_len} chars en {len(obj_cols)} columnas...", "info")
                for c in obj_cols:
                    # mapeo sin copiar todo el DF
                    df[c] = df[c].astype(str).str.slice(0, max_text_len)

        # Partición
        start = 0
        part_idx = 1
        while start < n:
            end = min(start + max_rows_per_file, n)
            out_path = f"{base_path}_part_{part_idx}.xlsx"
            self.logs.log(f"[ExcelPart] Generando '{out_path}' para filas {start:,}..{end-1:,} (total {end-start:,})", "info")

            # --- OPENPYXL STREAMING + ZIP64 ---
            wb = Workbook(write_only=True)
            ws = wb.create_sheet(title="data")
            # Eliminar hoja default si la crea
            try:
                default_ws = wb._sheets[0]
                if default_ws.title != "data":
                    wb.remove(default_ws)
            except Exception:
                pass
            # Forzar ZIP64 (método o atributo, según versión)
            try:
                wb.use_zip64()
            except Exception:
                try:
                    wb.use_zip64 = True
                except Exception:
                    self.logs.log("[ExcelPart] Aviso: no se pudo activar ZIP64 explícitamente (openpyxl lo activará si es necesario).", "warning")

            # Header
            ws.append(cols)

            # Filas en streaming (RAM constante)
            # .itertuples es más rápido y ligero que .values
            for row in df.iloc[start:end].itertuples(index=False, name=None):
                ws.append(row)

            # Guardar
            wb.save(out_path)
            file_paths.append(out_path)

            # Liberar
            del wb, ws
            gc.collect()

            self.logs.log(f"[ExcelPart] OK '{out_path}'", "info")
            start = end
            part_idx += 1

        self.logs.log(f"[ExcelPart] Total archivos generados: {len(file_paths)}", "info")
        return file_paths




    def ejecutar_carga_completa(self, table_name="airbnb_limpio", excel_path="data/airbnb_limpio.xlsx", sql_server_params=None):
        """
        Ejecuta todas las tareas de carga y verificación.
        """
        try:
            self.logs.log("=== INICIO DE CARGA DE DATOS ===","info")
            self.insertar_en_sqlite(table_name)
            registros_sql = self.verificar_carga_sqlite(table_name)
            if registros_sql != len(self.df):
                self.logs.log("La cantidad de registros en SQLite no coincide con el DataFrame original.","warning")
            self.exportar_a_excel_particionado(
            df=self.df,
            base_path="data/airbnb_limpio"  # generará ..._part_1.xlsx, ..._part_2.xlsx, etc.
            )
            self.logs.log("=== FIN DE CARGA DE DATOS ===","info")
        except Exception as e:
            self.logs.log(f"Error durante la carga completa: {str(e)}", "error")
            raise
