#Importando las librerias para extracción y conexión a mongo
import os
import logging
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConfigurationError

#Clase para configurar el logging
class Logs:
    def __init__(self):
        #Configuración del logging
        self.fecha_hora = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Usar la carpeta 'logs' en la raíz del proyecto
        root_logs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
        if not os.path.exists(root_logs_path):
            os.makedirs(root_logs_path)
        log_file_path = os.path.join(root_logs_path, f'log_{self.fecha_hora}.txt')
        logging.basicConfig(filename=log_file_path, level=logging.INFO, filemode='a',
                            format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

    #Función para definir el tipo de mensaje del log
    def log(self, mensaje, nivel):
        if nivel == 'info':
            logging.info(mensaje)
        elif nivel == 'error':
            logging.error(mensaje)

class Extraction:
    def __init__(self):
        self.logs = Logs()

    #Función para realizar la conexión a la base de datos MongoDB
    def mongodb_connection(self, uri, database):
        try:
            client = MongoClient(uri)
            db = client[database]
            db.list_collection_names()

            print(f"Conexión exitosa a la base de datos: {database}")
            self.logs.log(f'Conexión exitosa a la base de datos: {database}', 'info')
            return db
        except ConnectionError as e:
            print("Error al conectar con la base de datos: ", e)
            self.logs.log(f'Error al conectar con la base de datos {database}: {str(e)}', 'error')
            return None
    
    #Función para cargar colecciones a un dataframe
    def load_mongodb_datasets(self, db, colecction_name):
        if db is None:
            raise RuntimeError("Primero se debe llamar al metodo mongodb_connection()")
        
        try:
            cursor = db[colecction_name].find()
            data = list(cursor)
            df = pd.DataFrame(data)
            n = len(df) #Número de registros de la colección para el log
            self.logs.log(f'Colección {colecction_name} añadida al dataframe exitosamente. \
                          Número de registros: {n}', 'info')
            return df
        except Exception as e:
            self.logs.log(f'Error al cargar la coleccion {colecction_name}: {str(e)}', 'error')
            print(f'Error al cargar la colección {colecction_name}')