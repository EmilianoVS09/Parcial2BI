#Importando las librerias para extracción y conexión a mongo
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConfigurationError
from logs import Logs

class Extraction:
    #Constructor con la clase logs
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
            self.logs.log(f'Atención: función de conección no llamada, no se puede continuar con la operación', 'warning')
            raise RuntimeError("Primero se debe llamar al metodo mongodb_connection()")
        
        try:
            cursor = db[colecction_name].find()
            data = list(cursor)
            df = pd.DataFrame(data)
            n = len(df) #Número de registros de la colección para el log
            self.logs.log(f'Colección {colecction_name} añadida al dataframe exitosamente. \
                          #Número de registros: {n}', 'info')
            return df
        except Exception as e:
            self.logs.log(f'Error al cargar la coleccion {colecction_name}: {str(e)}', 'error')
            print(f'Error al cargar la colección {colecction_name}')

    def close_mongodb_connection(self, uri):
        client = MongoClient(uri)
        if client:
            client.close()
            self.logs.log(f'Conexión con MongoDB cerrada existosamente', 'info')