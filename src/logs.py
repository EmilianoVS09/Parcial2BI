#Importar librerías necesarias
import os
import logging
from datetime import datetime

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
        elif nivel == "warning":
            logging.warning(mensaje)