import requests
import os
import json
import logging
from pymongo import MongoClient
from dotenv import load_dotenv
from pathlib import Path

# -------------------------------------------------
# Configuración del Logger para consola y archivo
# -------------------------------------------------
log_dir = r"D:\ProyectoSIMP\2025\DashBoardSIMP\app\logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file = os.path.join(log_dir, "devices_script.log")

logging.basicConfig(
    level=logging.INFO,  # Se registran todos los niveles: INFO, WARNING, ERROR, CRITICAL
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode='a', encoding='utf-8'),
        logging.StreamHandler()  # Muestra el log en la consola
    ]
)
logger = logging.getLogger()

# -------------------------------------------------
# Cargar variables de entorno desde config.env
# -------------------------------------------------
env_path = Path(r"D:\ProyectoSIMP\2025\DashBoardSIMP\config.env")
load_dotenv(dotenv_path=env_path)

# -------------------------------------------------
# Obtener configuraciones desde variables de entorno
# -------------------------------------------------
API_URL = os.getenv("API_URL")
ENCODED_KEY = os.getenv("ENCODED_KEY")
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME", "SDSAPI")
CUSTOMER_COLLECTION_NAME = os.getenv("COLLECTION_NAME", "CUSTOMER")
DEVICE_COLLECTION_NAME = "DEVICE"

# -------------------------------------------------
# Conectar a MongoDB
# -------------------------------------------------
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
customer_collection = db[CUSTOMER_COLLECTION_NAME]
device_collection = db[DEVICE_COLLECTION_NAME]

# -------------------------------------------------
# Función para obtener el token JWT
# -------------------------------------------------
def obtener_token():
    """Obtiene el token JWT a partir de la Encode Key."""
    logger.info("Iniciando obtención del token JWT.")
    url = f"{API_URL}/login"
    headers = {
        "Authorization": f"Basic {ENCODED_KEY}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(url, headers=headers)
        logger.debug("Respuesta de la API para token: %s", response.text)
        if response.status_code == 200:
            token = response.json().get("access_token")
            logger.info("Token obtenido correctamente.")
            return token
        else:
            logger.error("Error al obtener el token: %s", response.text)
            return None
    except Exception as e:
        logger.exception("Excepción durante la obtención del token: %s", e)
        return None

# -------------------------------------------------
# Función para extraer dispositivos y guardarlos en MongoDB
# -------------------------------------------------
def extraer_dispositivos(token):
    """Extrae los dispositivos de todos los clientes y los guarda en MongoDB sin duplicados."""
    logger.info("Iniciando extracción de dispositivos desde la API.")
    
    customer_ids = customer_collection.find({}, {"customerId": 1})  # Obtener todos los customerId

    inserted_count = 0
    updated_count = 0

    for customer in customer_ids:
        customer_id = customer.get("customerId")
        if customer_id:
            logger.info(f"Consultando dispositivos para el cliente {customer_id}...")

            url = f"{API_URL}/api/devices?customerId={customer_id}&includeExtendedFields=true"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            try:
                response = requests.get(url, headers=headers)
                logger.debug("Respuesta de la API para dispositivos (cliente %s): %s", customer_id, response.text)
                
                if response.status_code == 200:
                    dispositivos = response.json()

                    if isinstance(dispositivos, list):  # Si es una lista de dispositivos
                        for dispositivo in dispositivos:
                            device_id = dispositivo.get("deviceId")
                            if device_id:
                                result = device_collection.update_one(
                                    {"deviceId": device_id},
                                    {"$set": dispositivo},
                                    upsert=True
                                )
                                if result.upserted_id:
                                    inserted_count += 1
                                    logger.info("Dispositivo %s insertado.", device_id)
                                else:
                                    updated_count += 1
                                    logger.info("Dispositivo %s actualizado.", device_id)

                    elif isinstance(dispositivos, dict):  # Si es un solo dispositivo
                        device_id = dispositivos.get("deviceId")
                        if device_id:
                            result = device_collection.update_one(
                                {"deviceId": device_id},
                                {"$set": dispositivos},
                                upsert=True
                            )
                            if result.upserted_id:
                                inserted_count += 1
                                logger.info("Dispositivo %s insertado.", device_id)
                            else:
                                updated_count += 1
                                logger.info("Dispositivo %s actualizado.", device_id)

                else:
                    logger.error("Error al obtener dispositivos para el cliente %s: %s", customer_id, response.text)
            
            except Exception as e:
                logger.exception("Excepción al obtener dispositivos para el cliente %s: %s", customer_id, e)
    
    logger.info("Se insertaron %d nuevos dispositivos y se actualizaron %d dispositivos.", inserted_count, updated_count)

# -------------------------------------------------
# Ejecución del script
# -------------------------------------------------
if __name__ == "__main__":
    logger.info("Inicio de ejecución del script de extracción de dispositivos.")
    token = obtener_token()
    if token:
        extraer_dispositivos(token)
    else:
        logger.error("No se pudo obtener el token. Finalizando la ejecución.")
    logger.info("Fin de ejecución del script.")
