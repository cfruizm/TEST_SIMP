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
log_file = os.path.join(log_dir, "monitors_script.log")

logging.basicConfig(
    level=logging.INFO,  # Se registran INFO, WARNING, ERROR y CRITICAL
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
MONITOR_COLLECTION_NAME = "MONITOR"

# -------------------------------------------------
# Conectar a MongoDB
# -------------------------------------------------
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
customer_collection = db[CUSTOMER_COLLECTION_NAME]
monitor_collection = db[MONITOR_COLLECTION_NAME]

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
# Función para extraer monitores y guardarlos en MongoDB
# -------------------------------------------------
def extraer_monitores(token):
    """Extrae los monitores de todos los clientes y los guarda en MongoDB sin duplicados."""
    logger.info("Iniciando extracción de monitores desde la API.")
    
    # Obtener todos los customerId de la colección CUSTOMER
    customer_ids = customer_collection.find({}, {"customerId": 1})
    inserted_count = 0
    updated_count = 0

    for customer in customer_ids:
        customer_id = customer.get("customerId")
        if customer_id:
            logger.info(f"Consultando monitores para el cliente {customer_id}...")
            # Realizar la consulta a la API para obtener los monitores de este customerId
            url = f"{API_URL}/api/monitors?customerId={customer_id}"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            try:
                response = requests.get(url, headers=headers)
                logger.debug("Respuesta de la API para monitores (cliente %s): %s", customer_id, response.text)
                
                if response.status_code == 200:
                    monitores = response.json()
                    if isinstance(monitores, list):  # Si es una lista de monitores
                        for monitor in monitores:
                            monitor_id = monitor.get("monitorId")
                            if monitor_id:
                                result = monitor_collection.update_one(
                                    {"monitorId": monitor_id},
                                    {"$set": monitor},
                                    upsert=True
                                )
                                if result.upserted_id:  # Si se insertó un nuevo monitor
                                    inserted_count += 1
                                    logger.info("Monitor %s insertado.", monitor_id)
                                else:  # Si se actualizó un monitor
                                    updated_count += 1
                                    logger.info("Monitor %s actualizado.", monitor_id)
                    elif isinstance(monitores, dict):  # Si es un solo monitor
                        monitor_id = monitores.get("monitorId")
                        if monitor_id:
                            result = monitor_collection.update_one(
                                {"monitorId": monitor_id},
                                {"$set": monitores},
                                upsert=True
                            )
                            if result.upserted_id:  # Si se insertó un nuevo monitor
                                inserted_count += 1
                                logger.info("Monitor %s insertado.", monitor_id)
                            else:  # Si se actualizó un monitor
                                updated_count += 1
                                logger.info("Monitor %s actualizado.", monitor_id)
                else:
                    logger.error("Error al obtener monitores para el cliente %s: %s", customer_id, response.text)
            except Exception as e:
                logger.exception("Excepción al obtener monitores para el cliente %s: %s", customer_id, e)
    
    logger.info("Se insertaron %d nuevos monitores y se actualizaron %d monitores.", inserted_count, updated_count)

# -------------------------------------------------
# Ejecución del script
# -------------------------------------------------
if __name__ == "__main__":
    logger.info("Inicio de ejecución del script de extracción de monitores.")
    token = obtener_token()
    if token:
        extraer_monitores(token)
    else:
        logger.error("No se pudo obtener el token. Finalizando la ejecución.")
    logger.info("Fin de ejecución del script.")
