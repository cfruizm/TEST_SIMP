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
log_file = os.path.join(log_dir, "meters_script.log")

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
# Cargar variables de entorno
# -------------------------------------------------
env_path = Path(r"D:\ProyectoSIMP\2025\DashBoardSIMP\config.env")
load_dotenv(dotenv_path=env_path)

# -------------------------------------------------
# Configuración de conexión
# -------------------------------------------------
API_URL = os.getenv("API_URL")
ENCODED_KEY = os.getenv("ENCODED_KEY")
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME", "SDSAPI")
CUSTOMER_COLLECTION_NAME = os.getenv("COLLECTION_NAME", "CUSTOMER")
METERS_COLLECTION_NAME = "METERS"

# -------------------------------------------------
# Conectar a MongoDB
# -------------------------------------------------
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
customer_collection = db[CUSTOMER_COLLECTION_NAME]
meters_collection = db[METERS_COLLECTION_NAME]

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
# Función para extraer contadores y guardarlos en MongoDB
# -------------------------------------------------
def extraer_contadores(token):
    """Extrae los contadores de los dispositivos y almacena los datos sin duplicados."""
    logger.info("Iniciando extracción de contadores desde la API.")
    customer_ids = customer_collection.find({}, {"customerId": 1})  # Obtener customerId de la BD
    inserted_count = 0

    for customer in customer_ids:
        customer_id = customer.get("customerId")
        if not customer_id:
            logger.warning("Cliente sin customerId encontrado. Se omite.")
            continue

        logger.info(f"Consultando contadores para el cliente {customer_id}...")
        # Consultar la API
        url = f"{API_URL}/api/devices/meters/{customer_id}" #para exportar contador de cierta fecha usar ?billingDate=2025-02-14 despues del customer_id
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        try:
            response = requests.get(url, headers=headers)
            logger.debug("Respuesta de la API para contadores (cliente %s): %s", customer_id, response.text)
            if response.status_code == 200:
                contadores = response.json()
                if isinstance(contadores, list):  # Validar si la respuesta es una lista de registros
                    for contador in contadores:
                        device_id = contador.get("deviceId")
                        reading_datetime = contador.get("readingDateTime")
                        if device_id and reading_datetime:
                            # Verificar si ya existe un registro con el mismo deviceId y readingDateTime
                            existe = meters_collection.find_one({"deviceId": device_id, "readingDateTime": reading_datetime})
                            if not existe:
                                meters_collection.insert_one(contador)
                                inserted_count += 1
                                logger.info("Contador para dispositivo %s insertado.", device_id)
                else:
                    logger.warning("La respuesta para el cliente %s no es una lista.", customer_id)
            else:
                logger.error("Error al obtener contadores para el cliente %s: %s", customer_id, response.text)
        except Exception as e:
            logger.exception("Excepción al obtener contadores para el cliente %s: %s", customer_id, e)

    logger.info("Se insertaron %d nuevos registros de contadores.", inserted_count)

# -------------------------------------------------
# Ejecución del script
# -------------------------------------------------
if __name__ == "__main__":
    logger.info("Inicio de ejecución del script de extracción de contadores.")
    token = obtener_token()
    if token:
        extraer_contadores(token)
    else:
        logger.error("No se pudo obtener el token. Finalizando la ejecución.")
    logger.info("Fin de ejecución del script.")
