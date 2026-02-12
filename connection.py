from dotenv import load_dotenv
import os
from pymongo import MongoClient
from pathlib import Path

# Cargar las variables desde el archivo .env
# Ruta explícita al archivo .env
env_path = Path(r"D:\ProyectoSIMP\2025\DashBoardSIMP\config.env")
load_dotenv(dotenv_path=env_path)

# Leer la URI desde las variables de entorno
MONGO_URI = os.getenv("MONGO_URI")
# Verificar que se está cargando la URI
#print("MONGO_URI:", MONGO_URI)

# Conectar a MongoDB
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
# print(client.server_info())  # Si falla aquí, revisa las credenciales
print("Bases de datos disponibles:", client.list_database_names())
