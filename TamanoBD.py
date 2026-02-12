from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pathlib import Path

# Cargar variables de entorno
env_path = Path(r"D:\ProyectoSIMP\2025\DashBoardSIMP\config.env")
load_dotenv(dotenv_path=env_path)

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME", "SDSAPI")

# Conectar a MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Obtener estadísticas de la base de datos
db_stats = db.command("dbStats")
db_size_mb = db_stats["dataSize"] / (1024 * 1024)  # Convertir bytes a MB

print(f"Tamaño de la base de datos '{DATABASE_NAME}': {db_size_mb:.2f} MB")
