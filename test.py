from dotenv import load_dotenv
import os
from pathlib import Path

# Ruta explícita al archivo .env
env_path = Path("D:\Pruebas\config.env")
print("¿El archivo connect.env existe?", env_path.exists())  # Verificar si el archivo existe

# Cargar el archivo .env desde la ruta explícita
load_dotenv(dotenv_path=env_path)

# Intentar obtener la variable de entorno
mongo_uri = os.getenv("MONGO_URI")
print("MONGO_URI:", mongo_uri)
