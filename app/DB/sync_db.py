import os
import subprocess
import logging
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno desde config.env
load_dotenv("D:\\ProyectoSIMP\\2025\\DashBoardSIMP\\config.env")
MONGO_URI = os.getenv("MONGO_URI")  # Conexión a MongoDB local
DATABASE_NAME = os.getenv("DATABASE_NAME", "SDSAPI")
ATLAS_URI = os.getenv("ATLAS_URI")  # Ejemplo: mongodb+srv://usuario:contraseña@arussimp.dkglaet.mongodb.net/?retryWrites=true&w=majority
BACKUP_DIR = os.getenv("BACKUP_DIR", "D:\\backups\\SDSAPI_backup")

# Rutas completas a los ejecutables de mongodump y mongorestore
DUMP_CMD = r"D:\Program Files\MongoDB\mongodb-database-tools\bin\mongodump.exe"
RESTORE_CMD = r"D:\Program Files\MongoDB\mongodb-database-tools\bin\mongorestore.exe"

# Configuración de logging (consola y archivo)
log_file = r"D:\ProyectoSIMP\2025\DashBoardSIMP\app\logs\sync_db.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def make_backup():
    """
    Realiza un mongodump de la base de datos local.
    Crea un directorio con timestamp para almacenar el volcado.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR)
    os.makedirs(backup_path, exist_ok=True)
    
    dump_cmd = [
        DUMP_CMD,
        "--uri", MONGO_URI,
        "--db", DATABASE_NAME,
        "--out", backup_path
    ]
    
    logging.info("Ejecutando mongodump...")
    try:
        subprocess.run(dump_cmd, check=True)
        logging.info(f"mongodump completado. Backup almacenado en: {backup_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error ejecutando mongodump: {e}")
        raise
    return backup_path

def restore_to_atlas(backup_path):
    """
    Restaura el volcado de la base de datos local en MongoDB Atlas usando mongorestore.
    Se elimina la base de datos de destino antes de la restauración.
    """
    db_backup_path = backup_path
    
    restore_cmd = [
        RESTORE_CMD,
        "--uri", ATLAS_URI,
        "--drop",  # Elimina la base de datos en Atlas antes de restaurar
        "--dir", db_backup_path
    ]
    
    logging.info("Ejecutando mongorestore a MongoDB Atlas...")
    try:
        subprocess.run(restore_cmd, check=True)
        logging.info("mongorestore completado exitosamente en Atlas.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error ejecutando mongorestore: {e}")
        raise

def main():
    try:
        backup_path = make_backup()
        restore_to_atlas(backup_path)
        logging.info("Sincronización completada exitosamente.")
    except Exception as e:
        logging.error(f"Error en la sincronización: {e}")

if __name__ == "__main__":
    main()
