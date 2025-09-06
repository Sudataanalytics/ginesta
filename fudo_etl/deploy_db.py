# fudo-etl-project/deploy_db.py
import psycopg2
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_target_database():
    load_dotenv()
    admin_conn_string = os.getenv("DONWEB_ADMIN_CONNECTION_STRING")
    target_db_name = os.getenv("TARGET_DATABASE_NAME")

    if not admin_conn_string or not target_db_name:
        logger.error("DONWEB_ADMIN_CONNECTION_STRING o TARGET_DATABASE_NAME no configurados en .env")
        return

    conn = None
    try:
        # Conectarse a la BD por defecto 'postgres' con privilegios de administrador
        logger.info(f"Conectando a {admin_conn_string.split('@')[1].split('/')[0]} para crear la base de datos '{target_db_name}'...")
        conn = psycopg2.connect(admin_conn_string)
        conn.autocommit = True # Autocommit es necesario para CREATE DATABASE
        cursor = conn.cursor()

        # Verificar si la base de datos ya existe
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{target_db_name}'")
        if cursor.fetchone():
            logger.info(f"La base de datos '{target_db_name}' ya existe. No es necesario crearla.")
        else:
            # Crear la nueva base de datos
            # Es importante especificar ENCODING y COLLATE para consistencia.
            # CONNECTION LIMIT -1 significa sin límite de conexiones.
            create_db_sql = f"""
            CREATE DATABASE "{target_db_name}"
            WITH ENCODING 'UTF8'
            TEMPLATE template0
            LC_COLLATE 'en_US.UTF-8'
            LC_CTYPE 'en_US.UTF-8'
            CONNECTION LIMIT -1;
            """
            cursor.execute(create_db_sql)
            logger.info(f"Base de datos '{target_db_name}' creada exitosamente.")
        
        cursor.close()

    except Exception as e:
        logger.error(f"Error al crear la base de datos '{target_db_name}': {e}", exc_info=True)
        if "database exists" in str(e).lower(): # Manejo específico si justo otro proceso la creó
            logger.warning(f"La base de datos '{target_db_name}' ya existía, continuando.")
        else:
            raise # Re-lanzar si es un error diferente
    finally:
        if conn:
            conn.close()
            logger.info("Conexión de administrador cerrada.")

if __name__ == "__main__":
    create_target_database()