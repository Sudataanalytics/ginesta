# fudo_etl/modules/db_manager.py
import psycopg2
from psycopg2 import extras
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class DBManager:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        self._connect() # Conectar en la inicialización

    def _connect(self):
        """Establece una nueva conexión a la base de datos."""
        try:
            if self.connection: # Si ya hay una conexión, la cerramos antes de crear una nueva
                self.connection.close()
            self.connection = psycopg2.connect(self.connection_string)
            self.connection.autocommit = False
            logger.info("Conexión a la base de datos PostgreSQL establecida/restablecida.")
        except Exception as e:
            logger.error(f"Error al conectar a la base de datos: {e}", exc_info=True)
            raise

    def _ensure_connection(self):
        """Asegura que la conexión esté activa. Intenta reconectar si es necesario."""
        if not self.connection:
            logger.warning("Conexión a la base de datos no encontrada. Intentando reconectar.")
            self._connect()
            return

        try:
            # Intenta ejecutar una consulta simple para verificar la conexión
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logger.warning(f"Conexión a la base de datos inactiva/cerrada ({e}). Intentando reconectar.")
            self._connect() # Reconectar
        except Exception as e:
            logger.error(f"Error inesperado al verificar la conexión: {e}", exc_info=True)
            raise

    def close(self):
        """Cierra la conexión a la base de datos."""
        if self.connection:
            self.connection.close()
            self.connection = None # Establecer a None después de cerrar
            logger.info("Conexión a la base de datos PostgreSQL cerrada.")

    def execute_query(self, query: str, params: tuple = None):
        """Ejecuta una consulta SQL que no devuelve resultados y hace commit (INSERT, UPDATE, DELETE, CREATE, REFRESH MATERIALIZED VIEW)."""
        self._ensure_connection() # <--- ¡AÑADIR ESTO AQUÍ!
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
            self.connection.commit()
            logger.debug(f"Consulta ejecutada con éxito: {query[:100]}...")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error al ejecutar la consulta: {e}. Query: {query[:100]}...", exc_info=True)
            raise

    def execute_upsert(self, query: str, params: tuple | list[tuple]):
        """
        Ejecuta una consulta SQL de UPSERT.
        Puede ser para una sola fila (params como tuple) o múltiples (params como list[tuple]).
        """
        self._ensure_connection() # <--- ¡AÑADIR ESTO AQUÍ!
        try:
            with self.connection.cursor() as cursor:
                if isinstance(params, list):
                    extras.execute_values(cursor, query, params, page_size=1000)
                else:
                    cursor.execute(query, params)
            self.connection.commit()
            logger.debug(f"UPSERT ejecutado con éxito: {query[:100]}...")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error en UPSERT: {e}. Query: {query[:100]}...", exc_info=True)
            raise

    def fetch_one(self, query: str, params: tuple = None) -> tuple | None:
        """Ejecuta una consulta SQL y devuelve una sola fila."""
        self._ensure_connection() # <--- ¡AÑADIR ESTO AQUÍ!
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchone()
        except Exception as e:
            logger.error(f"Error en fetch_one: {e}. Query: {query[:100]}...", exc_info=True)
            raise

    def fetch_all(self, query: str, params: tuple = None) -> list[tuple]:
        """Ejecuta una consulta SQL y devuelve todas las filas."""
        self._ensure_connection() # <--- ¡AÑADIR ESTO AQUÍ!
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error en fetch_all: {e}. Query: {query[:100]}...", exc_info=True)
            raise

    def insert_raw_data(self, table_name: str, records: list[dict]):
        """
        Inserta datos crudos en la tabla especificada.
        Utiliza `psycopg2.extras.execute_values` para inserciones en batch eficientes.
        Asume que `records` es una lista de diccionarios,
        y cada dict tiene las claves esperadas por la tabla fudo_raw_*.
        La inserción es un UPSERT optimizado para evitar duplicados idénticos y solo
        insertar nuevas versiones si el payload ha cambiado.
        """
        self._ensure_connection() # <--- ¡AÑADIR ESTO AQUÍ!
        if not records:
            logger.info(f"No hay registros para insertar en {table_name}.")
            return

        columns = [
            'id_fudo',
            'id_sucursal_fuente',
            'fecha_extraccion_utc',
            'payload_json',
            'last_updated_at_fudo',
            'payload_checksum'
        ]
        
        values_to_insert = []
        for record in records:
            values_to_insert.append((
                record.get('id_fudo'),
                record.get('id_sucursal_fuente'),
                record.get('fecha_extraccion_utc'),
                record.get('payload_json'),
                record.get('last_updated_at_fudo'),
                record.get('payload_checksum')
            ))
        
        cols_str = ', '.join(columns)

        insert_query = f"""
        INSERT INTO public.{table_name} ({cols_str})
        VALUES %s
        ON CONFLICT (id_fudo, id_sucursal_fuente, payload_checksum) DO NOTHING;
        """

        try:
            with self.connection.cursor() as cursor:
                extras.execute_values(cursor, insert_query, values_to_insert, page_size=1000)
            self.connection.commit()
            logger.info(f"Cargados {len(records)} registros en {table_name}.")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error al cargar datos crudos en {table_name}: {e}", exc_info=True)
            raise

    def execute_sql_script(self, sql_content: str):
        """
        Ejecuta un script SQL completo recibido como una cadena de texto.
        Útil para crear la estructura de la base de datos o refrescar MVs.
        """
        self._ensure_connection() # <--- ¡AÑADIR ESTO AQUÍ!
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_content)
            self.connection.commit()
            logger.info(f"Script SQL ejecutado exitosamente.")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error al ejecutar el script SQL: {e}", exc_info=True)
            raise