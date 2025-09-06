import logging
from datetime import datetime, timezone

from .db_manager import DBManager

logger = logging.getLogger(__name__)

class ETLMetadataManager:
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager

    def get_last_extraction_timestamp(self, id_sucursal: str, entity_name: str) -> datetime | None:
        """
        Obtiene el timestamp de la última extracción exitosa para una sucursal y entidad.
        """
        query = """
        SELECT last_successful_extraction_utc
        FROM public.etl_fudo_extraction_status
        WHERE id_sucursal = %s AND entity_name = %s;
        """
        result = self.db_manager.fetch_one(query, (id_sucursal, entity_name))
        if result and result[0]:
            last_ts = result[0]
            logger.debug(f"Último timestamp de extracción para {id_sucursal}/{entity_name}: {last_ts}")
            return last_ts
        logger.debug(f"No se encontró un último timestamp de extracción para {id_sucursal}/{entity_name}.")
        return None

    def update_last_extraction_timestamp(self, id_sucursal: str, entity_name: str, timestamp: datetime):
        """
        Actualiza el timestamp de la última extracción exitosa para una sucursal y entidad.
        """
        query = """
        INSERT INTO public.etl_fudo_extraction_status (id_sucursal, entity_name, last_successful_extraction_utc)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_sucursal, entity_name) DO UPDATE SET
            last_successful_extraction_utc = EXCLUDED.last_successful_extraction_utc,
            -- Podemos añadir un campo 'updated_at' en la tabla etl_fudo_extraction_status si es necesario
            -- para rastrear cuándo se actualizó el metadato.
            -- updated_at = CURRENT_TIMESTAMP
            id_sucursal = EXCLUDED.id_sucursal; -- Esto es solo para que DO UPDATE tenga un SET válido en caso de no haber otros campos
        """
        # Nota: El `id_sucursal = EXCLUDED.id_sucursal` en el DO UPDATE es un truco común en Postgres
        # para que el UPSERT funcione sin tener que actualizar un campo real si solo quieres
        # que el registro sea insertado/existente y no siempre tener un SET vacío.
        # Si la tabla tuviera un campo 'updated_at', usaríamos 'updated_at = CURRENT_TIMESTAMP' en su lugar.
        
        self.db_manager.execute_upsert(query, (id_sucursal, entity_name, timestamp))
        logger.info(f"Actualizado último timestamp de extracción para {id_sucursal}/{entity_name} a {timestamp}.")

    def get_fudo_token_data(self, id_sucursal: str) -> dict | None:
        """
        Obtiene los datos del token de Fudo (token y expiración) desde la base de datos.
        """
        query = """
        SELECT access_token, token_expiration_utc
        FROM public.etl_fudo_tokens
        WHERE id_sucursal = %s;
        """
        result = self.db_manager.fetch_one(query, (id_sucursal,))
        if result:
            return {"access_token": result[0], "token_expiration_utc": result[1]}
        return None

    def update_fudo_token_data(self, id_sucursal: str, access_token: str, token_expiration_utc: datetime):
        """
        Actualiza o inserta los datos del token de Fudo en la base de datos.
        """
        query = """
        INSERT INTO public.etl_fudo_tokens (id_sucursal, access_token, token_expiration_utc, last_updated_utc)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id_sucursal) DO UPDATE SET
            access_token = EXCLUDED.access_token,
            token_expiration_utc = EXCLUDED.token_expiration_utc,
            last_updated_utc = EXCLUDED.last_updated_utc;
        """
        self.db_manager.execute_upsert(query, (id_sucursal, access_token, token_expiration_utc, datetime.now(timezone.utc)))
        logger.info(f"Token de Fudo actualizado para sucursal {id_sucursal}.")