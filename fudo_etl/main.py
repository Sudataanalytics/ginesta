# fudo-etl-project/main.py
import logging
import json
from datetime import datetime, timezone
import uuid
import time
from hashlib import md5
# Tus módulos
from modules.config import load_config
from modules.db_manager import DBManager
from modules.etl_metadata_manager import ETLMetadataManager
from modules.fudo_auth import FudoAuthenticator
from modules.fudo_api_client import FudoApiClient

# Configuración básica de logging para todo el script principal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Función auxiliar para parsear fechas de la API
def parse_fudo_date(date_str: str | None) -> datetime | None:
    """
    Parsea una cadena de fecha de Fudo (ISO 8601 con 'Z') a un objeto datetime UTC.
    Maneja None y errores de formato.
    """
    if date_str is None:
        return None
    try:
        # Fudo usa 'Z' para UTC, Python fromisoformat necesita '+00:00'
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        logger.warning(f"No se pudo parsear la fecha de Fudo: '{date_str}'. Retornando None.")
        return None

# --- FUNCIÓN PARA LA FASE DE TRANSFORMACIÓN Y CARGA AL DER ---
def refresh_analytics_materialized_views(db_manager: DBManager):
    logger.info("==================================================")
    logger.info("  Iniciando fase de Transformación (Creación/Refresco de MVs)")
    logger.info("==================================================")

    # Definir el orden de refresco basado en dependencias.
    # Es crucial CREAR y refrescar dimensiones antes que hechos que dependen de ellas.
    # Cada tupla contiene (nombre_mv, sql_creacion_mv).
    materialized_views_configs = [
        # mv_sucursales
        ('mv_sucursales', """
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_sucursales AS
            SELECT
                id_sucursal,
                sucursal_name AS sucursal
            FROM public.config_fudo_branches
            WHERE is_active = TRUE;
        """),
        # mv_rubros
        ('mv_rubros', """
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rubros AS
            SELECT DISTINCT ON (id_fudo)
                (payload_json ->> 'id')::INTEGER AS id_rubro,
                (payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS rubro_name
            FROM public.fudo_raw_product_categories
    -- NUEVA LÍNEA: ADD WHERE payload_checksum IS NOT NULL for MV creation
            WHERE
                payload_json ->> 'id' IS NOT NULL AND
                payload_json -> 'attributes' ->> 'name' IS NOT NULL
            ORDER BY id_fudo, fecha_extraccion_utc DESC;
        """),
        # mv_medio_pago
        ('mv_medio_pago', """
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_medio_pago AS
            SELECT DISTINCT ON (id_fudo)
                (payload_json ->> 'id')::INTEGER AS id_payment,
                (payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS payment_method
            FROM public.fudo_raw_payment_methods
            WHERE
                payload_json ->> 'id' IS NOT NULL AND
                payload_json -> 'attributes' ->> 'name' IS NOT NULL
            ORDER BY id_fudo, fecha_extraccion_utc DESC;
        """),
        # mv_productos
        ('mv_productos', """
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_productos AS
            SELECT DISTINCT ON (p.id_fudo)
                (p.payload_json ->> 'id')::INTEGER AS id_product,
                (p.payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS product_name,
                (p.payload_json -> 'relationships' -> 'productCategory' -> 'data' ->> 'id')::INTEGER AS id_rubro
            FROM public.fudo_raw_products p
            WHERE
                p.payload_json ->> 'id' IS NOT NULL AND
                p.payload_json -> 'attributes' ->> 'name' IS NOT NULL
            ORDER BY p.id_fudo, p.fecha_extraccion_utc DESC;
        """),
        # mv_sales_order
        ('mv_sales_order', """
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_sales_order AS
            SELECT DISTINCT ON (s.id_fudo, s.id_sucursal_fuente)
                (s.payload_json ->> 'id')::INTEGER AS id_order,
                0.0::FLOAT AS amount_tax,
                (s.payload_json -> 'attributes' ->> 'total')::FLOAT AS amount_total,
                COALESCE(
                    (s.payload_json -> 'attributes' ->> 'closedAt')::TIMESTAMP WITH TIME ZONE,
                    (s.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE
                ) AS date_order,
                s.id_sucursal_fuente AS id_sucursal
            FROM public.fudo_raw_sales s
            WHERE
                s.payload_json ->> 'id' IS NOT NULL AND
                s.payload_json -> 'attributes' ->> 'total' IS NOT NULL AND
                (s.payload_json -> 'attributes' ->> 'saleState') = 'CLOSED' AND
                s.id_sucursal_fuente IS NOT NULL -- <--- ¡AÑADIR AQUÍ TAMBIÉN!
            ORDER BY s.id_fudo, s.id_sucursal_fuente, s.fecha_extraccion_utc DESC;
        """),
        # mv_pagos
        ('mv_pagos', """
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_pagos AS
            SELECT DISTINCT ON (p.id_fudo, p.id_sucursal_fuente)
                (p.payload_json ->> 'id')::INTEGER AS id,
                (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::INTEGER AS pos_order_id,
                (p.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id')::INTEGER AS id_payment,
                (p.payload_json -> 'attributes' ->> 'amount')::FLOAT AS amount,
                (p.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS payment_date
            FROM public.fudo_raw_payments p
            WHERE
                p.payload_json ->> 'id' IS NOT NULL AND
                (p.payload_json -> 'attributes' ->> 'amount') IS NOT NULL AND
                (p.payload_json -> 'attributes' ->> 'createdAt') IS NOT NULL AND
                (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') IS NOT NULL AND
                (p.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id') IS NOT NULL AND
                (p.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN IS NOT TRUE
            ORDER BY p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc DESC;
        """),
        # mv_sales_order_line
        ('mv_sales_order_line', """
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_sales_order_line AS
            SELECT DISTINCT ON (i.id_fudo, i.id_sucursal_fuente)
                (i.payload_json ->> 'id')::INTEGER AS id_order_line,
                (i.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::INTEGER AS id_order,
                (i.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS date_order_time,
                (i.payload_json -> 'attributes' ->> 'createdAt')::DATE AS date_order,
                (i.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id')::INTEGER AS id_product,
                (i.payload_json -> 'attributes' ->> 'price')::FLOAT AS price_unit,
                ((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT)::INTEGER AS qty,
                ((i.payload_json -> 'attributes' ->> 'price')::FLOAT * (i.payload_json -> 'attributes' ->> 'quantity')::FLOAT)::FLOAT AS amount_total,
                i.id_sucursal_fuente AS id_sucursal
            FROM public.fudo_raw_items i
            WHERE
                i.payload_json ->> 'id' IS NOT NULL AND
                (i.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') IS NOT NULL AND
                (i.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id') IS NOT NULL AND
                (i.payload_json -> 'attributes' ->> 'createdAt') IS NOT NULL AND
                (i.payload_json -> 'attributes' ->> 'price') IS NOT NULL AND
                (i.payload_json -> 'attributes' ->> 'quantity') IS NOT NULL AND
                (i.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN IS NOT TRUE AND
                ((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT)::INTEGER > 0
            ORDER BY i.id_fudo, i.id_sucursal_fuente, i.fecha_extraccion_utc DESC;
        """)
    ]

    for mv_name, create_sql in materialized_views_configs:
        logger.info(f"  Procesando Vista Materializada: '{mv_name}'...")
        try:
            # Intentar crear la MV si no existe
            logger.info(f"    Intentando crear MV '{mv_name}' si no existe...")
            db_manager.execute_query(create_sql)
            logger.info(f"    MV '{mv_name}' creada/existente.")

            # Luego, refrescarla
            logger.info(f"    Refrescando MV '{mv_name}'...")
            db_manager.execute_query(f"REFRESH MATERIALIZED VIEW public.{mv_name};")
            logger.info(f"    MV '{mv_name}' refrescada exitosamente.")

        except Exception as e:
            logger.error(f"  ERROR al procesar la Vista Materializada '{mv_name}': {e}", exc_info=True)
            continue 

    logger.info("==================================================")
    logger.info("  Fase de Transformación (Creación/Refresco de MVs) FINALIZADA.")
    logger.info("==================================================")


def run_fudo_raw_etl(db_manager: DBManager): # db_manager ahora se pasa como argumento
    logger.info("==================================================")
    logger.info("  Iniciando proceso ETL RAW de Fudo - EXTRACT & LOAD")
    logger.info("==================================================")
    
    # db ya se pasa como argumento, no se crea aquí
    try:
        config = load_config()
        project_id = config.get("gcp_project_id")

        # Reutilizar el db_manager pasado
        metadata_manager = ETLMetadataManager(db_manager) # Usar db_manager pasado
        authenticator = FudoAuthenticator(db_manager, config['fudo_auth_endpoint'], project_id) # Usar db_manager pasado
        api_client = FudoApiClient(config['fudo_api_base_url'])

        logger.info("Obteniendo lista de sucursales activas de la base de datos...")
        branches_config = db_manager.fetch_all( # Usar db_manager pasado
            "SELECT id_sucursal, fudo_branch_identifier, sucursal_name, "
            "secret_manager_apikey_name, secret_manager_apisecret_name "
            "FROM public.config_fudo_branches WHERE is_active = TRUE"
        )
        if not branches_config:
            logger.warning("No se encontraron sucursales activas para procesar en config_fudo_branches. Finalizando.")
            return 

        entities_to_extract = [
            'customers', 'discounts', 'expenses', 'expense-categories', 'ingredients',
            'items', 'kitchens', 'payments', 'payment-methods', 'product-categories',
            'product-modifiers', 'products', 'roles', 'rooms', 'sales', 'tables', 'users'
        ]

        for branch_data in branches_config:
            id_sucursal_internal = branch_data[0]
            fudo_branch_id = branch_data[1]
            branch_name = branch_data[2]
            api_key_secret_name = branch_data[3]
            api_secret_secret_name = branch_data[4]

            logger.info(f"\n--- Procesando Sucursal: '{branch_name}' (ID interno: '{id_sucursal_internal}') ---")

            try:
                token = authenticator.get_valid_token(
                    id_sucursal_internal, 
                    api_key_secret_name, 
                    api_secret_secret_name
                )
                api_client.set_auth_token(token)
                logger.debug(f"Token válido establecido para {id_sucursal_internal}.")

                for entity in entities_to_extract:
                    raw_table_name = f"fudo_raw_{entity.replace('-', '_')}"

                    logger.info(f"  Extrayendo entidad '{entity}' para sucursal '{id_sucursal_internal}'...")
                    
                    try:
                        last_extracted_ts = metadata_manager.get_last_extraction_timestamp(
                            id_sucursal_internal, entity
                        )
                        
                        raw_data_records_from_api = api_client.get_data(
                            entity, 
                            id_sucursal_internal,
                            last_extracted_ts 
                        )
                        
                        if raw_data_records_from_api:
                            logger.info(f"    {len(raw_data_records_from_api)} registros extraídos de '{entity}'. Preparando para carga en '{raw_table_name}'...")
                            
                            prepared_records_for_db = []
                            for record in raw_data_records_from_api:
                                fudo_record_id = record.get('id', str(uuid.uuid4())) 

                                last_updated_fudo = None
                                attributes = record.get('attributes', {})

                                if entity == 'sales':
                                    last_updated_fudo = parse_fudo_date(attributes.get('closedAt'))
                                    if last_updated_fudo is None:
                                        last_updated_fudo = parse_fudo_date(attributes.get('createdAt'))
                                elif entity in ['customers', 'expenses', 'items', 'payments', 'products', 'discounts', 'ingredients', 'roles', 'tables', 'users', 'expense_categories', 'kitchens', 'product_categories', 'product_modifiers', 'rooms']:
                                    last_updated_fudo = parse_fudo_date(attributes.get('createdAt'))
                                
                                payload_str = json.dumps(record, sort_keys=True)
                                payload_checksum = md5(payload_str.encode('utf-8')).hexdigest()

                                prepared_records_for_db.append({
                                    'id_fudo': fudo_record_id,
                                    'id_sucursal_fuente': id_sucursal_internal,
                                    'fecha_extraccion_utc': datetime.now(timezone.utc),
                                    'payload_json': payload_str,
                                    'last_updated_at_fudo': last_updated_fudo,
                                    'payload_checksum': payload_checksum
                                })

                            db_manager.insert_raw_data(raw_table_name, prepared_records_for_db) # Usar db_manager pasado
                            logger.info(f"    {len(prepared_records_for_db)} registros cargados en '{raw_table_name}'.")
                            
                            metadata_manager.update_last_extraction_timestamp(
                                id_sucursal_internal, entity, datetime.now(timezone.utc)
                            )
                        else:
                            logger.info(f"    No se extrajeron nuevos registros para '{entity}' (o no hubo actividad relevante desde el último filtro GTE).")

                    except ConnectionError as ce:
                        logger.error(f"  Fallo de conexión o reintentos agotados para entidad '{entity}' de sucursal '{id_sucursal_internal}': {ce}", exc_info=True)
                        continue
                    except Exception as e:
                        logger.error(f"  Error inesperado al procesar entidad '{entity}' para sucursal '{id_sucursal_internal}': {e}", exc_info=True)
                        continue
            except Exception as e:
                logger.error(f"Error crítico al procesar sucursal '{branch_name}' ({id_sucursal_internal}): {e}", exc_info=True)
                continue 
            
            time.sleep(1) 

        # --- LLAMADA A LA FASE DE TRANSFORMACIÓN DESPUÉS DE LA EXTRACCIÓN RAW COMPLETA ---
        refresh_analytics_materialized_views(db_manager) # Usar db_manager pasado
        # ----------------------------------------------------------------------------------

    except Exception as e:
        logger.critical(f"ERROR FATAL en el proceso ETL RAW principal: {e}", exc_info=True)
        print(f"ERROR FATAL: {e}")
    finally:
        # La conexión se cerrará en la función main()
        pass 


# --- FUNCIÓN PARA DESPLEGAR LA ESTRUCTURA INICIAL DE FUDO EN LA DB ---
def deploy_fudo_database_structure(db_manager: DBManager, ddl_script_path: str):
    logger.info("==================================================")
    logger.info(f"  Iniciando despliegue de estructura Fudo desde '{ddl_script_path}'")
    logger.info("==================================================")
    try:
        # Leer el script DDL completo
        with open(ddl_script_path, 'r', encoding='utf-8') as f:
            sql_script_content = f.read()
        
        # Ejecutar el script SQL completo
        # La función execute_sql_script en db_manager.py ya maneja la ejecución y commit.
        db_manager.execute_sql_script(sql_script_content)
        logger.info("Estructura de la base de datos Fudo desplegada/actualizada exitosamente.")
    except Exception as e:
        logger.critical(f"ERROR FATAL al desplegar la estructura de la base de datos Fudo: {e}", exc_info=True)
        raise
    logger.info("==================================================")
    logger.info("  Despliegue de estructura Fudo FINALIZADO.")
    logger.info("==================================================")


if __name__ == "__main__":
    # --- FASE DE DESPLIEGUE INICIAL Y EJECUCIÓN REGULAR ---
    config = load_config()
    db_conn_string = config['db_connection_string'] # La cadena de conexión del ETL (apunta a la DB 'ginesta')

    db_for_all_phases = None
    try:
        db_for_all_phases = DBManager(db_conn_string)

        # Paso 1: Ejecutar el script DDL maestro para crear/actualizar toda la estructura.
        # Esto se puede ejecutar siempre, ya que usa CREATE IF NOT EXISTS.
        logger.info("Iniciando fase de DESPLIEGUE DE ESTRUCTURA...")
        deploy_fudo_database_structure(db_for_all_phases, 'sql/deploy_fudo_structure.sql')
        # ---------------------------------------------
        logger.info("Fase de DESPLIEGUE DE ESTRUCTURA completada.")

        # Paso 2: Ejecutar el ETL RAW completo y la fase de refresco de MVs
        logger.info("\nIniciando fase de EJECUCIÓN REGULAR del ETL (EXTRACCIÓN y TRANSFORMACIÓN)...")
        run_fudo_raw_etl(db_for_all_phases)
        logger.info("\n¡Proceso ETL de Fudo (Extracción y Transformación) finalizado!")

    except Exception as e:
        logger.critical(f"ERROR FATAL en el proceso ETL principal: {e}", exc_info=True)
        print(f"ERROR FATAL: {e}")
    finally:
        if db_for_all_phases:
            db_for_all_phases.close()
        logger.info("==================================================")
        logger.info("  FINALIZACIÓN COMPLETA DEL SCRIPT ETL.")
        logger.info("==================================================")