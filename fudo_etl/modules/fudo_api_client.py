# fudo_etl/modules/fudo_api_client.py
import requests
import time
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class FudoApiClient:
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url
        self.auth_token = None
        self.max_retries = 15
        self.initial_backoff_delay = 5
        self.max_backoff_delay = 300
        self.inter_page_delay = 1.0
        
        # --- ELIMINAR self.fields_key_mapping y self.fields_parameters ---
        # Ya no los usaremos para enviar el parámetro 'fields'
        # (Se asume que la API devuelve los detalles por defecto para la mayoría o tendremos que vivir con menos detalles para otras)
        # ------------------------------------------------------------------

        # --- DEFINIR QUÉ ENTIDADES SOPORTAN EXPLÍCITAMENTE FILTROS INCREMENTALES POR 'createdAt' ---
        # ¡SOLO 'sales' ha demostrado funcionar consistentemente!
        self.incremental_filter_entities = {
            'sales': 'createdAt',
        }
        # ----------------------------------------------------------------------------------------

    def set_auth_token(self, token: str):
        self.auth_token = token
        logger.debug("Token de autenticación establecido para FudoApiClient.")

    def get_data(self, entity_name: str, id_sucursal: str, last_extracted_ts: datetime = None) -> list[dict]:
        if not self.auth_token:
            raise ValueError("Token de autenticación no establecido para FudoApiClient. Asegúrate de llamar a set_auth_token primero.")

        all_items = []
        page_number = 1
        page_size = 500

        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Accept": "application/json"
        }
        
        params = {}
        
        # --- Lógica de Filtro Incremental (SOLO para 'sales') ---
        filter_field = self.incremental_filter_entities.get(entity_name)

        if last_extracted_ts and filter_field:
            formatted_ts = last_extracted_ts.astimezone(timezone.utc).isoformat(timespec='seconds')
            if not formatted_ts.endswith('Z'):
                formatted_ts += 'Z'
            params[f'filter[{filter_field}]'] = f"gte.{formatted_ts}"
            logger.info(f"  Aplicando filtro incremental '{filter_field} >= {formatted_ts}' para {entity_name}.")
        else:
            logger.info(f"  No se aplicó filtro incremental para '{entity_name}'. Realizando full load (primera vez o entidad no soporta filtro GTE).")

        # --- ELIMINAR LA LÓGICA PARA EL PARÁMETRO 'fields' ---
        # Ya no enviaremos 'fields' porque la API es impredecible y causa 400s.
        # Asumimos que la API devuelve lo que puede por defecto.
        logger.debug(f"  No se aplicará parámetro 'fields' para '{entity_name}' debido a la inconsistencia de la API.")
        # --------------------------------------------------------

        request_url = f"{self.api_base_url}/v1alpha1/{entity_name}"
        
        retries = 0
        delay = self.initial_backoff_delay
        
        while retries < self.max_retries:
            try:
                logger.debug(f"  Solicitando: GET {request_url} con params: {params} (Intento {retries + 1}/{self.max_retries})")
                response = requests.get(request_url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                
                response_json = response.json()
                items = response_json.get('data', [])
                
                all_items.extend(items)
                logger.debug(f"    Página {page_number} de '{entity_name}' para '{id_sucursal}' extraída: {len(items)} ítems.")
                
                if len(items) < page_size:
                    logger.info(f"  Total de {len(all_items)} ítems extraídos para '{entity_name}' de sucursal '{id_sucursal}'.")
                    return all_items
                
                page_number += 1
                time.sleep(self.inter_page_delay)
                break
            
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [429, 500, 502, 503, 504]:
                    retries += 1
                    logger.warning(f"  HTTP Error {e.response.status_code} para '{entity_name}' de sucursal '{id_sucursal}' (Pág {page_number}). Reintentando en {delay}s...")
                    time.sleep(delay)
                    delay = min(delay * 2, self.max_backoff_delay)
                elif e.response.status_code == 401:
                    logger.error("  Token probablemente expirado o inválido. No reintentar con este token.")
                    raise
                else:
                    logger.error(f"  HTTP Error no reintentable {e.response.status_code} para '{entity_name}' de sucursal '{id_sucursal}' (Pág {page_number}): {e.response.text}", exc_info=True)
                    raise
            except requests.exceptions.RequestException as e:
                retries += 1
                logger.warning(f"  Request Error para '{entity_name}' de sucursal '{id_sucursal}' (Pág {page_number}). Reintentando en {delay}s: {e}")
                time.sleep(delay)
                delay = min(delay * 2, self.max_backoff_delay)
            
        else:
            logger.error(f"  Máximo de reintentos ({self.max_retries}) alcanzado para '{entity_name}' de sucursal '{id_sucursal}' (Pág {page_number}). Abortando extracción para esta entidad.")
            raise ConnectionError(f"Fallo al extraer '{entity_name}' de sucursal '{id_sucursal}' después de {self.max_retries} reintentos.")
    
        logger.info(f"  Total de {len(all_items)} ítems extraídos para '{entity_name}' de sucursal '{id_sucursal}'.")
        return all_items# fudo_etl/modules/fudo_api_client.py
import requests
import time
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class FudoApiClient:
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url
        self.auth_token = None
        self.max_retries = 15
        self.initial_backoff_delay = 5
        self.max_backoff_delay = 300
        self.inter_page_delay = 1.0
        
        # --- Mapeo EXPLÍCITO: SOLO PARA ENTIDADES QUE SOPORTAN 'fields' ---
        # ¡SOLO 'expenses' confirmado hasta ahora!
        self.fields_key_mapping = {
            'expenses': 'expense',
        }
        
        self.fields_parameters = {
            'expense': 'amount,canceled,cashRegister,createdAt,date,description,dueDate,expenseCategory,expenseItems,paymentDate,paymentMethod,provider,receiptNumber,receiptType,status,useInCashCount,user',
        }
        # ----------------------------------------------------------------------------------------

        # --- DEFINIR QUÉ ENTIDADES SOPORTAN EXPLÍCITAMENTE FILTROS INCREMENTALES POR 'createdAt' ---
        # ¡SOLO 'sales' ha demostrado funcionar consistentemente!
        self.incremental_filter_entities = {
            'sales': 'createdAt',
        }
        # ----------------------------------------------------------------------------------------

    def set_auth_token(self, token: str):
        self.auth_token = token
        logger.debug("Token de autenticación establecido para FudoApiClient.")

    def get_data(self, entity_name: str, id_sucursal: str, last_extracted_ts: datetime = None) -> list[dict]:
        if not self.auth_token:
            raise ValueError("Token de autenticación no establecido para FudoApiClient. Asegúrate de llamar a set_auth_token primero.")

        all_items = []
        page_number = 1
        page_size = 500

        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Accept": "application/json"
        }
        
        params = {}
        
        # --- Lógica de Filtro Incremental (SOLO para 'sales') ---
        filter_field = self.incremental_filter_entities.get(entity_name)

        if last_extracted_ts and filter_field:
            formatted_ts = last_extracted_ts.astimezone(timezone.utc).isoformat(timespec='seconds')
            if not formatted_ts.endswith('Z'):
                formatted_ts += 'Z'
            params[f'filter[{filter_field}]'] = f"gte.{formatted_ts}"
            logger.info(f"  Aplicando filtro incremental '{filter_field} >= {formatted_ts}' para {entity_name}.")
        else:
            logger.info(f"  No se aplicó filtro incremental para '{entity_name}'. Realizando full load (primera vez o entidad no soporta filtro GTE).")

        # --- Lógica para el parámetro 'fields' (SOLO para 'expenses') ---
        fields_key = self.fields_key_mapping.get(entity_name)
        
        if fields_key and self.fields_parameters.get(fields_key):
            params[f'fields[{fields_key}]'] = self.fields_parameters[fields_key]
            logger.info(f"  Aplicando parámetro 'fields[{fields_key}]' para '{entity_name}'.")
        else:
            logger.debug(f"  No se aplicó parámetro 'fields' para '{entity_name}' (no soportado o no definido).")
        # --------------------------------------------------------

        request_url = f"{self.api_base_url}/v1alpha1/{entity_name}"
        
        retries = 0
        delay = self.initial_backoff_delay
        
        while retries < self.max_retries:
            try:
                logger.debug(f"  Solicitando: GET {request_url} con params: {params} (Intento {retries + 1}/{self.max_retries})")
                response = requests.get(request_url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                
                response_json = response.json()
                items = response_json.get('data', [])
                
                all_items.extend(items)
                logger.debug(f"    Página {page_number} de '{entity_name}' para '{id_sucursal}' extraída: {len(items)} ítems.")
                
                if len(items) < page_size:
                    logger.info(f"  Total de {len(all_items)} ítems extraídos para '{entity_name}' de sucursal '{id_sucursal}'.")
                    return all_items
                
                page_number += 1
                time.sleep(self.inter_page_delay)
                break
            
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [429, 500, 502, 503, 504]:
                    retries += 1
                    logger.warning(f"  HTTP Error {e.response.status_code} para '{entity_name}' de sucursal '{id_sucursal}' (Pág {page_number}). Reintentando en {delay}s...")
                    time.sleep(delay)
                    delay = min(delay * 2, self.max_backoff_delay)
                elif e.response.status_code == 401:
                    logger.error("  Token probablemente expirado o inválido. No reintentar con este token.")
                    raise
                else:
                    logger.error(f"  HTTP Error no reintentable {e.response.status_code} para '{entity_name}' de sucursal '{id_sucursal}' (Pág {page_number}): {e.response.text}", exc_info=True)
                    raise
            except requests.exceptions.RequestException as e:
                retries += 1
                logger.warning(f"  Request Error para '{entity_name}' de sucursal '{id_sucursal}' (Pág {page_number}). Reintentando en {delay}s: {e}")
                time.sleep(delay)
                delay = min(delay * 2, self.max_backoff_delay)
            
        else:
            logger.error(f"  Máximo de reintentos ({self.max_retries}) alcanzado para '{entity_name}' de sucursal '{id_sucursal}' (Pág {page_number}). Abortando extracción para esta entidad.")
            raise ConnectionError(f"Fallo al extraer '{entity_name}' de sucursal '{id_sucursal}' después de {self.max_retries} reintentos.")
    
        logger.info(f"  Total de {len(all_items)} ítems extraídos para '{entity_name}' de sucursal '{id_sucursal}'.")
        return all_items