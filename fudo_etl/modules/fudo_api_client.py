# fudo_etl/modules/fudo_api_client.py
import requests
import time
import logging
from datetime import datetime, timezone

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
        self.fields_key_mapping = {
            'expenses': 'expense',
        }
        
        self.fields_parameters = {
            'expense': (
                'amount,canceled,cashRegister,createdAt,date,description,dueDate,'
                'expenseCategory,expenseItems,paymentDate,paymentMethod,provider,'
                'receiptNumber,receiptType,status,useInCashCount,user'
            ),
        }

        # --- ENTIDADES CON FILTRO INCREMENTAL POR 'createdAt' ---
        self.incremental_filter_entities = {
            'sales': 'createdAt',
        }

    def set_auth_token(self, token: str):
        self.auth_token = token
        logger.debug("Token de autenticación establecido para FudoApiClient.")

    def get_data(self, entity_name: str, id_sucursal: str, last_extracted_ts: datetime = None) -> list[dict]:
        """
        Extrae datos paginados de la API de Fudo con control de reintentos y backoff exponencial.
        """
        if not self.auth_token:
            raise ValueError("Token de autenticación no establecido. Llama a set_auth_token primero.")

        all_items = []
        current_page = 1
        page_size = 500

        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Accept": "application/json"
        }

        params = {}

        # --- Lógica de filtro incremental ---
        filter_field = self.incremental_filter_entities.get(entity_name)
        if last_extracted_ts and filter_field:
            formatted_ts = last_extracted_ts.astimezone(timezone.utc).isoformat(timespec='seconds')
            if not formatted_ts.endswith('Z'):
                formatted_ts += 'Z'
            params[f'filter[{filter_field}]'] = f"gte.{formatted_ts}"
            logger.info(f"Aplicando filtro incremental '{filter_field} >= {formatted_ts}' para {entity_name}.")
        else:
            logger.info(f"No se aplicó filtro incremental para '{entity_name}' (full load o entidad no soporta filtro).")

        # --- Parámetros de 'fields' si aplica ---
        fields_key = self.fields_key_mapping.get(entity_name)
        if fields_key and self.fields_parameters.get(fields_key):
            params[f'fields[{fields_key}]'] = self.fields_parameters[fields_key]
            logger.info(f"Aplicando parámetro 'fields[{fields_key}]' para '{entity_name}'.")
        else:
            logger.debug(f"No se aplicó parámetro 'fields' para '{entity_name}' (no definido o no soportado).")

        request_url = f"{self.api_base_url}/v1alpha1/{entity_name}"

        # --- Bucle de paginación completa ---
        while True:
            retries = 0
            delay = self.initial_backoff_delay

            while retries < self.max_retries:
                try:
                    current_params = params.copy()
                    current_params['page[size]'] = page_size
                    current_params['page[number]'] = current_page

                    logger.debug(f"GET {request_url} params={current_params} (Intento {retries+1}/{self.max_retries}, pág {current_page})")
                    response = requests.get(request_url, params=current_params, headers=headers, timeout=60)
                    response.raise_for_status()

                    data = response.json().get('data', [])
                    logger.debug(f"Página {current_page}: {len(data)} ítems recuperados para '{entity_name}' ({id_sucursal}).")

                    all_items.extend(data)

                    # Condición de salida: si la página devuelve menos elementos que el tamaño solicitado
                    if not data or len(data) < page_size:
                        logger.info(f"Extracción completa: {len(all_items)} ítems totales para '{entity_name}' ({id_sucursal}).")
                        return all_items

                    current_page += 1
                    time.sleep(self.inter_page_delay)
                    break

                except requests.exceptions.HTTPError as e:
                    status = e.response.status_code
                    if status in [429, 500, 502, 503, 504]:
                        retries += 1
                        logger.warning(f"HTTP {status} en '{entity_name}' (pág {current_page}). Reintentando en {delay}s ({retries}/{self.max_retries})...")
                        time.sleep(delay)
                        delay = min(delay * 2, self.max_backoff_delay)
                    elif status == 401:
                        logger.error("Token expirado o inválido (401). No reintentar.")
                        raise
                    elif status == 400:
                        logger.error(f"Error 400: parámetro 'fields' o filtro inválido para '{entity_name}'. {e.response.text}", exc_info=True)
                        raise
                    else:
                        logger.error(f"HTTP {status} no reintentable en '{entity_name}' (pág {current_page}): {e.response.text}", exc_info=True)
                        raise

                except requests.exceptions.RequestException as e:
                    retries += 1
                    logger.warning(f"Error de conexión en '{entity_name}' (pág {current_page}). Reintentando en {delay}s ({retries}/{self.max_retries})... {e}")
                    time.sleep(delay)
                    delay = min(delay * 2, self.max_backoff_delay)

            else:
                logger.error(f"Máximo de reintentos ({self.max_retries}) alcanzado para '{entity_name}' ({id_sucursal}) en la pág {current_page}.")
                raise ConnectionError(f"Fallo al extraer '{entity_name}' tras {self.max_retries} reintentos.")

