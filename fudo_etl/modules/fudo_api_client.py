# fudo-etl-project/fudo_api_client.py
import requests
import time
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class FudoApiClient:
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url
        self.auth_token = None
        self.max_retries = 10 
        self.initial_backoff_delay = 2 
        self.max_backoff_delay = 120 
        self.inter_page_delay = 0.5 
        
        # --- DEFINIR EXPLICITAMENTE QUÉ ENTIDADES SOPORTAN FILTROS INCREMENTALES POR 'createdAt' ---
        # Basado en las pruebas, solo 'sales' soporta 'filter[createdAt]'
        # Si la documentación de otras entidades mostrara un patrón 'filter[campo]=gte.', se añadiría aquí.
        self.incremental_filter_entities = {
            'sales': 'createdAt', 
            # Si descubrimos que 'customers' o 'items' u 'payments' o 'products' u otras
            # SÍ soportan 'filter[createdAt]' o similar, lo añadiríamos aquí.
            # Por ahora, se asume que solo 'sales' lo hace para evitar el 400 Bad Request.
        }
        # ----------------------------------------------------------------------------------------

    def set_auth_token(self, token: str):
        self.auth_token = token
        logger.debug("Token de autenticación establecido para FudoApiClient.")

    def get_data(self, entity_name: str, id_sucursal: str, last_extracted_ts: datetime = None) -> list[dict]:
        """
        Extrae datos paginados de una entidad específica de Fudo.
        Implementa filtros incrementales basados en self.incremental_filter_entities.
        Incluye lógica de reintento con backoff exponencial para errores 429 y 5xx.
        """
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
        
        # --- Lógica de Filtro Incremental Optimizada ---
        filter_field = self.incremental_filter_entities.get(entity_name)

        if last_extracted_ts and filter_field:
            formatted_ts = last_extracted_ts.isoformat(timespec='seconds')
            if not formatted_ts.endswith('Z'):
                formatted_ts += 'Z'
            
            params[f'filter[{filter_field}]'] = f"gte.{formatted_ts}"
            logger.info(f"  Aplicando filtro incremental '{filter_field} >= {formatted_ts}' para {entity_name}.")
        else:
            logger.info(f"  No se aplicó filtro incremental para '{entity_name}'. Realizando full load (primera vez o entidad no soporta filtro GTE).")


        # Bucle de paginación
        while True:
            current_params = params.copy()
            current_params['page[size]'] = page_size
            current_params['page[number]'] = page_number
            
            request_url = f"{self.api_base_url}/v1alpha1/{entity_name}" # Definir request_url aquí

            retries = 0
            delay = self.initial_backoff_delay
            
            while retries < self.max_retries:
                try:
                    logger.debug(f"  Solicitando: GET {request_url} con params: {current_params} (Intento {retries + 1}/{self.max_retries})")
                    response = requests.get(request_url, params=current_params, headers=headers, timeout=30)
                    response.raise_for_status()
                    
                    response_json = response.json()
                    items = response_json.get('data', []) # CORRECCIÓN: Usar 'data', no 'items'
                    
                    all_items.extend(items)
                    logger.debug(f"    Página {page_number} de '{entity_name}' para '{id_sucursal}' extraída: {len(items)} ítems.")
                    
                    if len(items) < page_size:
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