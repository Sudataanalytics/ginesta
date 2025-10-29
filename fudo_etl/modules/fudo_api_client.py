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
        # Mantener solo las entidades que REALMENTE lo necesitan.
        self.fields_key_mapping = {
            'expenses': 'expense',
            'expense-categories': 'expenseCategory',
        }

        self.fields_parameters = {
            'expense': 'amount,canceled,cashRegister,createdAt,date,description,dueDate,expenseCategory,expenseItems,paymentDate,paymentMethod,provider,receiptNumber,receiptType,status,useInCashCount,user',
            'expenseCategory': ('active,financialCategory,name,parentCategory'),
        }

        # --- ENTIDADES CON FILTRO INCREMENTAL POR 'createdAt' (si la API lo soporta bien) ---
        self.incremental_filter_entities = {
            'sales': 'createdAt',
        }
        
        # --- CONFIGURACIÓN: Ventana de páginas recientes a re-procesar para 'sales' ---
        # Esto es para asegurar que capturamos cambios de estado en ventas recientes.
        self.sales_recent_pages_to_reprocess = 20 # Número de páginas más recientes a siempre traer (sin filtro de fecha)


    def set_auth_token(self, token: str):
        self.auth_token = token
        logger.debug("Token de autenticación establecido para FudoApiClient.")

    def get_data(self, entity_name: str, id_sucursal: str, last_extracted_ts: datetime = None) -> list[dict]:
        """
        Extrae datos paginados de la API de Fudo. Para 'sales', combina full load inicial,
        ventana deslizante para estados recientes e incremental para ventas nuevas.
        """
        if not self.auth_token:
            raise ValueError("Token de autenticación no establecido. Llama a set_auth_token primero.")

        request_url = f"{self.api_base_url}/v1alpha1/{entity_name}"
        page_size = 500
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Accept": "application/json"
        }

        # --- Lógica específica para 'sales' ---
        if entity_name == 'sales':
            if last_extracted_ts is None:
                logger.info(f"\n--- PRIMERA EJECUCIÓN: Extracción de '{entity_name}' para sucursal '{id_sucursal}' con CARGA HISTÓRICA COMPLETA. ---")
                # Realizar una extracción completa, sin límite de páginas ni filtro incremental
                all_sales = self._get_paginated_data_generic(
                    request_url=request_url,
                    headers=headers,
                    page_size=page_size,
                    entity_name=entity_name,
                    id_sucursal=id_sucursal,
                    apply_incremental_filter=False, # NO aplicar filtro de fecha
                    start_page=1,
                    max_pages=-1 # Sin límite de páginas = traer todo
                )
                logger.info(f"  Total de ventas únicas extraídas (carga completa): {len(all_sales)}")
                return all_sales
            else:
                logger.info(f"\n--- EJECUCIÓN REGULAR: Extracción de '{entity_name}' para sucursal '{id_sucursal}' con estrategia combinada (Ventana Reciente + Incremental). ---")
                
                # --- 1. Extracción de Ventana Deslizante (páginas más recientes, SIN filtro incremental) ---
                # Esto captura cambios de estado en ventas recientes que ya existían.
                logger.info(f"    Trayendo las últimas {self.sales_recent_pages_to_reprocess} páginas de ventas recientes (sin filtro incremental).")
                recent_sales = self._get_paginated_data_generic(
                    request_url=request_url,
                    headers=headers,
                    page_size=page_size,
                    entity_name=entity_name,
                    id_sucursal=id_sucursal,
                    apply_incremental_filter=False, # NO aplicar filtro de fecha aquí
                    start_page=1, # Siempre desde la página 1 para las recientes
                    max_pages=self.sales_recent_pages_to_reprocess # Limita el número de páginas
                )
                
                # --- 2. Extracción Incremental (nuevas ventas desde last_extracted_ts) ---
                # Esto captura ventas que son completamente nuevas.
                incremental_sales = []
                logger.info(f"    Trayendo ventas incrementales desde {last_extracted_ts} (filtrado por 'createdAt').")
                incremental_sales = self._get_paginated_data_generic(
                    request_url=request_url,
                    headers=headers,
                    page_size=page_size,
                    entity_name=entity_name,
                    id_sucursal=id_sucursal,
                    apply_incremental_filter=True, # SÍ aplicar filtro de fecha aquí
                    incremental_filter_ts=last_extracted_ts,
                    start_page=1, # Se aplica el filtro, así que puede empezar desde la página 1
                    max_pages=-1 # No hay límite de páginas para el incremental
                )
                
                # Combinar y eliminar duplicados (una venta puede aparecer en ambas listas si está en las páginas recientes Y es nueva)
                # Usamos un diccionario para mantener el último estado si hay duplicados por ID
                combined_items_dict = {item['id']: item for item in recent_sales}
                for item in incremental_sales:
                    combined_items_dict[item['id']] = item # Sobrescribirá si ya existe de 'recent_sales' con un ID, garantizando el más reciente
                
                logger.info(f"  Total de ventas únicas extraídas para '{entity_name}' ({id_sucursal}): {len(combined_items_dict)}")
                return list(combined_items_dict.values())
            
        else:
            # --- Lógica genérica para otras entidades (solo incremental o full load) ---
            logger.info(f"  Iniciando extracción de '{entity_name}' para sucursal '{id_sucursal}' con estrategia incremental/full.")
            return self._get_paginated_data_generic(
                request_url=request_url,
                headers=headers,
                page_size=page_size,
                entity_name=entity_name,
                id_sucursal=id_sucursal,
                apply_incremental_filter=bool(last_extracted_ts),
                incremental_filter_ts=last_extracted_ts,
                fields_key=self.fields_key_mapping.get(entity_name),
                fields_params=self.fields_parameters.get(self.fields_key_mapping.get(entity_name)),
                start_page=1,
                max_pages=-1
            )

    # --- MÉTODO AUXILIAR que encapsula la lógica de paginación y reintentos ---
    def _get_paginated_data_generic(self, request_url: str, headers: dict, page_size: int,
                                    entity_name: str, id_sucursal: str,
                                    apply_incremental_filter: bool, incremental_filter_ts: datetime = None,
                                    fields_key: str = None, fields_params: str = None,
                                    start_page: int = 1, max_pages: int = -1) -> list[dict]:
        """
        Método auxiliar para extraer datos paginados de la API de Fudo con control de reintentos y backoff exponencial.
        Más genérico y reutilizable.
        """
        all_items = []
        current_page = start_page
        
        params = {}
        # Aplicar filtro incremental si se solicita
        if apply_incremental_filter and incremental_filter_ts and self.incremental_filter_entities.get(entity_name):
            filter_field = self.incremental_filter_entities.get(entity_name)
            formatted_ts = incremental_filter_ts.astimezone(timezone.utc).isoformat(timespec='seconds')
            if not formatted_ts.endswith('Z'):
                formatted_ts += 'Z'
            params[f'filter[{filter_field}]'] = f"gte.{formatted_ts}"
            logger.debug(f"  Aplicando filtro incremental '{filter_field} >= {formatted_ts}' para {entity_name}.")
        
        # Aplicar parámetros 'fields' si se solicitan
        if fields_key and fields_params:
            params[f'fields[{fields_key}]'] = fields_params
            logger.debug(f"  Aplicando parámetro 'fields[{fields_key}]' para '{entity_name}'.")

        while True:
            # Control de límite de páginas
            if max_pages != -1 and current_page > (start_page - 1) + max_pages:
                logger.debug(f"  Alcanzado el límite de {max_pages} páginas para '{entity_name}'.")
                break
            
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
                        logger.debug(f"  Última página o página incompleta. Extracción de '{entity_name}' finalizada.")
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
        
        return all_items # Asegurarse de devolver los ítems si el bucle termina por max_pages