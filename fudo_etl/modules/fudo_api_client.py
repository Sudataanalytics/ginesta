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
        self.max_retries = 20 # Aumentado retries para paginación
        self.initial_backoff_delay = 5 # Mayor delay inicial
        self.max_backoff_delay = 300 # Hasta 5 minutos
        self.inter_page_delay = 1.0 # Pausa entre páginas

        self.fields_key_mapping = {
            'customers': 'customer',
            'discounts': 'discount',
            'expenses': 'expense',
            'expense-categories': 'expenseCategory',
            'ingredients': 'ingredient',
            'items': 'item',
            'kitchens': 'kitchen',
            'payments': 'payment',
            'payment-methods': 'paymentMethod',
            'product-categories': 'productCategory',
            'product-modifiers': 'productModifier',
            'products': 'product',
            'roles': 'role',
            'rooms': 'room',
            'sales': 'sale',
            'tables': 'table',
            'users': 'user'
        }
        
        # --- DEFINIR QUÉ ENTIDADES SOPORTAN PARÁMETROS 'fields' Y CUÁLES SON ESOS CAMPOS ---
        self.fields_parameters = {
            'customers': 'active,address,comment,createdAt,discountPercentage,email,firstSaleDate,historicalSalesCount,historicalTotalSpent,houseAccountBalance,houseAccountEnabled,lastSaleDate,name,origin,phone,salesCount,vatNumber,paymentMethod',
            'discounts': 'amount,canceled,percentage,sale',
            'expenses': 'amount,canceled,cashRegister,createdAt,date,description,dueDate,expenseCategory,expenseItems,paymentDate,paymentMethod,provider,receiptNumber,receiptType,status,useInCashCount,user',
            'expense-categories': 'active,financialCategory,name,parentCategory',
            # 'ingredients': 'name,cost,stock,stockControl,ingredientCategory', # API lo da por defecto
            'items': 'canceled,cancellationComment,comment,cost,createdAt,price,product,quantity,sale,status,subitems,paid,priceList,lastStockCountAt',
            # 'kitchens': 'name', # API lo da por defecto
            'payments': 'amount,canceled,createdAt,external_reference,paymentMethod,sale',
            # 'payment-methods': 'name,active,code,position', # API lo da por defecto
            'product-categories': 'enableOnlineMenu,name,preparationTime,position,kitchen,parentCategory,products,taxes',
            # 'product-modifiers': 'maxQuantity,price,product,productModifiersGroup', # API lo da por defecto
            'products': 'active,code,cost,description,enableOnlineMenu,enableQrMenu,favourite,imageUrl,name,position,preparationTime,price,sellAlone,stock,stockControl,kitchen,productCategory,productModifiersGroups,productProportions,proportions,unit',
            # 'roles': 'isWaiter,isDeliveryman,name,permissions', # API lo da por defecto
            # 'rooms': 'name', # API lo da por defecto
            'sales': 'closedAt,comment,createdAt,people,customerName,anonymousCustomer,total,saleType,saleState,expectedPayments,commercialDocuments,customer,discounts,items,payments,tips,shippingCosts,table,waiter,saleIdentifier',
            # 'tables': 'column,number,row,shape,size,activeSales,room', # API lo da por defecto
            # 'users': 'active,admin,email,name,promotionalCode,role' # API lo da por defecto
        }

        # --- ENTIDADES QUE TIENEN FILTRO INCREMENTAL 'createdAt' ---
        self.incremental_filter_entities = {
            'sales': 'createdAt', 
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
        current_page_number = 1 # <--- USAR ESTO PARA LA PAGINACIÓN
        page_size = 500

        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Accept": "application/json"
        }
        
        params = {}
        
        # --- Lógica de Filtro Incremental ---
        filter_field = self.incremental_filter_entities.get(entity_name)

        if last_extracted_ts and filter_field:
            formatted_ts = last_extracted_ts.astimezone(timezone.utc).isoformat(timespec='seconds')
            if not formatted_ts.endswith('Z'):
                formatted_ts += 'Z'
            params[f'filter[{filter_field}]'] = f"gte.{formatted_ts}"
            logger.info(f"  Aplicando filtro incremental '{filter_field} >= {formatted_ts}' para {entity_name}.")
        else:
            logger.info(f"  No se aplicó filtro incremental para '{entity_name}' (last_extracted_ts es None o entidad no soporta filtro GTE).")

        # --- Lógica para el parámetro 'fields' ---
        fields_key_for_param = self.fields_key_mapping.get(entity_name)
        fields_to_request = self.fields_parameters.get(fields_key_for_param) # Usar fields_key_for_param para buscar en fields_parameters
        
        if fields_key_for_param and fields_to_request:
            params[f'fields[{fields_key_for_param}]'] = fields_to_request
            logger.info(f"  Aplicando parámetro 'fields[{fields_key_for_param}]' para '{entity_name}'.")
        else:
            # Loggear cuando no se aplica 'fields' porque no es soportado o no es necesario
            logger.debug(f"  No se aplicó parámetro 'fields' para '{entity_name}' (no configurado o devuelve detalles por defecto).")


        request_url = f"{self.api_base_url}/v1alpha1/{entity_name}"
        
        # --- Bucle de Paginación Robusto ---
        while True: 
            retries = 0
            delay = self.initial_backoff_delay
            
            while retries < self.max_retries:
                try:
                    current_params = params.copy()
                    current_params['page[size]'] = page_size
                    current_params['page[number]'] = current_page_number # <--- ¡USAR current_page_number!
                    
                    logger.debug(f"  Solicitando: GET {request_url} con params: {current_params} (Intento {retries + 1}/{self.max_retries}, Pág {current_page_number})")
                    response = requests.get(request_url, params=current_params, headers=headers, timeout=60) # Aumentar timeout
                    response.raise_for_status()
                    
                    response_json = response.json()
                    items_on_page = response_json.get('data', []) # Los ítems de esta página
                    
                    all_items.extend(items_on_page) # Acumula los ítems
                    logger.debug(f"    Página {current_page_number} de '{entity_name}' para '{id_sucursal}' extraída: {len(items_on_page)} ítems.")
                    
                    # --- Condición de SALIDA del bucle de paginación ---
                    if len(items_on_page) < page_size:
                        logger.info(f"  Paginación completa para '{entity_name}'. Última página con {len(items_on_page)} ítems. Total acumulado: {len(all_items)}.")
                        return all_items # <--- ¡Salida exitosa!
                    
                    # Si la página NO está vacía y tiene el tamaño máximo, hay más páginas
                    if len(items_on_page) == page_size:
                        current_page_number += 1 # <--- ¡Incrementa para la próxima iteración!
                        time.sleep(self.inter_page_delay)
                        break # Sale del bucle de reintentos para ir a la siguiente página
                    else: # Si len(items_on_page) es 0 pero page_size no es 0, algo raro pasó. O es el fin y es 0.
                        logger.warning(f"  Paginación inesperada para '{entity_name}' (Pág {current_page_number}): {len(items_on_page)} ítems < page_size ({page_size}), pero no se esperaban más datos o la página está vacía antes de tiempo.")
                        return all_items # Asumimos que es el fin de la paginación

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code in [429, 500, 502, 503, 504]:
                        retries += 1
                        logger.warning(f"  HTTP Error {e.response.status_code} para '{entity_name}' de sucursal '{id_sucursal}' (Pág {current_page_number}). Reintentando en {delay}s... ({retries}/{self.max_retries})")
                        time.sleep(delay)
                        delay = min(delay * 2, self.max_backoff_delay)
                    elif e.response.status_code == 401:
                        logger.error("  Token probablemente expirado o inválido (401). No reintentar con este token.")
                        raise
                    elif e.response.status_code == 400: # Capturar 400 Bad Request por 'fields' incorrecto
                        logger.error(f"  HTTP Error no reintentable 400 para '{entity_name}' de sucursal '{id_sucursal}' (Pág {current_page_number}). Detalle: {e.response.text}. Posiblemente 'fields' incorrecto o no soportado.", exc_info=True)
                        # Si es un 400 Bad Request persistente (no se reintenta), lo relanzamos
                        raise
                    else: # Otros errores 4xx no reintentables
                        logger.error(f"  HTTP Error no reintentable {e.response.status_code} para '{entity_name}' de sucursal '{id_sucursal}' (Pág {current_page_number}): {e.response.text}", exc_info=True)
                        raise
                except requests.exceptions.RequestException as e: # Errores de red, timeout, etc.
                    retries += 1
                    logger.warning(f"  Request Error para '{entity_name}' de sucursal '{id_sucursal}' (Pág {current_page_number}). Reintentando en {delay}s: {e} ({retries}/{self.max_retries})")
                    time.sleep(delay)
                    delay = min(delay * 2, self.max_backoff_delay)
                
            else: # Este else se ejecuta si el bucle `while retries < self.max_retries` se completa sin un 'break'
                logger.error(f"  Máximo de reintentos ({self.max_retries}) alcanzado para '{entity_name}' de sucursal '{id_sucursal}' (Pág {current_page_number}). Abortando extracción para esta entidad.")
                raise ConnectionError(f"Fallo al extraer '{entity_name}' de sucursal '{id_sucursal}' después de {self.max_retries} reintentos.")
        
        # Esta línea solo se alcanzaría si el bucle 'while True' se completa sin un 'return'
        # y el bucle de reintentos interno también se completó sin raise.
        # Esto NO debería ocurrir con la lógica actual de paginación/reintentos.
        logger.info(f"  Error lógico: Bucle de paginación interno terminó sin retornar o lanzar excepción. Total: {len(all_items)}.")
        return all_items # Retornar para evitar bucles infinitos en casos inesperados