import logging
import json
from datetime import datetime, timezone
import uuid
import time
from hashlib import md5
import os

import psycopg2

# Tus módulos (importaciones directas/relativas a la raíz del WORKDIR /app en Docker)
# Dentro del contenedor Docker, 'main.py' está en /app, y 'modules' está en /app/modules
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
    logger.info("  Iniciando fase de Transformación (Creación/Refresco de MVs y Vistas RAW)")
    logger.info("==================================================")

    materialized_views_configs = [
        # MVs del DER (ya existentes)
        ('mv_sucursales', """
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_sucursales AS
            SELECT
                id_sucursal,
                sucursal_name AS sucursal
            FROM public.config_fudo_branches
            WHERE is_active = TRUE;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sucursales_id ON public.mv_sucursales (id_sucursal);
        """),
        ('mv_rubros', """
            DROP MATERIALIZED VIEW IF EXISTS public.mv_rubros CASCADE;
            CREATE MATERIALIZED VIEW public.mv_rubros AS
            SELECT DISTINCT ON (id_fudo, id_sucursal_fuente)
                (payload_json ->> 'id')::FLOAT::INTEGER AS id_rubro_fudo, -- <--- ¡CORRECCIÓN AQUÍ!
                id_sucursal_fuente AS id_sucursal,
                (payload_json ->> 'id') || '-' || id_sucursal_fuente AS rubro_key,
                (payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS rubro_name
            FROM public.fudo_raw_product_categories
            WHERE payload_json ->> 'id' IS NOT NULL AND payload_json -> 'attributes' ->> 'name' IS NOT NULL
            ORDER BY id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_rubros_rubro_key ON public.mv_rubros (rubro_key);
        """),
        ('mv_medio_pago', """
            DROP MATERIALIZED VIEW IF EXISTS public.mv_medio_pago CASCADE;
            CREATE MATERIALIZED VIEW public.mv_medio_pago AS
            SELECT DISTINCT ON (id_fudo, id_sucursal_fuente)
                (payload_json ->> 'id')::FLOAT::INTEGER AS id_payment_fudo, -- <--- ¡CORRECCIÓN AQUÍ!
                id_sucursal_fuente AS id_sucursal,
                (payload_json ->> 'id') || '-' || id_sucursal_fuente AS payment_method_key,
                (payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS payment_method
            FROM public.fudo_raw_payment_methods
            WHERE payload_json ->> 'id' IS NOT NULL AND payload_json -> 'attributes' ->> 'name' IS NOT NULL
            ORDER BY id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_medio_pago_payment_method_key ON public.mv_medio_pago (payment_method_key);
        """),
        ('mv_productos', """
            DROP MATERIALIZED VIEW IF EXISTS public.mv_productos CASCADE;
            CREATE MATERIALIZED VIEW public.mv_productos AS
            SELECT DISTINCT ON (p.id_fudo, p.id_sucursal_fuente)
                (p.payload_json ->> 'id')::FLOAT::INTEGER AS id_product_fudo, -- <--- ¡CORRECCIÓN AQUÍ!
                p.id_sucursal_fuente AS id_sucursal,
                (p.payload_json ->> 'id') || '-' || p.id_sucursal_fuente AS product_key,
                (p.payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS product_name,
                (p.payload_json -> 'relationships' -> 'productCategory' -> 'data' ->> 'id')::FLOAT::INTEGER AS id_rubro_fudo, -- <--- ¡CORRECCIÓN AQUÍ!
                (
                    (p.payload_json -> 'relationships' -> 'productCategory' -> 'data' ->> 'id') || '-' || p.id_sucursal_fuente
                ) AS rubro_key_fk
            FROM public.fudo_raw_products p
            WHERE p.payload_json ->> 'id' IS NOT NULL AND p.payload_json -> 'attributes' ->> 'name' IS NOT NULL
            ORDER BY p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc DESC;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_productos_product_key ON public.mv_productos (product_key);
        """),
        ('mv_sales_order', """
    DROP MATERIALIZED VIEW IF EXISTS public.mv_sales_order CASCADE;
    CREATE MATERIALIZED VIEW public.mv_sales_order AS
    SELECT DISTINCT ON (s.id_fudo, s.id_sucursal_fuente)
        (s.payload_json ->> 'id')::FLOAT::INTEGER AS id_order, -- <--- ¡CORRECCIÓN AQUÍ!
        s.id_sucursal_fuente AS id_sucursal,
        (s.payload_json ->> 'id') || '-' || s.id_sucursal_fuente AS order_key,
        0.0::FLOAT AS amount_tax,
        (s.payload_json -> 'attributes' ->> 'total')::FLOAT AS amount_total,
        (s.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS date_order,
        (s.payload_json -> 'attributes' ->> 'saleType') AS sale_type,
        (s.payload_json -> 'relationships' -> 'table' -> 'data' ->> 'id') AS table_id,
        (s.payload_json -> 'relationships' -> 'waiter' -> 'data' ->> 'id') AS waiter_id,
        (s.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
        (s.payload_json -> 'attributes' ->> 'closedAt')::TIMESTAMP WITH TIME ZONE AS closed_at
    FROM public.fudo_raw_sales s
    WHERE
        s.payload_json ->> 'id' IS NOT NULL AND
        s.id_sucursal_fuente IS NOT NULL AND
        (s.payload_json -> 'attributes' ->> 'createdAt') IS NOT NULL AND
        (s.payload_json -> 'attributes' ->> 'total') IS NOT NULL AND
        (s.payload_json -> 'attributes' ->> 'saleState') IS NOT NULL AND
        (s.payload_json -> 'attributes' ->> 'saleState') != 'CANCELED'
    ORDER BY s.id_fudo, s.id_sucursal_fuente, s.fecha_extraccion_utc DESC;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sales_order_order_key ON public.mv_sales_order (order_key);       
        """),
        ('mv_pagos', """
DROP MATERIALIZED VIEW IF EXISTS public.mv_pagos CASCADE;
CREATE MATERIALIZED VIEW public.mv_pagos AS
SELECT DISTINCT ON (p.id_fudo, p.id_sucursal_fuente)
    (p.payload_json ->> 'id')::FLOAT::INTEGER AS id,
    p.id_sucursal_fuente AS id_sucursal,
    (p.payload_json ->> 'id') || '-' || p.id_sucursal_fuente AS payment_key,
    (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::FLOAT::INTEGER AS pos_order_id,
    (p.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id')::FLOAT::INTEGER AS id_payment,
    (p.payload_json -> 'attributes' ->> 'amount')::FLOAT AS amount,
    
    CASE WHEN (p.payload_json -> 'relationships' -> 'expense' -> 'data' ->> 'id') IS NOT NULL THEN 'EXPENSE'
         WHEN (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') IS NOT NULL THEN 'SALE'
         ELSE 'OTHER' END AS transaction_type,
    CASE WHEN (p.payload_json -> 'relationships' -> 'expense' -> 'data' ->> 'id') IS NOT NULL THEN -((p.payload_json -> 'attributes' ->> 'amount')::FLOAT)
         ELSE (p.payload_json -> 'attributes' ->> 'amount')::FLOAT END AS signed_amount,

    (p.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS payment_date,
    (p.payload_json -> 'relationships' -> 'expense' -> 'data' ->> 'id') AS expense_id,
    (
        (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') || '-' || p.id_sucursal_fuente
    ) AS order_key_fk
FROM public.fudo_raw_payments p
LEFT JOIN public.fudo_raw_sales frs ON (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::FLOAT::INTEGER = (frs.payload_json ->> 'id')::FLOAT::INTEGER
                                    AND p.id_sucursal_fuente = frs.id_sucursal_fuente
WHERE p.payload_json ->> 'id' IS NOT NULL 
  AND (p.payload_json -> 'attributes' ->> 'amount') IS NOT NULL 
  AND (p.payload_json -> 'attributes' ->> 'createdAt') IS NOT NULL
  AND (p.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id') IS NOT NULL
  AND (p.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN IS NOT TRUE
ORDER BY p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc DESC;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_pagos_payment_key ON public.mv_pagos (payment_key);
        """),
        ('mv_sales_order_line', """
DROP MATERIALIZED VIEW IF EXISTS public.mv_sales_order_line CASCADE;
            CREATE MATERIALIZED VIEW public.mv_sales_order_line AS
            SELECT DISTINCT ON (i.id_fudo, i.id_sucursal_fuente)
                -- ID de la línea de orden (del ítem)
                (i.payload_json ->> 'id')::FLOAT::INTEGER AS id_order_line_fudo, -- <--- ¡Asegurarnos de que este esté!
                i.id_sucursal_fuente AS id_sucursal,
                (i.payload_json ->> 'id') || '-' || i.id_sucursal_fuente AS order_line_key,
                
                -- ID de la venta (del relationships.sale)
                (i.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::FLOAT::INTEGER AS id_order_fudo, -- <--- ¡Asegurarnos de que este esté!
                
                (
                    (i.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') || '-' || i.id_sucursal_fuente
                ) AS order_key_fk,
                (i.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS date_order_time,
                (i.payload_json -> 'attributes' ->> 'createdAt')::DATE AS date_order,
                
                -- ID del producto (del relationships.product)
                (i.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id')::FLOAT::INTEGER AS id_product_fudo, -- <--- ¡Asegurarnos de que este esté!
                
                (
                    (i.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id') || '-' || i.id_sucursal_fuente
                ) AS product_key_fk,
                
                -- Cantidad y precio original de la API (campos auxiliares)
                COALESCE(((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT), 0) AS qty_from_api,
                COALESCE(((i.payload_json -> 'attributes' ->> 'price')::FLOAT), 0) AS price_from_api,
                
                -- Precio unitario corregido
                CASE
                    WHEN COALESCE(((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT), 0) > 0 
                    THEN COALESCE(((i.payload_json -> 'attributes' ->> 'price')::FLOAT), 0) / COALESCE(((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT), 0)
                    ELSE 0.0
                END AS price_unit, 
                
                -- Cantidad final (siempre entero)
                COALESCE(((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT::INTEGER), 0) AS qty, -- Esta línea suele estar bien, pero si Fudo manda "3.0" aquí fallaría.
                                                                                               -- Si falla, cambiar a ::FLOAT::INTEGER también.
                
                -- Monto total de la línea
                (
                    CASE
                        WHEN COALESCE(((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT), 0) > 0 
                        THEN COALESCE(((i.payload_json -> 'attributes' ->> 'price')::FLOAT), 0) / COALESCE(((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT), 0)
                        ELSE 0.0
                    END
                ) * COALESCE(((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT), 0) AS amount_total
            FROM public.fudo_raw_items i
            WHERE i.payload_json ->> 'id' IS NOT NULL 
              AND (i.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') IS NOT NULL
              AND (i.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id') IS NOT NULL
              AND (i.payload_json -> 'attributes' ->> 'createdAt') IS NOT NULL
              AND (i.payload_json -> 'attributes' ->> 'price') IS NOT NULL
              AND (i.payload_json -> 'attributes' ->> 'quantity') IS NOT NULL
              AND (i.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN IS NOT TRUE
              AND COALESCE(((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT), 0) > 0
            ORDER BY i.id_fudo, i.id_sucursal_fuente, i.fecha_extraccion_utc DESC;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sales_order_line_order_line_key ON public.mv_sales_order_line (order_line_key);
        """),
          # --- AÑADIMOS EL NUEVO DER DE GASTOS ---
        # mv_expense_categories (NUEVA MV - CON ÍNDICE ÚNICO)
        ('mv_expense_categories', """
DROP MATERIALIZED VIEW IF EXISTS public.mv_expense_categories CASCADE;
CREATE MATERIALIZED VIEW public.mv_expense_categories AS
SELECT DISTINCT ON (ec.id_fudo, ec.id_sucursal_fuente)
    (ec.payload_json ->> 'id')::FLOAT::INTEGER AS id_expense_category,
    (ec.payload_json -> 'attributes' ->> 'name') AS expense_category_name,
    (ec.payload_json -> 'attributes' ->> 'financialCategory') AS financial_category,
    (ec.payload_json -> 'attributes' ->> 'active')::BOOLEAN AS active,
    (ec.payload_json -> 'relationships' -> 'parentCategory' -> 'data' ->> 'id') AS parent_category_id,
    ec.id_sucursal_fuente AS id_sucursal,
    (ec.payload_json ->> 'id') || '-' || ec.id_sucursal_fuente AS expense_category_key
FROM public.fudo_raw_expense_categories ec
WHERE (ec.payload_json ->> 'id') IS NOT NULL 
  AND (ec.payload_json -> 'attributes' ->> 'name') IS NOT NULL
  AND ec.id_sucursal_fuente IS NOT NULL
ORDER BY ec.id_fudo, ec.id_sucursal_fuente, ec.fecha_extraccion_utc DESC;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_expense_categories_key ON public.mv_expense_categories (expense_category_key);
        """),

        # mv_expenses (NUEVA MV - CON ÍNDICE ÚNICO)
        ('mv_expenses', """
           DROP MATERIALIZED VIEW IF EXISTS public.mv_expenses CASCADE;
            CREATE MATERIALIZED VIEW public.mv_expenses AS
            SELECT DISTINCT ON (e.id_fudo, e.id_sucursal_fuente)
                (e.payload_json ->> 'id')::FLOAT::INTEGER AS id_expense,
                e.id_sucursal_fuente AS id_sucursal,
                (e.payload_json -> 'attributes' ->> 'amount')::FLOAT AS amount,
                (e.payload_json -> 'attributes' ->> 'description') AS description,
                (e.payload_json -> 'attributes' ->> 'date')::TIMESTAMP WITH TIME ZONE AS expense_date,
                (e.payload_json -> 'attributes' ->> 'status') AS status,
                (e.payload_json -> 'attributes' ->> 'dueDate')::TIMESTAMP WITH TIME ZONE AS due_date,
                (e.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN AS canceled,
                (e.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
                (e.payload_json -> 'attributes' ->> 'paymentDate')::TIMESTAMP WITH TIME ZONE AS payment_date,
                (e.payload_json -> 'attributes' ->> 'receiptNumber') AS receipt_number,
                (e.payload_json -> 'attributes' ->> 'useInCashCount')::BOOLEAN AS use_in_cash_count,
                (e.payload_json -> 'relationships' -> 'user' -> 'data' ->> 'id') AS user_id,
                (e.payload_json -> 'relationships' -> 'provider' -> 'data' ->> 'id') AS provider_id,
                (e.payload_json -> 'relationships' -> 'receiptType' -> 'data' ->> 'id') AS receipt_type_id,
                (e.payload_json -> 'relationships' -> 'cashRegister' -> 'data' ->> 'id') AS cash_register_id,
                (e.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id') AS payment_method_id,
                (e.payload_json -> 'relationships' -> 'expenseCategory' -> 'data' ->> 'id') AS expense_category_id,
                (e.payload_json -> 'relationships' -> 'expenseCategory' -> 'data' ->> 'id') || '-' || e.id_sucursal_fuente AS expense_category_key
            FROM public.fudo_raw_expenses e
            WHERE (e.payload_json ->> 'id') IS NOT NULL
              AND e.id_sucursal_fuente IS NOT NULL
            ORDER BY e.id_fudo, e.id_sucursal_fuente, e.fecha_extraccion_utc DESC;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_expenses_id_sucursal ON public.mv_expenses (id_expense, id_sucursal); 
        """),
        ('mv_product_categories_details', """
DROP MATERIALIZED VIEW IF EXISTS public.mv_product_categories_details CASCADE;
CREATE MATERIALIZED VIEW public.mv_product_categories_details AS
SELECT DISTINCT ON (pc.id_fudo, pc.id_sucursal_fuente)
    (pc.payload_json ->> 'id')::FLOAT::INTEGER AS id_product_category,
    (pc.payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS product_category_name,
    (pc.payload_json -> 'attributes' ->> 'position')::INTEGER AS "position",
    (pc.payload_json -> 'attributes' ->> 'preparationTime')::INTEGER AS preparation_time,
    (pc.payload_json -> 'attributes' ->> 'enableOnlineMenu')::BOOLEAN AS enable_online_menu,
    (pc.payload_json -> 'relationships' -> 'kitchen' -> 'data' ->> 'id') AS kitchen_id,
    (pc.payload_json -> 'relationships' -> 'parentCategory' -> 'data' ->> 'id') AS parent_category_id,
    pc.id_sucursal_fuente AS id_sucursal,
    (pc.payload_json ->> 'id') || '-' || pc.id_sucursal_fuente AS product_category_key
FROM public.fudo_raw_product_categories pc
WHERE (pc.payload_json ->> 'id') IS NOT NULL 
  AND (pc.payload_json -> 'attributes' ->> 'name') IS NOT NULL
  AND pc.id_sucursal_fuente IS NOT NULL
ORDER BY pc.id_fudo, pc.id_sucursal_fuente, pc.fecha_extraccion_utc DESC;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_product_categories_key ON public.mv_product_categories_details (product_category_key);
        """),
    ]

    raw_views_configs = [
        # fudo_view_raw_customers
         ('fudo_view_raw_customers', """
            DROP VIEW IF EXISTS public.fudo_view_raw_customers;
            CREATE OR REPLACE VIEW public.fudo_view_raw_customers AS
            SELECT
                c.id_fudo, c.id_sucursal_fuente, c.fecha_extraccion_utc, c.payload_checksum,
                (c.payload_json ->> 'id') AS customer_id, (c.payload_json -> 'attributes' ->> 'active')::BOOLEAN AS active,
                (c.payload_json -> 'attributes' ->> 'address') AS address,
                (c.payload_json -> 'attributes' ->> 'comment') AS comment,
                (c.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
                (c.payload_json -> 'attributes' ->> 'discountPercentage')::FLOAT AS discount_percentage,
                (c.payload_json -> 'attributes' ->> 'email') AS email,
                (c.payload_json -> 'attributes' ->> 'firstSaleDate')::TIMESTAMP WITH TIME ZONE AS first_sale_date,
                (c.payload_json -> 'attributes' ->> 'historicalSalesCount')::INTEGER AS historical_sales_count,
                (c.payload_json -> 'attributes' ->> 'historicalTotalSpent')::FLOAT AS historical_total_spent,
                (c.payload_json -> 'attributes' ->> 'houseAccountBalance')::FLOAT AS house_account_balance,
                (c.payload_json -> 'attributes' ->> 'houseAccountEnabled')::BOOLEAN AS house_account_enabled,
                (c.payload_json -> 'attributes' ->> 'lastSaleDate')::TIMESTAMP WITH TIME ZONE AS last_sale_date,
                (c.payload_json -> 'attributes' ->> 'name') AS customer_name,
                (c.payload_json -> 'attributes' ->> 'origin') AS origin,
                (c.payload_json -> 'attributes' ->> 'phone') AS phone,
                (c.payload_json -> 'attributes' ->> 'salesCount')::INTEGER AS sales_count,
                (c.payload_json -> 'attributes' ->> 'vatNumber') AS vat_number,
                (c.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id') AS payment_method_id,
                c.payload_json AS original_payload
            FROM public.fudo_raw_customers c
            ORDER BY c.id_fudo, c.id_sucursal_fuente, c.fecha_extraccion_utc DESC;
        """),
        # fudo_view_raw_discounts (¡ACTUALIZADA!)
        ('fudo_view_raw_discounts', """
            DROP VIEW IF EXISTS public.fudo_view_raw_discounts;
            CREATE OR REPLACE VIEW public.fudo_view_raw_discounts AS
            SELECT
                d.id_fudo, d.id_sucursal_fuente, d.fecha_extraccion_utc, d.payload_checksum,
                (d.payload_json ->> 'id') AS discount_id, (d.payload_json -> 'attributes' ->> 'amount')::FLOAT AS amount,
                (d.payload_json -> 'attributes' ->> 'percentage')::FLOAT AS percentage,
                (d.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN AS canceled,
                (d.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') AS sale_id,
                d.payload_json AS original_payload
            FROM public.fudo_raw_discounts d
            ORDER BY d.id_fudo, d.id_sucursal_fuente, d.fecha_extraccion_utc DESC;
        """),
        # fudo_view_raw_expenses (¡ACTUALIZADA!)
        ('fudo_view_raw_expenses', """
            DROP VIEW IF EXISTS public.fudo_view_raw_expenses;
            CREATE OR REPLACE VIEW public.fudo_view_raw_expenses AS
            SELECT
                e.id_fudo, e.id_sucursal_fuente, e.fecha_extraccion_utc, e.payload_checksum,
                (e.payload_json ->> 'id') AS expense_id,
                (e.payload_json -> 'attributes' ->> 'amount')::FLOAT AS amount,
                (e.payload_json -> 'attributes' ->> 'description') AS description,
                (e.payload_json -> 'attributes' ->> 'date')::TIMESTAMP WITH TIME ZONE AS expense_date,
                (e.payload_json -> 'attributes' ->> 'status') AS status,
                (e.payload_json -> 'attributes' ->> 'dueDate')::TIMESTAMP WITH TIME ZONE AS due_date,
                (e.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN AS canceled,
                (e.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
                (e.payload_json -> 'attributes' ->> 'paymentDate')::TIMESTAMP WITH TIME ZONE AS payment_date,
                (e.payload_json -> 'attributes' ->> 'receiptNumber') AS receipt_number,
                (e.payload_json -> 'attributes' ->> 'useInCashCount')::BOOLEAN AS use_in_cash_count,
                (e.payload_json -> 'relationships' -> 'user' -> 'data' ->> 'id') AS user_id,
                (e.payload_json -> 'relationships' -> 'provider' -> 'data' ->> 'id') AS provider_id,
                (e.payload_json -> 'relationships' -> 'receiptType' -> 'data' ->> 'id') AS receipt_type_id,
                (e.payload_json -> 'relationships' -> 'cashRegister' -> 'data' ->> 'id') AS cash_register_id,
                (e.payload_json -> 'relationships' -> 'expenseItems' -> 'data') AS expense_items, -- Mantener como JSONB array
                (e.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id') AS payment_method_id,
                (e.payload_json -> 'relationships' -> 'expenseCategory' -> 'data' ->> 'id') AS expense_category_id,
                e.payload_json AS original_payload
            FROM public.fudo_raw_expenses e
            ORDER BY e.id_fudo, e.id_sucursal_fuente, e.fecha_extraccion_utc DESC;
        """),
        # fudo_view_raw_expense_categories (Basado en ejemplo, sin 'fields')
        ('fudo_view_raw_expense_categories', """
            DROP VIEW IF EXISTS public.fudo_view_raw_expense_categories;
            CREATE OR REPLACE VIEW public.fudo_view_raw_expense_categories AS
            SELECT
                ec.id_fudo, ec.id_sucursal_fuente, ec.fecha_extraccion_utc, ec.payload_checksum,
                (ec.payload_json ->> 'id') AS category_id,
                (ec.payload_json -> 'attributes' ->> 'name') AS category_name, -- CORREGIDO, era category_active
                (ec.payload_json -> 'attributes' ->> 'active')::BOOLEAN AS active,
                (ec.payload_json -> 'attributes' ->> 'financialCategory') AS financial_category,
                (ec.payload_json -> 'relationships' -> 'parentCategory' -> 'data' ->> 'id') AS parent_category_id,
                ec.payload_json AS original_payload
            FROM public.fudo_raw_expense_categories ec
            ORDER BY ec.id_fudo, ec.id_sucursal_fuente, ec.fecha_extraccion_utc DESC;
        """),
        # fudo_view_raw_ingredients (¡ACTUALIZADA!)
        ('fudo_view_raw_ingredients', """
            DROP VIEW IF EXISTS public.fudo_view_raw_ingredients;
            CREATE OR REPLACE VIEW public.fudo_view_raw_ingredients AS
            SELECT
                i.id_fudo, i.id_sucursal_fuente, i.fecha_extraccion_utc, i.payload_checksum,
                (i.payload_json ->> 'id') AS ingredient_id, (i.payload_json -> 'attributes' ->> 'name') AS ingredient_name,
                (i.payload_json -> 'attributes' ->> 'cost')::FLOAT AS cost,
                (i.payload_json -> 'attributes' ->> 'stock')::FLOAT AS stock,
                (i.payload_json -> 'attributes' ->> 'stockControl')::BOOLEAN AS stock_control,
                (i.payload_json -> 'relationships' -> 'ingredientCategory' -> 'data' ->> 'id') AS ingredient_category_id,
                i.payload_json AS original_payload
            FROM public.fudo_raw_ingredients i
            ORDER BY i.id_fudo, i.id_sucursal_fuente, i.fecha_extraccion_utc DESC;
        """),
        #fudo_view_raw_items (¡ACTUALIZADA con campos completos!)
        ('fudo_view_raw_items', """
            drop view if exists public.fudo_view_raw_items CASCADE;
            CREATE OR REPLACE VIEW public.fudo_view_raw_items AS
            SELECT
                i.id_fudo, i.id_sucursal_fuente, i.fecha_extraccion_utc, i.payload_checksum,
                (i.payload_json ->> 'id') AS item_id,
                (i.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN AS canceled,
                (i.payload_json -> 'attributes' ->> 'cancellationComment') AS cancellation_comment,
                (i.payload_json -> 'attributes' ->> 'comment') AS comment,
                (i.payload_json -> 'attributes' ->> 'cost')::FLOAT AS cost,
                (i.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
                (i.payload_json -> 'attributes' ->> 'price')::FLOAT AS price,
                (i.payload_json -> 'attributes' ->> 'quantity')::FLOAT AS quantity,
                (i.payload_json -> 'attributes' ->> 'status') AS status,
                (i.payload_json -> 'attributes' ->> 'paid')::BOOLEAN AS paid,
                (i.payload_json -> 'attributes' ->> 'lastStockCountAt')::TIMESTAMP WITH TIME ZONE AS last_stock_count_at,
                (i.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id') AS product_id,
                (i.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') AS sale_id,
                (i.payload_json -> 'relationships' -> 'priceList' -> 'data' ->> 'id') AS price_list_id,
                (i.payload_json -> 'relationships' -> 'subitems' -> 'data') AS subitems_data,
                i.payload_json AS original_payload
            FROM public.fudo_raw_items i
            ORDER BY i.id_fudo, i.id_sucursal_fuente, i.fecha_extraccion_utc DESC;
        """),
        # fudo_view_raw_kitchens (Basado en ejemplo, sin 'fields')
        ('fudo_view_raw_kitchens', """
            DROP VIEW IF EXISTS public.fudo_view_raw_kitchens;
            CREATE OR REPLACE VIEW public.fudo_view_raw_kitchens AS
            SELECT
                k.id_fudo, k.id_sucursal_fuente, k.fecha_extraccion_utc, k.payload_checksum,
                (k.payload_json ->> 'id') AS kitchen_id,
                (k.payload_json -> 'attributes' ->> 'name') AS kitchen_name,
                k.payload_json AS original_payload
            FROM public.fudo_raw_kitchens k
            ORDER BY k.id_fudo, k.id_sucursal_fuente, k.fecha_extraccion_utc DESC;
        """),
        
        #fudo_view_raw_payments (¡NUEVA VISTA RAW DESNORMALIZADA!)
        ('fudo_view_raw_payments', """
            drop view if exists public.fudo_view_raw_payments CASCADE;
            CREATE OR REPLACE VIEW public.fudo_view_raw_payments AS
            SELECT
                p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc, p.payload_checksum,
                (p.payload_json ->> 'id') AS payment_id,
                (p.payload_json -> 'attributes' ->> 'amount')::FLOAT AS amount,
                (p.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN AS canceled,
                (p.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
                (p.payload_json -> 'attributes' ->> 'externalReference') AS external_reference,
                (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') AS sale_id,
                (p.payload_json -> 'relationships' -> 'expense' -> 'data' ->> 'id') AS expense_id,
                (p.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id') AS payment_method_id,
                p.payload_json AS original_payload
            FROM public.fudo_raw_payments p
            ORDER BY p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc DESC;
        """),
        #fudo_view_raw_products (¡ACTUALIZADA con campos completos!)
        ('fudo_view_raw_products', """
            drop view if exists public.fudo_view_raw_products CASCADE;
            CREATE OR REPLACE VIEW public.fudo_view_raw_products AS
            SELECT
                p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc, p.payload_checksum,
                (p.payload_json ->> 'id') AS product_id,
                (p.payload_json -> 'attributes' ->> 'active')::BOOLEAN AS active,
                (p.payload_json -> 'attributes' ->> 'code') AS code,
                (p.payload_json -> 'attributes' ->> 'cost')::FLOAT AS cost,
                (p.payload_json -> 'attributes' ->> 'description') AS description,
                (p.payload_json -> 'attributes' ->> 'enableOnlineMenu')::BOOLEAN AS enable_online_menu,
                (p.payload_json -> 'attributes' ->> 'enableQrMenu')::BOOLEAN AS enable_qr_menu,
                (p.payload_json -> 'attributes' ->> 'favourite')::BOOLEAN AS favourite,
                (p.payload_json -> 'attributes' ->> 'imageUrl') AS image_url,
                (p.payload_json -> 'attributes' ->> 'name') AS product_name,
                (p.payload_json -> 'attributes' ->> 'position')::INTEGER AS "position",
                (p.payload_json -> 'attributes' ->> 'preparationTime')::INTEGER AS preparation_time,
                (p.payload_json -> 'attributes' ->> 'price')::FLOAT AS price,
                (p.payload_json -> 'attributes' ->> 'sellAlone')::BOOLEAN AS sell_alone,
                (p.payload_json -> 'attributes' ->> 'stock')::FLOAT AS stock,
                (p.payload_json -> 'attributes' ->> 'stockControl')::BOOLEAN AS stock_control,
                (p.payload_json -> 'relationships' -> 'kitchen' -> 'data' ->> 'id') AS kitchen_id,
                (p.payload_json -> 'relationships' -> 'productCategory' -> 'data' ->> 'id') AS product_category_id,
                (p.payload_json -> 'relationships' -> 'productModifiersGroups' -> 'data') AS product_modifiers_groups,
                (p.payload_json -> 'relationships' -> 'productProportions' -> 'data') AS product_proportions,
                p.payload_json AS original_payload
            FROM public.fudo_raw_products p
            ORDER BY p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc DESC;
        """),
        
        # fudo_view_raw_product_modifiers (Basado en ejemplo, sin 'fields')
        ('fudo_view_raw_product_modifiers', """
            DROP VIEW IF EXISTS public.fudo_view_raw_product_modifiers;
            CREATE OR REPLACE VIEW public.fudo_view_raw_product_modifiers AS
            SELECT
                pm.id_fudo, pm.id_sucursal_fuente, pm.fecha_extraccion_utc, pm.payload_checksum,
                (pm.payload_json ->> 'id') AS modifier_id, (pm.payload_json -> 'attributes' ->> 'maxQuantity')::INTEGER AS max_quantity,
                (pm.payload_json -> 'attributes' ->> 'price')::FLOAT AS price,
                (pm.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id') AS product_id,
                (pm.payload_json -> 'relationships' -> 'productModifiersGroup' -> 'data' ->> 'id') AS product_modifiers_group_id,
                pm.payload_json AS original_payload
            FROM public.fudo_raw_product_modifiers pm
            ORDER BY pm.id_fudo, pm.id_sucursal_fuente, pm.fecha_extraccion_utc DESC;
        """),
        ('fudo_view_raw_roles', """
            DROP VIEW IF EXISTS public.fudo_view_raw_roles;
            CREATE OR REPLACE VIEW public.fudo_view_raw_roles AS
            SELECT
                r.id_fudo, r.id_sucursal_fuente, r.fecha_extraccion_utc, r.payload_checksum,
                (r.payload_json ->> 'id') AS role_id, (r.payload_json -> 'attributes' ->> 'isWaiter')::BOOLEAN AS is_waiter,
                (r.payload_json -> 'attributes' ->> 'isDeliveryman')::BOOLEAN AS is_deliveryman,
                (r.payload_json -> 'attributes' ->> 'name') AS role_name,
                (r.payload_json -> 'attributes' -> 'permissions') AS permissions,
                r.payload_json AS original_payload
            FROM public.fudo_raw_roles r
            ORDER BY r.id_fudo, r.id_sucursal_fuente, r.fecha_extraccion_utc DESC;
        """),
        ('fudo_view_raw_rooms', """
            DROP VIEW IF EXISTS public.fudo_view_raw_rooms;
            CREATE OR REPLACE VIEW public.fudo_view_raw_rooms AS
            SELECT
                r.id_fudo, r.id_sucursal_fuente, r.fecha_extraccion_utc, r.payload_checksum,
                (r.payload_json ->> 'id') AS room_id, (r.payload_json -> 'attributes' ->> 'name') AS room_name,
                (r.payload_json -> 'relationships' -> 'tables' -> 'data') AS table_ids,
                r.payload_json AS original_payload
            FROM public.fudo_raw_rooms r
            ORDER BY r.id_fudo, r.id_sucursal_fuente, r.fecha_extraccion_utc DESC;
        """),
        ('fudo_view_raw_tables', """
            DROP VIEW IF EXISTS public.fudo_view_raw_tables;
            CREATE OR REPLACE VIEW public.fudo_view_raw_tables AS
            SELECT
                t.id_fudo, t.id_sucursal_fuente, t.fecha_extraccion_utc, t.payload_checksum,
                (t.payload_json ->> 'id') AS table_id, (t.payload_json -> 'attributes' ->> 'column')::INTEGER AS "column",
                (t.payload_json -> 'attributes' ->> 'number')::INTEGER AS table_number,
                (t.payload_json -> 'attributes' ->> 'row')::INTEGER AS "row",
                (t.payload_json -> 'attributes' ->> 'shape') AS shape,
                (t.payload_json -> 'attributes' ->> 'size') AS size,
                (t.payload_json -> 'relationships' -> 'room' -> 'data' ->> 'id') AS room_id,
                t.payload_json AS original_payload
            FROM public.fudo_raw_tables t
            ORDER BY t.id_fudo, t.id_sucursal_fuente, t.fecha_extraccion_utc DESC;
        """),
        ('fudo_view_raw_users', """
            DROP VIEW IF EXISTS public.fudo_view_raw_users;
            CREATE OR REPLACE VIEW public.fudo_view_raw_users AS
            SELECT
                u.id_fudo, u.id_sucursal_fuente, u.fecha_extraccion_utc, u.payload_checksum,
                (u.payload_json ->> 'id') AS user_id, (u.payload_json -> 'attributes' ->> 'active')::BOOLEAN AS active,
                (u.payload_json -> 'attributes' ->> 'admin')::BOOLEAN AS admin,
                (u.payload_json -> 'attributes' ->> 'email') AS email, (u.payload_json -> 'attributes' ->> 'name') AS user_name,
                (u.payload_json -> 'attributes' ->> 'promotionalCode') AS promotional_code,
                (u.payload_json -> 'relationships' -> 'role' -> 'data' ->> 'id') AS role_id,
                u.payload_json AS original_payload
            FROM public.fudo_raw_users u
            ORDER BY u.id_fudo, u.id_sucursal_fuente, u.fecha_extraccion_utc DESC;
        """),

        #fudo_view_raw_sales (¡NUEVA VISTA RAW DESNORMALIZADA con todos los campos del JSON!)
        ('fudo_view_raw_sales', """
            drop view if exists public.fudo_view_raw_sales CASCADE;
            CREATE OR REPLACE VIEW public.fudo_view_raw_sales AS
            SELECT
                s.id_fudo, s.id_sucursal_fuente, s.fecha_extraccion_utc, s.payload_checksum,
                (s.payload_json ->> 'id') AS sale_id,
                (s.payload_json -> 'attributes' ->> 'closedAt')::TIMESTAMP WITH TIME ZONE AS closed_at,
                (s.payload_json -> 'attributes' ->> 'comment') AS comment,
                (s.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
                (s.payload_json -> 'attributes' ->> 'people')::INTEGER AS people,
                (s.payload_json -> 'attributes' ->> 'customerName') AS customer_name,
                (s.payload_json -> 'attributes' ->> 'total')::FLOAT AS total_amount,
                (s.payload_json -> 'attributes' ->> 'saleType') AS sale_type,
                (s.payload_json -> 'attributes' ->> 'saleState') AS sale_state,
                (s.payload_json -> 'attributes' -> 'anonymousCustomer' ->> 'name') AS anonymous_customer_name,
                (s.payload_json -> 'attributes' -> 'anonymousCustomer' ->> 'phone') AS anonymous_customer_phone,
                (s.payload_json -> 'attributes' -> 'anonymousCustomer' ->> 'address') AS anonymous_customer_address,
                (s.payload_json -> 'attributes' -> 'expectedPayments') AS expected_payments,
                (s.payload_json -> 'relationships' -> 'customer' -> 'data' ->> 'id') AS customer_id,
                (s.payload_json -> 'relationships' -> 'discounts' -> 'data') AS discounts_data,
                (s.payload_json -> 'relationships' -> 'items' -> 'data') AS items_data,
                (s.payload_json -> 'relationships' -> 'payments' -> 'data') AS payments_data,
                (s.payload_json -> 'relationships' -> 'tips' -> 'data') AS tips_data,
                (s.payload_json -> 'relationships' -> 'shippingCosts' -> 'data') AS shipping_costs_data,
                (s.payload_json -> 'relationships' -> 'table' -> 'data' ->> 'id') AS table_id,
                (s.payload_json -> 'relationships' -> 'waiter' -> 'data' ->> 'id') AS waiter_id,
                (s.payload_json -> 'relationships' -> 'saleIdentifier' -> 'data' ->> 'id') AS sale_identifier_id,
                s.payload_json AS original_payload
            FROM public.fudo_raw_sales s
            ORDER BY s.id_fudo, s.id_sucursal_fuente, s.fecha_extraccion_utc DESC;
        """),
    ]

    # Iterar para crear/reemplazar las vistas RAW
    for view_name, create_sql in raw_views_configs:
        logger.info(f"  Procesando Vista RAW Desnormalizada: '{view_name}'...")
        try:
            db_manager.execute_query(create_sql)  # Ejecuta el CREATE OR REPLACE VIEW
            logger.info(f"  Vista RAW '{view_name}' creada/reemplazada exitosamente.")
        except Exception as e:
            logger.error(f"  ERROR al procesar la Vista RAW '{view_name}': {e}", exc_info=True)
            continue  # Continuar con las siguientes vistas aunque esta falle

    # Luego, las MVs del DER (ya existentes y se refrescan)
    for mv_name, create_sql in materialized_views_configs: # Usa materialized_views_configs aquí
        logger.info(f"  Procesando Vista Materializada: '{mv_name}'...")
        try:
            # Intentar crear la MV si no existe
            logger.info(f"    Intentando crear MV '{mv_name}' si no existe...")
            db_manager.execute_query(create_sql) # Ejecuta el CREATE MV IF NOT EXISTS
            logger.info(f"    MV '{mv_name}' creada/existente.")

            # --- CORRECCIÓN CRÍTICA AQUÍ: Usar REFRESH CONCURRENTLY ---
            logger.info(f"    Refrescando MV '{mv_name}' CONCURRENTLY...")
            db_manager.execute_query(f"REFRESH MATERIALIZED VIEW CONCURRENTLY public.{mv_name};")
            logger.info(f"    MV '{mv_name}' refrescada exitosamente.")
            # --------------------------------------------------------

        except psycopg2.errors.LockNotAvailable as e:
            logger.warning(f"  Advertencia: No se pudo adquirir bloqueo para REFRESH CONCURRENTLY de '{mv_name}'. Intentando REFRESH normal. Error: {e}")
            # Si CONCURRENTLY falla por bloqueo (raro), intentamos el normal
            try:
                db_manager.execute_query(f"REFRESH MATERIALIZED VIEW public.{mv_name};")
                logger.info(f"    MV '{mv_name}' refrescada exitosamente (modo normal).")
            except Exception as e_normal:
                logger.error(f"  ERROR (normal) al refrescar la Vista Materializada '{mv_name}': {e_normal}", exc_info=True)
                continue
        except Exception as e:
            logger.error(f"  ERROR al procesar la Vista Materializada '{mv_name}': {e}", exc_info=True)
            continue 

    logger.info("==================================================")
    logger.info("  Fase de Transformación (Creación/Refresco de MVs y Vistas RAW) FINALIZADA.")
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
                        # --- AÑADIR ESTE LOG CRÍTICO ---
                        logger.info(f"    Usando last_extracted_ts para '{entity}': {last_extracted_ts}")
                        # --------------------------------
                        
                        raw_data_records_from_api = api_client.get_data(
                            entity, 
                            id_sucursal_internal,
                            last_extracted_ts 
                        )
                        
                        if raw_data_records_from_api:
                            # --- AÑADIR LOG DE AUDITORÍA AQUÍ ---
                            logger.info(f"    [AUDIT] '{entity}' extraídos de la API: {len(raw_data_records_from_api)} registros. Preparando para carga...")
                            # ------------------------------------
                            
                            prepared_records_for_db = []
                            for record in raw_data_records_from_api:
                                fudo_record_id = record.get('id', str(uuid.uuid4())) 

                                last_updated_fudo = None
                                attributes = record.get('attributes', {})

                                if entity == 'sales':
                                    last_updated_fudo = parse_fudo_date(attributes.get('closedAt')) or \
                                                        parse_fudo_date(attributes.get('createdAt'))
                                elif entity in ['customers', 'expenses', 'items', 'payments', 'products',
                                                'discounts', 'ingredients', 'roles', 'tables', 'users',
                                                'expense-categories', 'kitchens', 'product-categories',
                                                'product-modifiers', 'rooms']:
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

                            db_manager.insert_raw_data(raw_table_name, prepared_records_for_db)
                            # --- AÑADIR LOG DE AUDITORÍA AQUÍ ---
                            logger.info(f"    [AUDIT] '{entity}' cargados en DB: {len(prepared_records_for_db)} registros en '{raw_table_name}'.")
                            # ------------------------------------
                            
                            metadata_manager.update_last_extraction_timestamp(
                                id_sucursal_internal, entity, datetime.now(timezone.utc)
                            )
                        else:
                            logger.info(f"    No se extrajeron nuevos registros para '{entity}'.")
                    except Exception as e:
                        logger.error(f"  Error al procesar entidad '{entity}': {e}", exc_info=True)
                        logger.error(f"    [AUDIT] '{entity}' extracción FALLIDA para sucursal '{id_sucursal_internal}'.") # Log de auditoría de fallo
                        continue
            except Exception as e:
                logger.error(f"Error crítico en sucursal '{branch_name}': {e}", exc_info=True)
                logger.error(f"    [AUDIT] Sucursal '{branch_name}' procesamiento FALLIDO.") # Log de auditoría de fallo crítico
                continue
            
            time.sleep(1) # Pequeña pausa entre sucursales

        # --- LLAMADA A LA FASE DE TRANSFORMACIÓN DESPUÉS DE LA EXTRACCIÓN RAW COMPLETA ---
        refresh_analytics_materialized_views(db_manager)
        # ----------------------------------------------------------------------------------

    except Exception as e:
        logger.critical(f"ERROR FATAL en el proceso ETL RAW principal: {e}", exc_info=True)
        print(f"ERROR FATAL: {e}") # Asegurar que se imprima a consola en caso de fallo crítico
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
        current_dir = os.path.dirname(__file__)
        absolute_ddl_path = os.path.join(current_dir, ddl_script_path)

        with open(absolute_ddl_path, 'r', encoding='utf-8') as f:
            sql_script_content = f.read()

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
    db_conn_string = config['db_connection_string']  # La cadena de conexión del ETL (apunta a la DB 'ginesta')

    db_for_all_phases = None
    try:
        db_for_all_phases = DBManager(db_conn_string)

        # Paso 1: Ejecutar el script DDL maestro para crear/actualizar toda la estructura.
        logger.info("Iniciando fase de DESPLIEGUE DE ESTRUCTURA...")
        deploy_fudo_database_structure(db_for_all_phases, 'sql/deploy_fudo_structure.sql')
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