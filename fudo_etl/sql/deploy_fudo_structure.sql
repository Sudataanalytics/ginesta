-- ######################################################################
-- #            SCRIPT MAESTRO DE DESPLIEGUE PARA DONWEB                #
-- #            ESTRUCTURA DE DATOS ETL FUDO (RAW y ANALÍTICA)          #
-- #            DB: ginesta                                             #
-- ######################################################################

-- NOTA IMPORTANTE:
-- Este script ASUME que la base de datos 'ginesta' YA HA SIDO CREADA.
-- Este script DEBE ejecutarse CONECTADO a la base de datos 'ginesta'
-- utilizando el usuario 'sudata_owner'.


-- ----------------------------------------------------------------------
-- PERMISOS INICIALES (Para el usuario 'sudata_owner' en el esquema 'public')
-- Esto garantiza que sudata_owner tenga control total sobre los objetos.
-- ----------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO sudata_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO sudata_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO sudata_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO sudata_owner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sudata_owner;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sudata_owner;


-- ----------------------------------------------------------------------
-- 1. TABLAS DE METADATOS Y CONFIGURACIÓN DEL ETL
-- ----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.etl_fudo_tokens (
    id_sucursal VARCHAR(255) PRIMARY KEY,
    access_token TEXT NOT NULL,
    token_expiration_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    last_updated_utc TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.etl_fudo_extraction_status (
    id_sucursal VARCHAR(255) NOT NULL,
    entity_name VARCHAR(100) NOT NULL,
    last_successful_extraction_utc TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id_sucursal, entity_name)
);

CREATE TABLE IF NOT EXISTS public.config_fudo_branches (
    id_sucursal VARCHAR(255) PRIMARY KEY,
    fudo_branch_identifier VARCHAR(255),
    sucursal_name VARCHAR(255),
    secret_manager_apikey_name VARCHAR(255) NOT NULL,
    secret_manager_apisecret_name VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
DELETE FROM public.config_fudo_branches WHERE id_sucursal = 'punto_criollo';

-- Insertar las sucursales Fudo iniciales
INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('chale', 'MUAxNTk0MzM=', 'Chale', 'FUDO_CHALE_APIKEY', 'FUDO_CHALE_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('quito_bar', 'M0AyMjA0NDQ=', 'Quito Bar', 'FUDO_QUITO_BAR_APIKEY', 'FUDO_QUITO_BAR_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('nebraska_sas', 'MjBAMTU1NjMy', 'Nebraska SAS', 'FUDO_NEBRASKA_SAS_APIKEY', 'FUDO_NEBRASKA_SAS_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('punto_criollo-peron', 'MTVANDg2OTc=', 'Punto Criollo Peron', 'FUDO_PUNTO_CRIOLLO_PERON_APIKEY', 'FUDO_PUNTO_CRIOLLO_PERON_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;

-- Añadir la nueva sucursal 'punto_criollo-guemes'
INSERT INTO public.config_fudo_branches (id_sucursal, fudo_branch_identifier, sucursal_name, secret_manager_apikey_name, secret_manager_apisecret_name)
VALUES ('punto_criollo-guemes', 'MTZAMjc1MTM5', 'Punto Criollo Guemes', 'FUDO_PUNTO_CRIOLLO_GUEMES_APIKEY', 'FUDO_PUNTO_CRIOLLO_GUEMES_APISECRET')
ON CONFLICT (id_sucursal) DO UPDATE SET
    fudo_branch_identifier = EXCLUDED.fudo_branch_identifier, sucursal_name = EXCLUDED.sucursal_name,
    secret_manager_apikey_name = EXCLUDED.secret_manager_apikey_name, secret_manager_apisecret_name = EXCLUDED.secret_manager_apisecret_name,
    updated_at = CURRENT_TIMESTAMP;


-- ----------------------------------------------------------------------
-- 2. TABLAS PARA LA CAPA DE DATOS CRUDOS (RAW LAYER)
-- ----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.fudo_raw_customers (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL, 
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_customers_id_sucursal_fecha ON public.fudo_raw_customers (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_discounts (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_discounts_id_sucursal_fecha ON public.fudo_raw_discounts (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_expenses (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_expenses_id_sucursal_fecha ON public.fudo_raw_expenses (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_expense_categories (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_expense_categories_id_sucursal_fecha ON public.fudo_raw_expense_categories (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_ingredients (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_ingredients_id_sucursal_fecha ON public.fudo_raw_ingredients (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_items (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_items_id_sucursal_fecha ON public.fudo_raw_items (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_kitchens (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_kitchens_id_sucursal_fecha ON public.fudo_raw_kitchens (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_payments (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_payments_id_sucursal_fecha ON public.fudo_raw_payments (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_payment_methods (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_payment_methods_id_sucursal_fecha ON public.fudo_raw_payment_methods (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_product_categories (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_product_categories_id_sucursal_fecha ON public.fudo_raw_product_categories (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_product_modifiers (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_product_modifiers_id_sucursal_fecha ON public.fudo_raw_product_modifiers (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_products ( 
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_products_id_sucursal_fecha ON public.fudo_raw_products (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_roles (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_roles_id_sucursal_fecha ON public.fudo_raw_roles (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_rooms (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_rooms_id_sucursal_fecha ON public.fudo_raw_rooms (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_sales (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_sales_id_sucursal_fecha ON public.fudo_raw_sales (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_tables (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_tables_id_sucursal_fecha ON public.fudo_raw_tables (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);

CREATE TABLE IF NOT EXISTS public.fudo_raw_users (
    id_fudo TEXT NOT NULL, 
    id_sucursal_fuente VARCHAR(255) NOT NULL, 
    fecha_extraccion_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
    payload_json JSONB NOT NULL, 
    last_updated_at_fudo TIMESTAMP WITH TIME ZONE,
    payload_checksum TEXT NOT NULL,
    PRIMARY KEY (id_fudo, id_sucursal_fuente, payload_checksum)
);
CREATE INDEX IF NOT EXISTS idx_fudo_raw_users_id_sucursal_fecha ON public.fudo_raw_users (id_fudo, id_sucursal_fuente, fecha_extraccion_utc DESC);


-- ----------------------------------------------------------------------
-- 3. TABLAS Y VISTAS MATERIALIZADAS PARA LA CAPA ANALÍTICA (DER)
-- ----------------------------------------------------------------------

-- Sucursales (DER)
CREATE TABLE IF NOT EXISTS public.Sucursales (
  id_sucursal VARCHAR(255) PRIMARY KEY,
  sucursal VARCHAR(255) NOT NULL
);

-- mv_sucursales
CREATE TABLE IF NOT EXISTS public.Sucursales (id_sucursal VARCHAR(255) PRIMARY KEY, sucursal VARCHAR(255) NOT NULL);
DROP MATERIALIZED VIEW IF EXISTS public.mv_sucursales CASCADE;
CREATE MATERIALIZED VIEW public.mv_sucursales AS
SELECT id_sucursal, sucursal_name AS sucursal FROM public.config_fudo_branches WHERE is_active = TRUE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sucursales_id_sucursal ON public.mv_sucursales (id_sucursal); -- <--- NUEVO ÍNDICE

-- Rubros (DER)
CREATE TABLE IF NOT EXISTS public.Rubros (
  id_rubro INTEGER PRIMARY KEY,
  rubro_name VARCHAR(255) NOT NULL
);

-- mv_rubros
CREATE TABLE IF NOT EXISTS public.Rubros (id_rubro INTEGER PRIMARY KEY, rubro_name VARCHAR(255) NOT NULL);
DROP MATERIALIZED VIEW IF EXISTS public.mv_rubros CASCADE;
CREATE MATERIALIZED VIEW public.mv_rubros AS
SELECT DISTINCT ON (id_fudo) (payload_json ->> 'id')::INTEGER AS id_rubro, (payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS rubro_name FROM public.fudo_raw_product_categories WHERE payload_json ->> 'id' IS NOT NULL AND payload_json -> 'attributes' ->> 'name' IS NOT NULL ORDER BY id_fudo, fecha_extraccion_utc DESC;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_rubros_id_rubro ON public.mv_rubros (id_rubro); -- <--- NUEVO ÍNDICE

-- Medio_pago (DER)
CREATE TABLE IF NOT EXISTS public.Medio_pago (
  id_payment INTEGER PRIMARY KEY,
  payment_method VARCHAR(255) NOT NULL
);

-- mv_medio_pago
CREATE TABLE IF NOT EXISTS public.Medio_pago (id_payment INTEGER PRIMARY KEY, payment_method VARCHAR(255) NOT NULL);
DROP MATERIALIZED VIEW IF EXISTS public.mv_medio_pago CASCADE;
CREATE MATERIALIZED VIEW public.mv_medio_pago AS
SELECT DISTINCT ON (id_fudo) (payload_json ->> 'id')::INTEGER AS id_payment, (payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS payment_method FROM public.fudo_raw_payment_methods WHERE payload_json ->> 'id' IS NOT NULL AND payload_json -> 'attributes' ->> 'name' IS NOT NULL ORDER BY id_fudo, fecha_extraccion_utc DESC;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_medio_pago_id_payment ON public.mv_medio_pago (id_payment); -- <--- NUEVO ÍNDICEREATE UNIQUE INDEX IF NOT EXISTS idx_mv_medio_pago_id ON public.mv_medio_pago (id_payment); -- <--- ¡NUEVO ÍNDICE ÚNICO!

-- Productos (DER)
CREATE TABLE IF NOT EXISTS public.Productos (
  id_product INTEGER PRIMARY KEY,
  product_name VARCHAR(255) NOT NULL,
  id_rubro INTEGER
);

-- mv_productos
CREATE TABLE IF NOT EXISTS public.Productos (id_product INTEGER PRIMARY KEY, product_name VARCHAR(255) NOT NULL, id_rubro INTEGER);
DROP MATERIALIZED VIEW IF EXISTS public.mv_productos CASCADE;
CREATE MATERIALIZED VIEW public.mv_productos AS
SELECT DISTINCT ON (p.id_fudo) (p.payload_json ->> 'id')::INTEGER AS id_product, (p.payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS product_name, (p.payload_json -> 'relationships' -> 'productCategory' -> 'data' ->> 'id')::INTEGER AS id_rubro FROM public.fudo_raw_products p WHERE p.payload_json ->> 'id' IS NOT NULL AND p.payload_json -> 'attributes' ->> 'name' IS NOT NULL ORDER BY p.id_fudo, p.fecha_extraccion_utc DESC;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_productos_id_product ON public.mv_productos (id_product); -- <--- NUEVO ÍNDICE

-- Sales_order (DER)
CREATE TABLE IF NOT EXISTS public.Sales_order (
  id_order INTEGER PRIMARY KEY,
  amount_tax FLOAT,
  amount_total FLOAT NOT NULL,
  date_order TIMESTAMP NOT NULL
);
-- Añadir las nuevas columnas si no existen (idempotente)
ALTER TABLE public.Sales_order ADD COLUMN IF NOT EXISTS sale_type VARCHAR(50);
ALTER TABLE public.Sales_order ADD COLUMN IF NOT EXISTS table_id TEXT;
ALTER TABLE public.Sales_order ADD COLUMN IF NOT EXISTS waiter_id TEXT;

-- mv_sales_order
DROP MATERIALIZED VIEW IF EXISTS public.mv_sales_order CASCADE;
CREATE MATERIALIZED VIEW public.mv_sales_order AS
SELECT DISTINCT ON (s.id_fudo, s.id_sucursal_fuente)
    (s.payload_json ->> 'id')::INTEGER AS id_order,
    s.id_sucursal_fuente AS id_sucursal,
    (s.payload_json ->> 'id') || '-' || s.id_sucursal_fuente AS order_key,
    0.0::FLOAT AS amount_tax,
    (s.payload_json -> 'attributes' ->> 'total')::FLOAT AS amount_total,
    (s.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS date_order, -- <--- AHORA date_order ES createdAt
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
    (s.payload_json -> 'attributes' ->> 'total') IS NOT NULL AND -- Aseguramos que 'total' no sea NULL antes de usarlo
    (s.payload_json -> 'attributes' ->> 'saleState') IS NOT NULL AND
    (s.payload_json -> 'attributes' ->> 'saleState') != 'CANCELED' -- <--- FILTRO CLAVE
ORDER BY s.id_fudo, s.id_sucursal_fuente, s.fecha_extraccion_utc DESC;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sales_order_order_key ON public.mv_sales_order (order_key);
-- Pagos (DER)
CREATE TABLE IF NOT EXISTS public.Pagos (
  id INTEGER PRIMARY KEY,
  pos_order_id INTEGER NOT NULL,
  id_payment INTEGER NOT NULL,
  amount FLOAT NOT NULL,
  payment_date TIMESTAMP NOT NULL,
  id_sucursal VARCHAR(255) NOT NULL
);

-- mv_pagos (DER)
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_pagos AS
SELECT DISTINCT ON (p.id_fudo, p.id_sucursal_fuente)
    (p.payload_json ->> 'id')::INTEGER AS id, -- ID del pago de Fudo
    p.id_sucursal_fuente AS id_sucursal, -- Mantener id_sucursal
    -- Crear una clave primaria compuesta para el Pago
    (p.payload_json ->> 'id') || '-' || p.id_sucursal_fuente AS payment_key, -- <--- NUEVA CLAVE ÚNICA DEL PAGO
    (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::INTEGER AS pos_order_id,
    (p.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id')::INTEGER AS id_payment,
    (p.payload_json -> 'attributes' ->> 'amount')::FLOAT AS amount,
    (p.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS payment_date,
    -- Necesitamos el order_key de la venta para la FK
    (frs.payload_json ->> 'id') || '-' || frs.id_sucursal_fuente AS order_key_fk -- <--- FK a Sales_order.order_key
FROM public.fudo_raw_payments p
JOIN public.fudo_raw_sales frs ON (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::INTEGER = (frs.payload_json ->> 'id')::INTEGER
WHERE p.payload_json ->> 'id' IS NOT NULL AND (p.payload_json -> 'attributes' ->> 'amount') IS NOT NULL AND (p.payload_json -> 'attributes' ->> 'createdAt') IS NOT NULL AND (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') IS NOT NULL AND (p.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id') IS NOT NULL AND (p.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN IS NOT TRUE AND frs.id_sucursal_fuente IS NOT NULL
ORDER BY p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc DESC;
-- El índice único ahora es sobre la nueva columna 'payment_key'
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_pagos_payment_key ON public.mv_pagos (payment_key);

-- Sales_order_line (DER)
CREATE TABLE IF NOT EXISTS public.Sales_order_line (
  id_order_line INTEGER PRIMARY KEY,
  id_order INTEGER NOT NULL,
  date_order_time TIMESTAMP NOT NULL,
  date_order DATE NOT NULL,
  id_product INTEGER NOT NULL,
  price_unit FLOAT NOT NULL,
  qty INTEGER NOT NULL,
  amount_total FLOAT NOT NULL,
  id_sucursal VARCHAR(255) NOT NULL
);

-- mv_sales_order_line
CREATE TABLE IF NOT EXISTS public.Sales_order_line (id_order_line INTEGER PRIMARY KEY, id_order INTEGER NOT NULL, date_order_time TIMESTAMP NOT NULL, date_order DATE NOT NULL, id_product INTEGER NOT NULL, price_unit FLOAT NOT NULL, qty INTEGER NOT NULL, amount_total FLOAT NOT NULL, id_sucursal VARCHAR(255) NOT NULL);
DROP MATERIALIZED VIEW IF EXISTS public.mv_sales_order_line CASCADE;
CREATE MATERIALIZED VIEW public.mv_sales_order_line AS
SELECT DISTINCT ON (i.id_fudo, i.id_sucursal_fuente)
    (i.payload_json ->> 'id')::INTEGER AS id_order_line,
    (i.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::INTEGER AS id_order,
    (i.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS date_order_time,
    (i.payload_json -> 'attributes' ->> 'createdAt')::DATE AS date_order,
    (i.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id')::INTEGER AS id_product,
    (i.payload_json -> 'attributes' ->> 'price')::FLOAT AS price_unit,
    ((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT)::INTEGER AS qty,
    ((i.payload_json -> 'attributes' ->> 'price')::FLOAT * (i.payload_json -> 'attributes' ->> 'quantity')::FLOAT)::FLOAT AS amount_total,
    i.id_sucursal_fuente AS id_sucursal,
    (i.payload_json ->> 'id') || '-' || i.id_sucursal_fuente AS order_line_key -- <--- Asegurarnos que esta es la columna
FROM public.fudo_raw_items i
WHERE i.payload_json ->> 'id' IS NOT NULL AND (i.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') IS NOT NULL AND (i.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id') IS NOT NULL AND (i.payload_json -> 'attributes' ->> 'createdAt') IS NOT NULL AND (i.payload_json -> 'attributes' ->> 'price') IS NOT NULL AND (i.payload_json -> 'attributes' ->> 'quantity') IS NOT NULL AND (i.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN IS NOT TRUE AND ((i.payload_json -> 'attributes' ->> 'quantity')::FLOAT)::INTEGER > 0
ORDER BY i.id_fudo, i.id_sucursal_fuente, i.fecha_extraccion_utc DESC;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sales_order_line_order_line_key ON public.mv_sales_order_line (order_line_key); -- <--- Usar el nombre correcto aquí

-- mv_expense_categories (NUEVA MV - CON CLAVE SINTÉTICA Y ÍNDICE ÚNICO)
CREATE TABLE IF NOT EXISTS public.Expense_categories (
  id_expense_category TEXT, -- No PK si la clave sintética es la PK en Power BI
  expense_category_name VARCHAR(255) NOT NULL,
  financial_category VARCHAR(255),
  active BOOLEAN,
  parent_category_id TEXT,
  id_sucursal VARCHAR(255) NOT NULL,
  expense_category_key TEXT PRIMARY KEY -- <--- ¡AÑADIR ESTA COLUMNA COMO PK!
);
DROP MATERIALIZED VIEW IF EXISTS public.mv_expense_categories CASCADE;
CREATE MATERIALIZED VIEW public.mv_expense_categories AS
SELECT DISTINCT ON (ec.id_fudo, ec.id_sucursal_fuente)
    (ec.payload_json ->> 'id') AS id_expense_category,
    (ec.payload_json -> 'attributes' ->> 'name') AS expense_category_name,
    (ec.payload_json -> 'attributes' ->> 'financialCategory') AS financial_category,
    (ec.payload_json -> 'attributes' ->> 'active')::BOOLEAN AS active,
    (ec.payload_json -> 'relationships' -> 'parentCategory' -> 'data' ->> 'id') AS parent_category_id,
    ec.id_sucursal_fuente AS id_sucursal, -- Mantener id_sucursal
    (ec.payload_json ->> 'id') || '-' || ec.id_sucursal_fuente AS expense_category_key -- <--- ¡AÑADIR ESTO!
FROM public.fudo_raw_expense_categories ec
WHERE (ec.payload_json ->> 'id') IS NOT NULL AND (ec.payload_json -> 'attributes' ->> 'name') IS NOT NULL AND ec.id_sucursal_fuente IS NOT NULL
ORDER BY ec.id_fudo, ec.id_sucursal_fuente, ec.fecha_extraccion_utc DESC;
-- Crear el índice único sobre la clave sintética
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_expense_categories_key ON public.mv_expense_categories (expense_category_key);

-- mv_expenses (NUEVA MV - CON CLAVE SINTÉTICA PARA FK)
CREATE TABLE IF NOT EXISTS public.Expenses (
  id_expense TEXT PRIMARY KEY,
  id_sucursal VARCHAR(255) NOT NULL,
  amount FLOAT NOT NULL,
  description TEXT,
  expense_date TIMESTAMP NOT NULL,
  status VARCHAR(50),
  due_date TIMESTAMP,
  canceled BOOLEAN,
  created_at TIMESTAMP,
  payment_date TIMESTAMP,
  receipt_number TEXT,
  use_in_cash_count BOOLEAN,
  user_id TEXT,
  provider_id TEXT,
  receipt_type_id TEXT,
  cash_register_id TEXT,
  payment_method_id TEXT,
  expense_category_id TEXT, -- Mantener el id_expense_category de Fudo
  expense_category_key TEXT NOT NULL -- <--- ¡AÑADIR LA FK A LA CLAVE SINTÉTICA!
);
DROP MATERIALIZED VIEW IF EXISTS public.mv_expenses CASCADE;
CREATE MATERIALIZED VIEW public.mv_expenses AS
SELECT DISTINCT ON (e.id_fudo, e.id_sucursal_fuente)
    (e.payload_json ->> 'id') AS id_expense,
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
    -- --- AÑADIR LA FK A LA CLAVE SINTÉTICA ---
    (e.payload_json -> 'relationships' -> 'expenseCategory' -> 'data' ->> 'id') || '-' || e.id_sucursal_fuente AS expense_category_key
    -- ----------------------------------------
FROM public.fudo_raw_expenses e
WHERE (e.payload_json ->> 'id') IS NOT NULL AND e.id_sucursal_fuente IS NOT NULL
ORDER BY e.id_fudo, e.id_sucursal_fuente, e.fecha_extraccion_utc DESC;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_expenses_id_sucursal ON public.mv_expenses (id_expense, id_sucursal); -- Mantener este índice también, si id_expense + id_sucursal es la PK natural.

-- Product_categories (Tabla Lógica del DER - para categorización de productos)
CREATE TABLE IF NOT EXISTS public.Product_categories (
  id_product_category INTEGER,
  product_category_name VARCHAR(255) NOT NULL,
  "position" INTEGER,
  preparation_time INTEGER,
  enable_online_menu BOOLEAN,
  kitchen_id TEXT,
  parent_category_id TEXT,
  id_sucursal VARCHAR(255) NOT NULL,
  product_category_key TEXT PRIMARY KEY
);

-- 4. VISTAS DESNORMALIZADAS DE LA CAPA RAW (PARA EXPLORACIÓN Y REPORTES FLEXIBLES)
-- Estos son VISTAS estándar (no materializadas) que desestructuran el JSONB.
-- Se utilizan para facilitar la exploración y acceso directo a los datos.
-- Cada vista incluye el 'original_payload' para auditoría.
-- ----------------------------------------------------------------------

-- fudo_view_raw_customers
drop view if exists public.fudo_view_raw_customers CASCADE;
CREATE OR REPLACE VIEW public.fudo_view_raw_customers AS
SELECT
    c.id_fudo, c.id_sucursal_fuente, c.fecha_extraccion_utc, c.payload_checksum,
    (c.payload_json ->> 'id') AS customer_id,
    (c.payload_json -> 'attributes' ->> 'active')::BOOLEAN AS active,
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


-- fudo_view_raw_discounts (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_discounts CASCADE;
CREATE OR REPLACE VIEW public.fudo_view_raw_discounts AS
SELECT
    d.id_fudo, d.id_sucursal_fuente, d.fecha_extraccion_utc, d.payload_checksum,
    (d.payload_json ->> 'id') AS discount_id,
    (d.payload_json -> 'attributes' ->> 'amount')::FLOAT AS amount,
    (d.payload_json -> 'attributes' ->> 'percentage')::FLOAT AS percentage,
    (d.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN AS canceled,
    (d.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') AS sale_id,
    d.payload_json AS original_payload
FROM public.fudo_raw_discounts d
ORDER BY d.id_fudo, d.id_sucursal_fuente, d.fecha_extraccion_utc DESC;


-- fudo_view_raw_expenses (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_expenses CASCADE;
CREATE OR REPLACE VIEW public.fudo_view_raw_expenses AS
SELECT
    e.id_fudo,
    e.id_sucursal_fuente,
    e.fecha_extraccion_utc,
    e.payload_checksum,
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


-- fudo_view_raw_expense_categories (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_expense_categories CASCADE;
CREATE OR REPLACE VIEW public.fudo_view_raw_expense_categories AS
SELECT
    ec.id_fudo, ec.id_sucursal_fuente, ec.fecha_extraccion_utc, ec.payload_checksum,
    (ec.payload_json ->> 'id') AS category_id,
    (ec.payload_json -> 'attributes' ->> 'name') AS category_active,
    (ec.payload_json -> 'attributes' ->> 'active')::BOOLEAN AS active,
    (ec.payload_json -> 'attributes' ->> 'financialCategory') AS financial_category,
    (ec.payload_json -> 'relationships' -> 'parentCategory' -> 'data' ->> 'id') AS parent_category_id,
    ec.payload_json AS original_payload
FROM public.fudo_raw_expense_categories ec
ORDER BY ec.id_fudo, ec.id_sucursal_fuente, ec.fecha_extraccion_utc DESC;


-- fudo_view_raw_ingredients (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_ingredients CASCADE;
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

-- fudo_view_raw_items
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
    (i.payload_json -> 'relationships' -> 'subitems') AS subitems_data, -- JSONB array de subitems
    i.payload_json AS original_payload
FROM public.fudo_raw_items i
ORDER BY i.id_fudo, i.id_sucursal_fuente, i.fecha_extraccion_utc DESC;


-- fudo_view_raw_kitchens (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_kitchens CASCADE;
CREATE OR REPLACE VIEW public.fudo_view_raw_kitchens AS
SELECT
    k.id_fudo, k.id_sucursal_fuente, k.fecha_extraccion_utc, k.payload_checksum,
    (k.payload_json ->> 'id') AS kitchen_id,
    (k.payload_json -> 'attributes' ->> 'name') AS kitchen_name,
    k.payload_json AS original_payload
FROM public.fudo_raw_kitchens k
ORDER BY k.id_fudo, k.id_sucursal_fuente, k.fecha_extraccion_utc DESC;

-- fudo_view_raw_payments (¡NUEVA VISTA RAW DESNORMALIZADA!)
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


-- fudo_view_raw_products (¡NUEVA VISTA RAW DESNORMALIZADA!)
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
    (p.payload_json -> 'attributes' ->> 'position')::INTEGER AS "position", -- "position" es palabra reservada
    (p.payload_json -> 'attributes' ->> 'preparationTime')::INTEGER AS preparation_time,
    (p.payload_json -> 'attributes' ->> 'price')::FLOAT AS price,
    (p.payload_json -> 'attributes' ->> 'sellAlone')::BOOLEAN AS sell_alone,
    (p.payload_json -> 'attributes' ->> 'stock')::FLOAT AS stock,
    (p.payload_json -> 'attributes' ->> 'stockControl')::BOOLEAN AS stock_control,
    (p.payload_json -> 'relationships' -> 'kitchen' -> 'data' ->> 'id') AS kitchen_id,
    (p.payload_json -> 'relationships' -> 'productCategory' -> 'data' ->> 'id') AS product_category_id,
    (p.payload_json -> 'relationships' -> 'productModifiersGroups' -> 'data') AS product_modifiers_groups, -- JSONB array
    (p.payload_json -> 'relationships' -> 'productProportions' -> 'data') AS product_proportions, -- JSONB array
    p.payload_json AS original_payload
FROM public.fudo_raw_products p
ORDER BY p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc DESC;

-- fudo_view_raw_product_modifiers (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_product_modifiers CASCADE;
CREATE OR REPLACE VIEW public.fudo_view_raw_product_modifiers AS
SELECT
    pm.id_fudo, pm.id_sucursal_fuente, pm.fecha_extraccion_utc, pm.payload_checksum,
    (pm.payload_json ->> 'id') AS modifier_id,
    (pm.payload_json -> 'attributes' ->> 'maxQuantity')::INTEGER AS max_quantity,
    (pm.payload_json -> 'attributes' ->> 'price')::FLOAT AS price,
    (pm.payload_json -> 'relationships' -> 'product' -> 'data' ->> 'id') AS product_id,
    (pm.payload_json -> 'relationships' -> 'productModifiersGroup' -> 'data' ->> 'id') AS product_modifiers_group_id,
    pm.payload_json AS original_payload
FROM public.fudo_raw_product_modifiers pm
ORDER BY pm.id_fudo, pm.id_sucursal_fuente, pm.fecha_extraccion_utc DESC;


-- fudo_view_raw_roles (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_roles CASCADE;
CREATE OR REPLACE VIEW public.fudo_view_raw_roles AS
SELECT
    r.id_fudo, r.id_sucursal_fuente, r.fecha_extraccion_utc, r.payload_checksum,
    (r.payload_json ->> 'id') AS role_id,
    (r.payload_json -> 'attributes' ->> 'isWaiter')::BOOLEAN AS is_waiter,
    (r.payload_json -> 'attributes' ->> 'isDeliveryman')::BOOLEAN AS is_deliveryman,
    (r.payload_json -> 'attributes' ->> 'name') AS role_name,
    (r.payload_json -> 'attributes' -> 'permissions') AS permissions, -- Mantenemos como JSONB array
    r.payload_json AS original_payload
FROM public.fudo_raw_roles r
ORDER BY r.id_fudo, r.id_sucursal_fuente, r.fecha_extraccion_utc DESC;


-- fudo_view_raw_rooms (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_rooms CASCADE;
CREATE OR REPLACE VIEW public.fudo_view_raw_rooms AS
SELECT
    r.id_fudo, r.id_sucursal_fuente, r.fecha_extraccion_utc, r.payload_checksum,
    (r.payload_json ->> 'id') AS room_id,
    (r.payload_json -> 'attributes' ->> 'name') AS room_name,
    (r.payload_json -> 'relationships' -> 'tables' -> 'data') AS table_ids, -- Array de IDs de tablas
    r.payload_json AS original_payload
FROM public.fudo_raw_rooms r
ORDER BY r.id_fudo, r.id_sucursal_fuente, r.fecha_extraccion_utc DESC;


-- fudo_view_raw_tables (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_tables CASCADE;
CREATE OR REPLACE VIEW public.fudo_view_raw_tables AS
SELECT
    t.id_fudo, t.id_sucursal_fuente, t.fecha_extraccion_utc, t.payload_checksum,
    (t.payload_json ->> 'id') AS table_id,
    (t.payload_json -> 'attributes' ->> 'column')::INTEGER AS "column",
    (t.payload_json -> 'attributes' ->> 'number')::INTEGER AS table_number,
    (t.payload_json -> 'attributes' ->> 'row')::INTEGER AS "row",
    (t.payload_json -> 'attributes' ->> 'shape') AS shape,
    (t.payload_json -> 'attributes' ->> 'size') AS size,
    (t.payload_json -> 'relationships' -> 'room' -> 'data' ->> 'id') AS room_id,
    t.payload_json AS original_payload
FROM public.fudo_raw_tables t
ORDER BY t.id_fudo, t.id_sucursal_fuente, t.fecha_extraccion_utc DESC;


-- fudo_view_raw_users (Basado en ejemplo, sin 'fields')
drop view if exists public.fudo_view_raw_users;
CREATE OR REPLACE VIEW public.fudo_view_raw_users AS
SELECT
    u.id_fudo, u.id_sucursal_fuente, u.fecha_extraccion_utc, u.payload_checksum,
    (u.payload_json ->> 'id') AS user_id,
    (u.payload_json -> 'attributes' ->> 'active')::BOOLEAN AS active,
    (u.payload_json -> 'attributes' ->> 'admin')::BOOLEAN AS admin,
    (u.payload_json -> 'attributes' ->> 'email') AS email,
    (u.payload_json -> 'attributes' ->> 'name') AS user_name,
    (u.payload_json -> 'attributes' ->> 'promotionalCode') AS promotional_code,
    (u.payload_json -> 'relationships' -> 'role' -> 'data' ->> 'id') AS role_id,
    u.payload_json AS original_payload
FROM public.fudo_raw_users u
ORDER BY u.id_fudo, u.id_sucursal_fuente, u.fecha_extraccion_utc DESC;

-- fudo_view_raw_sales (¡NUEVA VISTA RAW DESNORMALIZADA con todos los campos del JSON!)
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
    (s.payload_json -> 'attributes' -> 'expectedPayments') AS expected_payments, -- Mantener como JSONB array
    (s.payload_json -> 'relationships' -> 'customer' -> 'data' ->> 'id') AS customer_id,
    (s.payload_json -> 'relationships' -> 'discounts' -> 'data') AS discounts_data, -- JSONB array de IDs
    (s.payload_json -> 'relationships' -> 'items' -> 'data') AS items_data,       -- JSONB array de IDs
    (s.payload_json -> 'relationships' -> 'payments' -> 'data') AS payments_data, -- JSONB array de IDs
    (s.payload_json -> 'relationships' -> 'tips' -> 'data') AS tips_data,
    (s.payload_json -> 'relationships' -> 'shippingCosts' -> 'data') AS shipping_costs_data,
    (s.payload_json -> 'relationships' -> 'table' -> 'data' ->> 'id') AS table_id,
    (s.payload_json -> 'relationships' -> 'waiter' -> 'data' ->> 'id') AS waiter_id,
    (s.payload_json -> 'relationships' -> 'saleIdentifier' -> 'data' ->> 'id') AS sale_identifier_id,
    s.payload_json AS original_payload
FROM public.fudo_raw_sales s
ORDER BY s.id_fudo, s.id_sucursal_fuente, s.fecha_extraccion_utc DESC;