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
VALUES ('punto_criollo', 'MTVANDg2OTc=', 'Punto Criollo', 'FUDO_PUNTO_CRIOLLO_APIKEY', 'FUDO_PUNTO_CRIOLLO_APISECRET')
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

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_sucursales AS
SELECT
    id_sucursal,
    sucursal_name AS sucursal
FROM public.config_fudo_branches
WHERE is_active = TRUE;

-- Rubros (DER)
CREATE TABLE IF NOT EXISTS public.Rubros (
  id_rubro INTEGER PRIMARY KEY,
  rubro_name VARCHAR(255) NOT NULL
);

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_rubros AS
SELECT DISTINCT ON (id_fudo)
    (payload_json ->> 'id')::INTEGER AS id_rubro,
    (payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS rubro_name
FROM public.fudo_raw_product_categories
WHERE
    payload_json ->> 'id' IS NOT NULL AND
    payload_json -> 'attributes' ->> 'name' IS NOT NULL
ORDER BY id_fudo, fecha_extraccion_utc DESC;

-- Medio_pago (DER)
CREATE TABLE IF NOT EXISTS public.Medio_pago (
  id_payment INTEGER PRIMARY KEY,
  payment_method VARCHAR(255) NOT NULL
);

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_medio_pago AS
SELECT DISTINCT ON (id_fudo)
    (payload_json ->> 'id')::INTEGER AS id_payment,
    (payload_json -> 'attributes' ->> 'name')::VARCHAR(255) AS payment_method
FROM public.fudo_raw_payment_methods
WHERE
    payload_json ->> 'id' IS NOT NULL AND
    payload_json -> 'attributes' ->> 'name' IS NOT NULL
ORDER BY id_fudo, fecha_extraccion_utc DESC;

-- Productos (DER)
CREATE TABLE IF NOT EXISTS public.Productos (
  id_product INTEGER PRIMARY KEY,
  product_name VARCHAR(255) NOT NULL,
  id_rubro INTEGER
);

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

-- Sales_order (DER)
CREATE TABLE IF NOT EXISTS public.Sales_order (
  id_order INTEGER PRIMARY KEY,
  amount_tax FLOAT,
  amount_total FLOAT NOT NULL,
  date_order TIMESTAMP NOT NULL
);

CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_sales_order AS
SELECT DISTINCT ON (s.id_fudo, s.id_sucursal_fuente)
    (s.payload_json ->> 'id')::INTEGER AS id_order,
    0.0::FLOAT AS amount_tax,
    (s.payload_json -> 'attributes' ->> 'total')::FLOAT AS amount_total,
    COALESCE(
        (s.payload_json -> 'attributes' ->> 'closedAt')::TIMESTAMP WITH TIME ZONE,
        (s.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE
    ) AS date_order
FROM public.fudo_raw_sales s
WHERE
    s.payload_json ->> 'id' IS NOT NULL AND
    s.payload_json -> 'attributes' ->> 'total' IS NOT NULL AND
    (s.payload_json -> 'attributes' ->> 'saleState') = 'CLOSED'
ORDER BY s.id_fudo, s.id_sucursal_fuente, s.fecha_extraccion_utc DESC;

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
    (p.payload_json ->> 'id')::INTEGER AS id,
    (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::INTEGER AS pos_order_id,
    (p.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id')::INTEGER AS id_payment,
    (p.payload_json -> 'attributes' ->> 'amount')::FLOAT AS amount,
    (p.payload_json -> 'attributes' ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS payment_date,
    -- --- AÑADIR LA COLUMNA id_sucursal ---
    frs.id_sucursal_fuente AS id_sucursal
    -- ------------------------------------
FROM public.fudo_raw_payments p
-- --- UNIR CON fudo_raw_sales para obtener id_sucursal ---
JOIN public.fudo_raw_sales frs ON (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id')::INTEGER = (frs.payload_json ->> 'id')::INTEGER
-- --------------------------------------------------------
WHERE
    p.payload_json ->> 'id' IS NOT NULL AND
    (p.payload_json -> 'attributes' ->> 'amount') IS NOT NULL AND
    (p.payload_json -> 'attributes' ->> 'createdAt') IS NOT NULL AND
    (p.payload_json -> 'relationships' -> 'sale' -> 'data' ->> 'id') IS NOT NULL AND
    (p.payload_json -> 'relationships' -> 'paymentMethod' -> 'data' ->> 'id') IS NOT NULL AND
    (p.payload_json -> 'attributes' ->> 'canceled')::BOOLEAN IS NOT TRUE AND
    frs.id_sucursal_fuente IS NOT NULL -- Asegurar que la venta join tenga sucursal
ORDER BY p.id_fudo, p.id_sucursal_fuente, p.fecha_extraccion_utc DESC;

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