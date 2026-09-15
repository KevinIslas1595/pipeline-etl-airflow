-- Estructura del data warehouse. Se ejecuta al inicio de cada corrida del DAG:
-- todo es "IF NOT EXISTS" / "OR REPLACE", así que repetirlo no rompe nada.
--
--   staging    datos de las 4 fuentes, ya limpios, sin combinar
--   analytics  modelo para análisis: dimensiones, hechos y vistas de reporte
--   control    bitácora de cada carga

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS control;

-- ---------------------------------------------------------------- staging --

-- Fuente 1: CSV de ventas
CREATE TABLE IF NOT EXISTS staging.ventas (
    folio            TEXT          NOT NULL,
    fecha            DATE          NOT NULL,
    producto_id      INTEGER       NOT NULL,
    cantidad         INTEGER       NOT NULL CHECK (cantidad > 0),
    precio_unitario  NUMERIC(10,2) NOT NULL CHECK (precio_unitario > 0),
    canal            TEXT          NOT NULL,
    cargado_en       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (folio, producto_id)
);

-- Fuente 2: JSON del catálogo de productos
CREATE TABLE IF NOT EXISTS staging.productos (
    producto_id  INTEGER       PRIMARY KEY,
    nombre       TEXT          NOT NULL,
    marca        TEXT          NOT NULL,
    tipo         TEXT          NOT NULL,
    precio       NUMERIC(10,2) NOT NULL CHECK (precio > 0),
    cargado_en   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Fuente 3: API de clima (Open-Meteo)
CREATE TABLE IF NOT EXISTS staging.clima (
    fecha       DATE         PRIMARY KEY,
    temp_max    NUMERIC(4,1),
    temp_min    NUMERIC(4,1),
    lluvia_mm   NUMERIC(6,1),
    cargado_en  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Fuente 4: API de tipo de cambio (Frankfurter / BCE)
CREATE TABLE IF NOT EXISTS staging.tipo_cambio (
    fecha       DATE           PRIMARY KEY,
    usd_mxn     NUMERIC(10,4)  NOT NULL CHECK (usd_mxn > 0),
    cargado_en  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------- analytics --

CREATE TABLE IF NOT EXISTS analytics.dim_producto (
    producto_id   INTEGER       PRIMARY KEY,
    nombre        TEXT          NOT NULL,
    marca         TEXT          NOT NULL,
    tipo          TEXT          NOT NULL,
    precio_lista  NUMERIC(10,2) NOT NULL,
    actualizado_en TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.dim_clima (
    fecha          DATE         PRIMARY KEY,
    temp_max       NUMERIC(4,1),
    temp_min       NUMERIC(4,1),
    lluvia_mm      NUMERIC(6,1),
    dia_lluvioso   BOOLEAN,
    actualizado_en TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.fact_ventas (
    folio            TEXT          NOT NULL,
    producto_id      INTEGER       NOT NULL REFERENCES analytics.dim_producto,
    fecha            DATE          NOT NULL,
    dia_semana       TEXT          NOT NULL,
    canal            TEXT          NOT NULL,
    cantidad         INTEGER       NOT NULL,
    precio_unitario  NUMERIC(10,2) NOT NULL,
    total_mxn        NUMERIC(12,2) NOT NULL,
    usd_mxn          NUMERIC(10,4),
    total_usd        NUMERIC(12,2),
    actualizado_en   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (folio, producto_id)
);

-- ---------------------------------------------------------------- control --

CREATE TABLE IF NOT EXISTS control.bitacora_cargas (
    id                BIGSERIAL    PRIMARY KEY,
    run_id            TEXT         NOT NULL,
    fuente            TEXT         NOT NULL,
    archivo_crudo     TEXT         NOT NULL,
    filas_leidas      INTEGER      NOT NULL,
    filas_rechazadas  INTEGER      NOT NULL,
    filas_duplicadas  INTEGER      NOT NULL,
    filas_cargadas    INTEGER      NOT NULL,
    cargado_en        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ----------------------------------------------------- vistas de reporte --

-- Un renglón por día: pedidos, piezas, ingresos en pesos y dólares, y clima.
CREATE OR REPLACE VIEW analytics.reporte_diario AS
SELECT f.fecha,
       f.dia_semana,
       count(DISTINCT f.folio)          AS pedidos,
       sum(f.cantidad)                  AS piezas,
       sum(f.total_mxn)                 AS ingreso_mxn,
       sum(f.total_usd)                 AS ingreso_usd,
       max(f.usd_mxn)                   AS usd_mxn,
       c.temp_max,
       c.lluvia_mm,
       c.dia_lluvioso
FROM analytics.fact_ventas f
LEFT JOIN analytics.dim_clima c ON c.fecha = f.fecha
GROUP BY f.fecha, f.dia_semana, c.temp_max, c.lluvia_mm, c.dia_lluvioso;

-- Ranking de productos.
CREATE OR REPLACE VIEW analytics.ventas_por_producto AS
SELECT p.producto_id,
       p.nombre,
       p.marca,
       p.tipo,
       sum(f.cantidad)                  AS piezas,
       sum(f.total_mxn)                 AS ingreso_mxn,
       count(DISTINCT f.folio)          AS pedidos
FROM analytics.fact_ventas f
JOIN analytics.dim_producto p USING (producto_id)
GROUP BY p.producto_id, p.nombre, p.marca, p.tipo;

-- ¿Se vende distinto cuando llueve? Promedio diario en días con y sin lluvia.
CREATE OR REPLACE VIEW analytics.ventas_segun_lluvia AS
SELECT CASE WHEN dia_lluvioso THEN 'Con lluvia'
            WHEN NOT dia_lluvioso THEN 'Sin lluvia'
            ELSE 'Sin dato de clima' END      AS clima,
       count(*)                               AS dias,
       round(avg(pedidos), 1)                 AS pedidos_promedio,
       round(avg(ingreso_mxn), 2)             AS ingreso_promedio_mxn
FROM analytics.reporte_diario
GROUP BY 1;
