-- TRANSFORMAR: combina las 4 fuentes de staging en el modelo de analytics.
-- Todo es upsert (INSERT ... ON CONFLICT), así que se puede repetir sin duplicar.

-- Catálogo (fuente JSON)
INSERT INTO analytics.dim_producto (producto_id, nombre, marca, tipo, precio_lista)
SELECT producto_id, nombre, marca, tipo, precio
FROM staging.productos
ON CONFLICT (producto_id) DO UPDATE
SET nombre         = EXCLUDED.nombre,
    marca          = EXCLUDED.marca,
    tipo           = EXCLUDED.tipo,
    precio_lista   = EXCLUDED.precio_lista,
    actualizado_en = now();

-- Clima (fuente API). Día lluvioso = 1 mm o más de lluvia.
INSERT INTO analytics.dim_clima (fecha, temp_max, temp_min, lluvia_mm, dia_lluvioso)
SELECT fecha, temp_max, temp_min, lluvia_mm,
       CASE WHEN lluvia_mm IS NULL THEN NULL ELSE lluvia_mm >= 1 END
FROM staging.clima
ON CONFLICT (fecha) DO UPDATE
SET temp_max       = EXCLUDED.temp_max,
    temp_min       = EXCLUDED.temp_min,
    lluvia_mm      = EXCLUDED.lluvia_mm,
    dia_lluvioso   = EXCLUDED.dia_lluvioso,
    actualizado_en = now();

-- Ventas (fuente CSV) + catálogo (JSON) + tipo de cambio (API).
-- Sábados, domingos y festivos no hay tipo de cambio: se usa el último día
-- hábil anterior, igual que hacen los bancos.
INSERT INTO analytics.fact_ventas
    (folio, producto_id, fecha, dia_semana, canal, cantidad, precio_unitario,
     total_mxn, usd_mxn, total_usd)
SELECT v.folio,
       v.producto_id,
       v.fecha,
       (ARRAY['Domingo','Lunes','Martes','Miércoles','Jueves','Viernes','Sábado'])
           [EXTRACT(DOW FROM v.fecha)::int + 1],
       v.canal,
       v.cantidad,
       v.precio_unitario,
       v.cantidad * v.precio_unitario,
       tc.usd_mxn,
       round(v.cantidad * v.precio_unitario / tc.usd_mxn, 2)
FROM staging.ventas v
JOIN analytics.dim_producto p ON p.producto_id = v.producto_id
LEFT JOIN LATERAL (
    SELECT t.usd_mxn
    FROM staging.tipo_cambio t
    WHERE t.fecha <= v.fecha
    ORDER BY t.fecha DESC
    LIMIT 1
) tc ON TRUE
ON CONFLICT (folio, producto_id) DO UPDATE
SET fecha           = EXCLUDED.fecha,
    dia_semana      = EXCLUDED.dia_semana,
    canal           = EXCLUDED.canal,
    cantidad        = EXCLUDED.cantidad,
    precio_unitario = EXCLUDED.precio_unitario,
    total_mxn       = EXCLUDED.total_mxn,
    usd_mxn         = EXCLUDED.usd_mxn,
    total_usd       = EXCLUDED.total_usd,
    actualizado_en  = now();
