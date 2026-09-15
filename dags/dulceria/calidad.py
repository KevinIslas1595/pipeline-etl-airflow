"""Reglas de calidad de datos que se revisan después de transformar.

Cada regla es una consulta que regresa cuántos problemas encontró (0 = bien).
Las reglas "obligatorias" detienen el pipeline; las demás solo avisan.
"""

REGLAS = [
    # (descripción, consulta, obligatoria)
    ("Hay ventas cargadas",
     "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM staging.ventas", True),
    ("Hay productos cargados",
     "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM staging.productos", True),
    ("Hay datos de clima",
     "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM staging.clima", True),
    ("Hay tipos de cambio",
     "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM staging.tipo_cambio", True),
    ("Toda venta es de un producto del catálogo",
     "SELECT count(*) FROM staging.ventas v "
     "LEFT JOIN analytics.dim_producto p ON p.producto_id = v.producto_id "
     "WHERE p.producto_id IS NULL", True),
    ("Todas las ventas de staging llegaron a fact_ventas",
     "SELECT count(*) FROM staging.ventas v "
     "LEFT JOIN analytics.fact_ventas f USING (folio, producto_id) "
     "WHERE f.folio IS NULL", True),
    ("Toda venta tiene tipo de cambio",
     "SELECT count(*) FROM analytics.fact_ventas WHERE usd_mxn IS NULL", True),
    ("El total en pesos cuadra con cantidad x precio",
     "SELECT count(*) FROM analytics.fact_ventas "
     "WHERE total_mxn <> cantidad * precio_unitario", True),
    ("Ningún pedido tiene fecha futura",
     "SELECT count(*) FROM analytics.fact_ventas WHERE fecha > current_date", True),
    ("Los días con ventas tienen dato de clima",
     "SELECT count(DISTINCT f.fecha) FROM analytics.fact_ventas f "
     "LEFT JOIN analytics.dim_clima c ON c.fecha = f.fecha "
     "WHERE c.fecha IS NULL OR c.lluvia_mm IS NULL", False),
    ("Precio vendido igual al de lista (descuentos o cambios de precio)",
     "SELECT count(*) FROM analytics.fact_ventas f "
     "JOIN analytics.dim_producto p USING (producto_id) "
     "WHERE f.precio_unitario <> p.precio_lista", False),
]
