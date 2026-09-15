"""Los pasos del pipeline. El DAG solo los ordena; aquí está lo que hace cada uno.

Separarlos del DAG permite probarlos sin levantar Airflow.
"""

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from dulceria import calidad, config, db, extraer, limpieza

log = logging.getLogger(__name__)


def _ayer() -> date:
    return datetime.now(ZoneInfo(config.ZONA_HORARIA)).date() - timedelta(days=1)


def _registrar(run_id: str, fuente: str, ruta: str, resultado: limpieza.Resultado,
               cargadas: int) -> int:
    for rechazo in resultado.rechazadas:
        log.warning("[%s] fila rechazada (%s): %s", fuente, rechazo["linea"], rechazo["motivo"])
    log.info("[%s] leídas=%s rechazadas=%s duplicadas=%s cargadas=%s", fuente,
             resultado.leidas, len(resultado.rechazadas), resultado.duplicadas, cargadas)
    db.registrar_carga(run_id, fuente, ruta, resultado.leidas, len(resultado.rechazadas),
                       resultado.duplicadas, cargadas)
    return cargadas


def preparar_tablas() -> None:
    db.ejecutar_archivo_sql("00_esquema.sql")


# --- Fuente 1: CSV de ventas -------------------------------------------------

def extraer_ventas() -> str:
    return extraer.copiar_archivo(config.ARCHIVO_VENTAS, "ventas")


def cargar_ventas(ruta: str, run_id: str) -> int:
    resultado = limpieza.limpiar_ventas(Path(ruta).read_text(encoding="utf-8-sig"))
    cargadas = db.upsert(
        "staging.ventas",
        ["folio", "fecha", "producto_id", "cantidad", "precio_unitario", "canal"],
        ["folio", "producto_id"],
        resultado.filas,
    )
    return _registrar(run_id, "ventas_csv", ruta, resultado, cargadas)


# --- Fuente 2: JSON de productos ---------------------------------------------

def extraer_productos() -> str:
    return extraer.copiar_archivo(config.ARCHIVO_PRODUCTOS, "productos")


def cargar_productos(ruta: str, run_id: str) -> int:
    datos = json.loads(Path(ruta).read_text(encoding="utf-8-sig"))
    resultado = limpieza.limpiar_productos(datos)
    cargadas = db.upsert(
        "staging.productos",
        ["producto_id", "nombre", "marca", "tipo", "precio"],
        ["producto_id"],
        resultado.filas,
    )
    return _registrar(run_id, "productos_json", ruta, resultado, cargadas)


# --- Fuente 3: API de clima --------------------------------------------------

def extraer_clima(hasta: date | None = None) -> str:
    inicio, fin = extraer.ventana_incremental(db.ultima_fecha("staging.clima"), hasta or _ayer())
    log.info("[clima_api] pidiendo del %s al %s", inicio, fin)
    return extraer.descargar_clima(inicio, fin)


def cargar_clima(ruta: str, run_id: str) -> int:
    resultado = limpieza.parsear_clima(json.loads(Path(ruta).read_text(encoding="utf-8")))
    cargadas = db.upsert(
        "staging.clima",
        ["fecha", "temp_max", "temp_min", "lluvia_mm"],
        ["fecha"],
        resultado.filas,
    )
    return _registrar(run_id, "clima_api", ruta, resultado, cargadas)


# --- Fuente 4: API de tipo de cambio -----------------------------------------

def extraer_tipo_cambio(hasta: date | None = None) -> str:
    inicio, fin = extraer.ventana_incremental(db.ultima_fecha("staging.tipo_cambio"),
                                              hasta or _ayer())
    log.info("[tipo_cambio_api] pidiendo del %s al %s", inicio, fin)
    return extraer.descargar_tipo_cambio(inicio, fin)


def cargar_tipo_cambio(ruta: str, run_id: str) -> int:
    resultado = limpieza.parsear_tipo_cambio(json.loads(Path(ruta).read_text(encoding="utf-8")))
    cargadas = db.upsert(
        "staging.tipo_cambio",
        ["fecha", "usd_mxn"],
        ["fecha"],
        resultado.filas,
    )
    return _registrar(run_id, "tipo_cambio_api", ruta, resultado, cargadas)


# --- Transformar, validar y reportar -----------------------------------------

def transformar() -> None:
    db.ejecutar_archivo_sql("transformar.sql")


def validar_calidad() -> dict:
    fallas, avisos = [], []
    for descripcion, consulta, obligatoria in calidad.REGLAS:
        problemas = db.consultar(consulta)[0][0]
        if problemas == 0:
            log.info("OK     %s", descripcion)
        elif obligatoria:
            log.error("FALLA  %s (%s casos)", descripcion, problemas)
            fallas.append(f"{descripcion}: {problemas}")
        else:
            log.warning("AVISO  %s (%s casos)", descripcion, problemas)
            avisos.append(f"{descripcion}: {problemas}")
    if fallas:
        raise ValueError("Falló la validación de calidad: " + "; ".join(fallas))
    return {"reglas": len(calidad.REGLAS), "avisos": avisos}


def resumen() -> dict:
    totales = db.consultar(
        "SELECT count(DISTINCT folio), coalesce(sum(cantidad), 0), coalesce(sum(total_mxn), 0), "
        "coalesce(sum(total_usd), 0), min(fecha), max(fecha) FROM analytics.fact_ventas"
    )[0]
    log.info("Pedidos: %s | piezas: %s | ingreso: $%s MXN (≈ $%s USD) | del %s al %s", *totales)

    for fila in db.consultar(
            "SELECT fecha, dia_semana, pedidos, piezas, ingreso_mxn, ingreso_usd, temp_max, lluvia_mm "
            "FROM analytics.reporte_diario ORDER BY fecha DESC LIMIT 7"):
        log.info("Día %s %-9s pedidos=%s piezas=%s $%s MXN ($%s USD) temp=%s°C lluvia=%s mm",
                 *fila)

    for nombre, piezas, ingreso in db.consultar(
            "SELECT nombre, piezas, ingreso_mxn FROM analytics.ventas_por_producto "
            "ORDER BY ingreso_mxn DESC LIMIT 3"):
        log.info("Top producto: %s — %s piezas, $%s MXN", nombre, piezas, ingreso)

    for clima, dias, pedidos, ingreso in db.consultar(
            "SELECT clima, dias, pedidos_promedio, ingreso_promedio_mxn "
            "FROM analytics.ventas_segun_lluvia ORDER BY clima"):
        log.info("%s: %s días, %s pedidos/día, $%s MXN/día", clima, dias, pedidos, ingreso)

    return {"pedidos": totales[0], "piezas": int(totales[1]), "ingreso_mxn": str(totales[2]),
            "desde": str(totales[4]), "hasta": str(totales[5])}
