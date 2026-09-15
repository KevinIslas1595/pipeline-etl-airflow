"""EXTRAER: trae los datos de cada fuente y los guarda tal cual en la zona cruda.

Guardar la copia original antes de transformar permite volver a procesar un
día sin pedirle otra vez los datos a la fuente, y revisar qué llegó exactamente.
"""

import json
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from dulceria import config


def _ruta_cruda(fuente: str, extension: str) -> Path:
    carpeta = config.CARPETA_CRUDO / fuente
    carpeta.mkdir(parents=True, exist_ok=True)
    marca = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return carpeta / f"{fuente}_{marca}.{extension}"


def copiar_archivo(origen: Path, fuente: str) -> str:
    """Copia un archivo de entrada (CSV o JSON) a la zona cruda."""
    if not origen.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {origen}")
    destino = _ruta_cruda(fuente, origen.suffix.lstrip("."))
    shutil.copyfile(origen, destino)
    return str(destino)


def ventana_incremental(ultima_fecha: date | None, fin: date) -> tuple[date, date]:
    """Rango de fechas a pedirle a una API según lo que ya está cargado."""
    if ultima_fecha is None:
        inicio = config.FECHA_INICIO_HISTORICO
    else:
        inicio = max(ultima_fecha - timedelta(days=config.DIAS_REPROCESO),
                     config.FECHA_INICIO_HISTORICO)
    return min(inicio, fin), fin


def _descargar_json(url: str, params: dict, fuente: str) -> str:
    respuesta = requests.get(url, params=params, timeout=config.TIMEOUT_HTTP)
    respuesta.raise_for_status()
    datos = respuesta.json()
    destino = _ruta_cruda(fuente, "json")
    destino.write_text(json.dumps(datos, ensure_ascii=False), encoding="utf-8")
    return str(destino)


def descargar_clima(inicio: date, fin: date) -> str:
    params = {
        "latitude": config.LATITUD,
        "longitude": config.LONGITUD,
        "start_date": inicio.isoformat(),
        "end_date": fin.isoformat(),
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": config.ZONA_HORARIA,
    }
    return _descargar_json(config.URL_CLIMA, params, "clima")


def descargar_tipo_cambio(inicio: date, fin: date) -> str:
    url = f"{config.URL_TIPO_CAMBIO}/{inicio.isoformat()}..{fin.isoformat()}"
    return _descargar_json(url, {"base": "USD", "symbols": "MXN"}, "tipo_cambio")
