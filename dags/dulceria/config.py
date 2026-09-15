"""Configuración del pipeline: rutas, conexión y parámetros de las fuentes."""

import os
from datetime import date
from pathlib import Path

# Conexión al data warehouse. En Docker se define con la variable de entorno
# AIRFLOW_CONN_DULCERIA_DW (ver docker-compose.yml).
CONEXION_DW = "dulceria_dw"

# Carpeta base. En los contenedores es /opt/airflow; las pruebas la cambian.
BASE = Path(os.environ.get("DULCERIA_BASE", "/opt/airflow"))
CARPETA_SQL = BASE / "sql"
CARPETA_ENTRADA = BASE / "data" / "entrada"
CARPETA_CRUDO = BASE / "data" / "crudo"

# Fuente 1 (CSV) y fuente 2 (JSON): archivos que exporta la tienda.
ARCHIVO_VENTAS = CARPETA_ENTRADA / "ventas.csv"
ARCHIVO_PRODUCTOS = CARPETA_ENTRADA / "productos.json"

# Fuente 3 (API): clima diario de la Ciudad de México, Open-Meteo (sin API key).
URL_CLIMA = "https://archive-api.open-meteo.com/v1/archive"
LATITUD = 19.4326
LONGITUD = -99.1332

# Fuente 4 (API): tipo de cambio USD -> MXN del Banco Central Europeo,
# vía Frankfurter (sin API key).
URL_TIPO_CAMBIO = "https://api.frankfurter.dev/v1"

ZONA_HORARIA = "America/Mexico_City"
TIMEOUT_HTTP = 30

# Carga incremental de las APIs: la primera vez se trae todo desde
# FECHA_INICIO_HISTORICO; después, solo desde la última fecha guardada menos
# DIAS_REPROCESO (por si la API corrigió datos de días recientes).
FECHA_INICIO_HISTORICO = date(2026, 6, 1)
DIAS_REPROCESO = 7
