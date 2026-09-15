"""
### ETL Dulcería Premium

Junta **4 fuentes** en un data warehouse PostgreSQL, todos los días a las 7:00 a.m.
(hora de la Ciudad de México):

| # | Fuente | Tipo | Qué trae |
|---|--------|------|----------|
| 1 | `data/entrada/ventas.csv` | Archivo CSV | Ventas por pedido y producto |
| 2 | `data/entrada/productos.json` | Archivo JSON | Catálogo con marca, tipo y precio |
| 3 | Open-Meteo | API REST | Temperatura y lluvia diaria en CDMX |
| 4 | Frankfurter (BCE) | API REST | Tipo de cambio USD → MXN |

Flujo: `preparar_tablas` → 4 grupos *extraer → cargar* en paralelo →
`transformar` → `validar_calidad` → `resumen`.

Cada paso se puede repetir sin duplicar datos (upsert por llave).
"""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task, task_group

from dulceria import pasos


def _run_id() -> str:
    return get_current_context()["run_id"]


@dag(
    dag_id="etl_dulceria_premium",
    description="ETL diario: ventas CSV + productos JSON + API clima + API tipo de cambio",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Mexico_City"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "kevin",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=15),
    },
    tags=["etl", "postgres", "dulceria"],
    doc_md=__doc__,
)
def etl_dulceria_premium():

    @task
    def preparar_tablas():
        pasos.preparar_tablas()

    # --- Fuente 1: CSV ---------------------------------------------------------
    @task_group(group_id="ventas_csv", tooltip="Fuente 1: archivo CSV de ventas")
    def ventas_csv():
        @task
        def extraer() -> str:
            return pasos.extraer_ventas()

        @task
        def cargar(ruta: str) -> int:
            return pasos.cargar_ventas(ruta, _run_id())

        cargar(extraer())

    # --- Fuente 2: JSON --------------------------------------------------------
    @task_group(group_id="productos_json", tooltip="Fuente 2: catálogo JSON")
    def productos_json():
        @task
        def extraer() -> str:
            return pasos.extraer_productos()

        @task
        def cargar(ruta: str) -> int:
            return pasos.cargar_productos(ruta, _run_id())

        cargar(extraer())

    # --- Fuente 3: API clima ---------------------------------------------------
    @task_group(group_id="clima_api", tooltip="Fuente 3: API Open-Meteo")
    def clima_api():
        @task
        def extraer() -> str:
            return pasos.extraer_clima()

        @task
        def cargar(ruta: str) -> int:
            return pasos.cargar_clima(ruta, _run_id())

        cargar(extraer())

    # --- Fuente 4: API tipo de cambio -----------------------------------------
    @task_group(group_id="tipo_cambio_api", tooltip="Fuente 4: API Frankfurter")
    def tipo_cambio_api():
        @task
        def extraer() -> str:
            return pasos.extraer_tipo_cambio()

        @task
        def cargar(ruta: str) -> int:
            return pasos.cargar_tipo_cambio(ruta, _run_id())

        cargar(extraer())

    @task
    def transformar():
        pasos.transformar()

    # Sin reintentos: si los datos están mal, reintentar no los arregla.
    @task(retries=0)
    def validar_calidad() -> dict:
        return pasos.validar_calidad()

    @task
    def resumen() -> dict:
        return pasos.resumen()

    # Los grupos no regresan nada a propósito: así `>>` conecta el grupo
    # completo (su "extraer" queda después de preparar_tablas).
    fuentes = [ventas_csv(), productos_json(), clima_api(), tipo_cambio_api()]
    preparadas = preparar_tablas()
    transformadas = transformar()

    preparadas >> fuentes
    fuentes >> transformadas
    transformadas >> validar_calidad() >> resumen()


dag_etl = etl_dulceria_premium()
