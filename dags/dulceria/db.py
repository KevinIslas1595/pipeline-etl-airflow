"""CARGAR: acceso al data warehouse (PostgreSQL)."""

from contextlib import closing
from datetime import date

from dulceria import config


def obtener_conexion():
    """Conexión DB-API al warehouse usando la conexión de Airflow `dulceria_dw`."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=config.CONEXION_DW).get_conn()


def ejecutar_archivo_sql(nombre: str) -> None:
    sql = (config.CARPETA_SQL / nombre).read_text(encoding="utf-8")
    with closing(obtener_conexion()) as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(sql)
        conexion.commit()


def consultar(sql: str, parametros: tuple = ()) -> list[tuple]:
    with closing(obtener_conexion()) as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(sql, parametros)
            return cursor.fetchall()


def upsert(tabla: str, columnas: list[str], llaves: list[str], filas: list[tuple]) -> int:
    """Inserta o actualiza filas. Se puede correr las veces que sea sin duplicar.

    `tabla`, `columnas` y `llaves` vienen del código, nunca de los datos.
    """
    if not filas:
        return 0
    actualizar = [f"{c} = EXCLUDED.{c}" for c in columnas if c not in llaves]
    sql = (
        f"INSERT INTO {tabla} ({', '.join(columnas)}) "
        f"VALUES ({', '.join(['%s'] * len(columnas))}) "
        f"ON CONFLICT ({', '.join(llaves)}) "
        f"DO UPDATE SET {', '.join(actualizar + ['cargado_en = now()'])}"
    )
    with closing(obtener_conexion()) as conexion:
        with conexion.cursor() as cursor:
            cursor.executemany(sql, filas)
        conexion.commit()
    return len(filas)


def ultima_fecha(tabla: str) -> date | None:
    return consultar(f"SELECT max(fecha) FROM {tabla}")[0][0]


def registrar_carga(run_id: str, fuente: str, archivo: str, leidas: int,
                    rechazadas: int, duplicadas: int, cargadas: int) -> None:
    with closing(obtener_conexion()) as conexion:
        with conexion.cursor() as cursor:
            cursor.execute(
                "INSERT INTO control.bitacora_cargas "
                "(run_id, fuente, archivo_crudo, filas_leidas, filas_rechazadas, "
                " filas_duplicadas, filas_cargadas) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (run_id, fuente, archivo, leidas, rechazadas, duplicadas, cargadas),
            )
        conexion.commit()
