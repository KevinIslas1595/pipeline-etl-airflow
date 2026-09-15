"""Limpieza y validación de los datos crudos, fila por fila.

Solo usa la librería estándar de Python, así se puede probar sin Airflow ni
base de datos (ver tests/test_limpieza.py).
"""

import csv
import io
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import NamedTuple

COLUMNAS_VENTAS = ["folio", "fecha", "producto_id", "cantidad", "precio_unitario", "canal"]
CANALES = {"whatsapp": "WhatsApp", "tienda": "Tienda"}


class Resultado(NamedTuple):
    filas: list          # tuplas listas para insertar en la base
    rechazadas: list     # [{"linea": n, "motivo": "..."}]
    duplicadas: int = 0

    @property
    def leidas(self) -> int:
        return len(self.filas) + len(self.rechazadas) + self.duplicadas


class DatoInvalido(ValueError):
    pass


def _texto(valor, campo: str) -> str:
    texto = "" if valor is None else str(valor).strip()
    if not texto:
        raise DatoInvalido(f"{campo} vacío")
    return texto


def _entero_positivo(valor, campo: str) -> int:
    texto = _texto(valor, campo)
    try:
        numero = int(texto)
    except ValueError:
        raise DatoInvalido(f"{campo} no es un número entero: {texto!r}") from None
    if numero <= 0:
        raise DatoInvalido(f"{campo} debe ser mayor a 0: {numero}")
    return numero


def _dinero(valor, campo: str) -> Decimal:
    texto = _texto(valor, campo).replace("$", "").replace(",", "").strip()
    try:
        numero = Decimal(texto)
    except InvalidOperation:
        raise DatoInvalido(f"{campo} no es una cantidad: {valor!r}") from None
    if not numero.is_finite() or numero <= 0:
        raise DatoInvalido(f"{campo} debe ser mayor a 0: {valor!r}")
    return numero.quantize(Decimal("0.01"))


def _fecha(valor, campo: str) -> date:
    texto = _texto(valor, campo)
    for formato in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            pass
    raise DatoInvalido(f"{campo} no es una fecha válida: {texto!r}")


def limpiar_ventas(texto_csv: str) -> Resultado:
    """Ventas del CSV: normaliza formatos, descarta filas malas y duplicados.

    Llave de cada venta: (folio, producto_id). Si se repite, gana la última.
    """
    lector = csv.DictReader(io.StringIO(texto_csv))
    faltantes = [c for c in COLUMNAS_VENTAS if c not in (lector.fieldnames or [])]
    if faltantes:
        raise ValueError(f"Al CSV de ventas le faltan columnas: {', '.join(faltantes)}")

    ventas, rechazadas, duplicadas = {}, [], 0
    for linea, fila in enumerate(lector, start=2):
        try:
            folio = _texto(fila["folio"], "folio")
            producto_id = _entero_positivo(fila["producto_id"], "producto_id")
            cantidad = _entero_positivo(fila["cantidad"], "cantidad")
            precio = _dinero(fila["precio_unitario"], "precio_unitario")
            fecha = _fecha(fila["fecha"], "fecha")
            canal = CANALES.get(_texto(fila["canal"], "canal").lower())
            if canal is None:
                raise DatoInvalido(f"canal desconocido: {fila['canal']!r}")
        except DatoInvalido as error:
            rechazadas.append({"linea": linea, "motivo": str(error)})
            continue
        llave = (folio, producto_id)
        if llave in ventas:
            duplicadas += 1
        ventas[llave] = (folio, fecha, producto_id, cantidad, precio, canal)
    return Resultado(list(ventas.values()), rechazadas, duplicadas)


def limpiar_productos(datos: dict) -> Resultado:
    """Catálogo JSON: {"productos": [{"id", "nombre", "marca", "tipo", "precio"}]}."""
    productos = datos.get("productos") if isinstance(datos, dict) else None
    if not isinstance(productos, list):
        raise ValueError('El JSON de productos debe tener una lista "productos"')

    filas, rechazadas, duplicadas = {}, [], 0
    for posicion, producto in enumerate(productos, start=1):
        try:
            if not isinstance(producto, dict):
                raise DatoInvalido("el producto no es un objeto")
            producto_id = _entero_positivo(producto.get("id"), "id")
            fila = (
                producto_id,
                _texto(producto.get("nombre"), "nombre"),
                _texto(producto.get("marca"), "marca"),
                _texto(producto.get("tipo"), "tipo"),
                _dinero(producto.get("precio"), "precio"),
            )
        except DatoInvalido as error:
            rechazadas.append({"linea": posicion, "motivo": str(error)})
            continue
        if producto_id in filas:
            duplicadas += 1
        filas[producto_id] = fila
    return Resultado(list(filas.values()), rechazadas, duplicadas)


def parsear_clima(datos: dict) -> Resultado:
    """Respuesta de Open-Meteo -> (fecha, temp_max, temp_min, lluvia_mm)."""
    diario = datos.get("daily") or {}
    fechas = diario.get("time") or []
    maximas = diario.get("temperature_2m_max") or [None] * len(fechas)
    minimas = diario.get("temperature_2m_min") or [None] * len(fechas)
    lluvias = diario.get("precipitation_sum") or [None] * len(fechas)

    filas, rechazadas = [], []
    for posicion, (dia, t_max, t_min, lluvia) in enumerate(
            zip(fechas, maximas, minimas, lluvias), start=1):
        if t_max is None and t_min is None and lluvia is None:
            # Los días más recientes a veces aún no tienen medición.
            rechazadas.append({"linea": posicion, "motivo": f"{dia}: sin datos todavía"})
            continue
        filas.append((
            _fecha(dia, "fecha"),
            None if t_max is None else Decimal(str(t_max)),
            None if t_min is None else Decimal(str(t_min)),
            None if lluvia is None else Decimal(str(lluvia)),
        ))
    return Resultado(filas, rechazadas)


def parsear_tipo_cambio(datos: dict) -> Resultado:
    """Respuesta de Frankfurter -> (fecha, usd_mxn). No hay datos en fines de semana."""
    filas, rechazadas = [], []
    for dia, tasas in sorted((datos.get("rates") or {}).items()):
        valor = (tasas or {}).get("MXN")
        if valor is None or Decimal(str(valor)) <= 0:
            rechazadas.append({"linea": dia, "motivo": f"{dia}: sin tipo de cambio MXN"})
            continue
        filas.append((_fecha(dia, "fecha"), Decimal(str(valor))))
    return Resultado(filas, rechazadas)
