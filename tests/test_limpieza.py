"""Pruebas de la limpieza de datos. No necesitan Airflow ni base de datos.

Correr:  python -m unittest discover -s tests -v
"""

import json
import sys
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))

from dulceria import config, extraer, limpieza  # noqa: E402

ENCABEZADO = "folio,fecha,producto_id,cantidad,precio_unitario,canal\n"


class LimpiarVentas(unittest.TestCase):

    def test_normaliza_formatos(self):
        csv = ENCABEZADO + (
            "DP-1,2026-07-01,4,2,$80.80, whatsapp \n"
            "DP-2,05/07/2026,7,1,\"1,070.70\",TIENDA\n"
        )
        resultado = limpieza.limpiar_ventas(csv)
        self.assertEqual(resultado.rechazadas, [])
        self.assertEqual(resultado.filas, [
            ("DP-1", date(2026, 7, 1), 4, 2, Decimal("80.80"), "WhatsApp"),
            ("DP-2", date(2026, 7, 5), 7, 1, Decimal("1070.70"), "Tienda"),
        ])

    def test_rechaza_filas_invalidas_con_motivo(self):
        csv = ENCABEZADO + (
            "DP-1,2026-07-01,4,,80.80,Tienda\n"
            "DP-2,2026-07-01,4,0,80.80,Tienda\n"
            "DP-3,2026-07-01,4,1,pendiente,Tienda\n"
            "DP-4,2026-13-45,4,1,80.80,Tienda\n"
            "DP-5,2026-07-01,4,1,80.80,Mercado Libre\n"
            ",2026-07-01,4,1,80.80,Tienda\n"
            "DP-7,2026-07-01,4\n"
            "DP-8,2026-07-01,4,1,80.80,Tienda\n"
        )
        resultado = limpieza.limpiar_ventas(csv)
        self.assertEqual(len(resultado.filas), 1)
        self.assertEqual([r["linea"] for r in resultado.rechazadas], [2, 3, 4, 5, 6, 7, 8])
        self.assertIn("cantidad vacío", resultado.rechazadas[0]["motivo"])
        self.assertIn("mayor a 0", resultado.rechazadas[1]["motivo"])
        self.assertIn("canal desconocido", resultado.rechazadas[4]["motivo"])
        self.assertEqual(resultado.leidas, 8)

    def test_duplicados_se_quedan_con_el_ultimo(self):
        csv = ENCABEZADO + (
            "DP-1,2026-07-01,4,2,80.80,Tienda\n"
            "DP-1,2026-07-01,4,3,80.80,Tienda\n"
            "DP-1,2026-07-01,5,1,72.30,Tienda\n"
        )
        resultado = limpieza.limpiar_ventas(csv)
        self.assertEqual(resultado.duplicadas, 1)
        self.assertEqual(len(resultado.filas), 2)
        self.assertEqual(resultado.filas[0][3], 3)
        self.assertEqual(resultado.leidas, 3)

    def test_falla_si_faltan_columnas(self):
        with self.assertRaisesRegex(ValueError, "canal"):
            limpieza.limpiar_ventas("folio,fecha,producto_id,cantidad,precio_unitario\n")


class LimpiarProductos(unittest.TestCase):

    def test_valida_catalogo(self):
        datos = {"productos": [
            {"id": 1, "nombre": "Ositos", "marca": "Gomitas Lucky", "tipo": "Dulce", "precio": 80.8},
            {"id": 2, "nombre": "Orugas", "marca": "Gomitas Lucky", "tipo": "Dulce", "precio": None},
            {"id": 1, "nombre": "Ositos 1 kg", "marca": "Gomitas Lucky", "tipo": "Dulce", "precio": 81},
            "no soy un producto",
        ]}
        resultado = limpieza.limpiar_productos(datos)
        self.assertEqual(resultado.filas,
                         [(1, "Ositos 1 kg", "Gomitas Lucky", "Dulce", Decimal("81.00"))])
        self.assertEqual(len(resultado.rechazadas), 2)
        self.assertEqual(resultado.duplicadas, 1)

    def test_falla_sin_lista(self):
        with self.assertRaises(ValueError):
            limpieza.limpiar_productos({"items": []})


class ParsearApis(unittest.TestCase):

    def test_clima(self):
        datos = {"daily": {
            "time": ["2026-09-12", "2026-09-13"],
            "temperature_2m_max": [23.3, None],
            "temperature_2m_min": [13.6, None],
            "precipitation_sum": [32.3, None],
        }}
        resultado = limpieza.parsear_clima(datos)
        self.assertEqual(resultado.filas,
                         [(date(2026, 9, 12), Decimal("23.3"), Decimal("13.6"), Decimal("32.3"))])
        self.assertEqual(len(resultado.rechazadas), 1)

    def test_tipo_cambio(self):
        datos = {"base": "USD", "rates": {
            "2026-09-14": {"MXN": 17.0721},
            "2026-09-11": {"MXN": 16.9771},
            "2026-09-12": {},
        }}
        resultado = limpieza.parsear_tipo_cambio(datos)
        self.assertEqual(resultado.filas, [
            (date(2026, 9, 11), Decimal("16.9771")),
            (date(2026, 9, 14), Decimal("17.0721")),
        ])
        self.assertEqual(len(resultado.rechazadas), 1)


class VentanaIncremental(unittest.TestCase):

    def test_primera_carga_trae_todo_el_historico(self):
        self.assertEqual(extraer.ventana_incremental(None, date(2026, 9, 13)),
                         (config.FECHA_INICIO_HISTORICO, date(2026, 9, 13)))

    def test_despues_reprocesa_solo_los_ultimos_dias(self):
        inicio, fin = extraer.ventana_incremental(date(2026, 9, 12), date(2026, 9, 13))
        self.assertEqual(inicio, date(2026, 9, 12 - config.DIAS_REPROCESO))
        self.assertEqual(fin, date(2026, 9, 13))


class ArchivosDeEntrada(unittest.TestCase):
    """Los archivos de ejemplo del repo deben poder cargarse."""

    def test_ventas_csv(self):
        texto = (RAIZ / "data" / "entrada" / "ventas.csv").read_text(encoding="utf-8-sig")
        resultado = limpieza.limpiar_ventas(texto)
        self.assertGreater(len(resultado.filas), 1000)
        self.assertEqual(len(resultado.rechazadas), 3)
        self.assertEqual(resultado.duplicadas, 2)

    def test_productos_json(self):
        datos = json.loads((RAIZ / "data" / "entrada" / "productos.json").read_text(encoding="utf-8"))
        resultado = limpieza.limpiar_productos(datos)
        self.assertEqual(len(resultado.filas), 13)
        self.assertEqual(resultado.rechazadas, [])


if __name__ == "__main__":
    unittest.main()
