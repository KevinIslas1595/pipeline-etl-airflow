"""Pruebas de la estructura del DAG. Necesitan Airflow instalado (corren en Docker):

    docker compose run --rm airflow-cli python -m unittest discover -s /opt/airflow/tests -v
"""

import importlib
import sys
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))

try:
    import airflow.sdk  # noqa: F401
    HAY_AIRFLOW = True
except ImportError:
    HAY_AIRFLOW = False

FUENTES = ["ventas_csv", "productos_json", "clima_api", "tipo_cambio_api"]


@unittest.skipUnless(HAY_AIRFLOW, "Airflow no está instalado")
class EstructuraDelDag(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dag = importlib.import_module("etl_dulceria").dag_etl

    def test_datos_generales(self):
        self.assertEqual(self.dag.dag_id, "etl_dulceria_premium")
        self.assertFalse(self.dag.catchup)
        self.assertEqual(len(self.dag.tasks), 12)

    def test_cada_fuente_extrae_despues_de_preparar_y_antes_de_cargar(self):
        for fuente in FUENTES:
            extraer = self.dag.get_task(f"{fuente}.extraer")
            cargar = self.dag.get_task(f"{fuente}.cargar")
            self.assertEqual(extraer.upstream_task_ids, {"preparar_tablas"})
            self.assertEqual(cargar.upstream_task_ids, {f"{fuente}.extraer"})

    def test_transformar_espera_a_las_cuatro_fuentes(self):
        self.assertEqual(self.dag.get_task("transformar").upstream_task_ids,
                         {f"{fuente}.cargar" for fuente in FUENTES})

    def test_final_del_flujo(self):
        self.assertEqual(self.dag.get_task("validar_calidad").upstream_task_ids, {"transformar"})
        self.assertEqual(self.dag.get_task("validar_calidad").retries, 0)
        self.assertEqual(self.dag.get_task("resumen").upstream_task_ids, {"validar_calidad"})


if __name__ == "__main__":
    unittest.main()
