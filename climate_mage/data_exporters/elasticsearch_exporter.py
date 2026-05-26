"""
Bloque Data Exporter: persiste DataFrame enriquecido directamente en Elasticsearch.
"""

import os
import sys
from pathlib import Path

import pandas as pd
from elasticsearch import Elasticsearch, helpers  # Conexión nativa

if "data_exporter" not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test

# Añadir raíz del proyecto Mage al path de forma segura
_PROJECT_ROOT = Path("/home/src")
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


@data_exporter
def export_to_elasticsearch(data: pd.DataFrame, *args, **kwargs) -> None:
    """Envía las filas limpias y enriquecidas directamente a Elasticsearch."""
    if data is None or data.empty:
        print("[Exporter] Sin datos para exportar.")
        return

    # Configuraciones del índice e historial
    index_name = os.getenv("ELASTICSEARCH_INDEX", "climate_iot_data")
    es_host = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")

    print(f"[Exporter] Conectando a Elasticsearch en {es_host}...")
    es = Elasticsearch([es_host])

    # Generar las acciones en lote (bulk) para optimizar la inserción
    actions = []
    for _, row in data.iterrows():
        # Convertimos la fila de Pandas a un diccionario limpio de Python
        record = row.to_dict()
        
        # Aseguramos tipos de datos correctos para evitar fallos de mapeo en Elastic
        doc = {
            "date": str(record.get("date")),
            "country": str(record.get("country")),
            "avg_temperature": float(record.get("avg_temperature", 0.0)),
            "humidity": float(record.get("humidity", 0.0)),
            "co2_emission": float(record.get("co2_emission", 0.0)),
            "energy_consumption": float(record.get("energy_consumption", 0.0)),
            "renewable_share": float(record.get("renewable_share", 0.0)),
            "urban_population": int(record.get("urban_population", 0)),
            "industrial_activity_index": float(record.get("industrial_activity_index", 0.0)),
            "energy_price": float(record.get("energy_price", 0.0)),
            "energy_price_eur_kwh": float(record.get("energy_price_eur_kwh", 0.0)),
            "energy_cost_eur": float(record.get("energy_cost_eur", 0.0)),
            "potential_savings_eur": float(record.get("potential_savings_eur", 0.0))
        }

        actions.append({
            "_index": index_name,
            "_source": doc
        })

    try:
        # Inserción masiva ultra rápida
        success, errors = helpers.bulk(es, actions)
        print(f"[Exporter] ¡Éxito! Indexados correctamente {success} documentos en el índice '{index_name}'.")
        if errors:
            print(f"[Exporter] Alerta, hubo algunos errores: {errors}")
    except Exception as e:
        print(f"[Exporter] Error crítico al indexar en Elasticsearch: {e}")
        raise


@test
def test_output(*args) -> None:
    pass