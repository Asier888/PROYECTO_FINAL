import os
import sys
from datetime import datetime  # Crucial para el tiempo real en Kibana
from pathlib import Path
import pandas as pd

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test

# Añadir raíz del proyecto Mage al path
_PROJECT_ROOT = Path("/home/src")
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from climate_mage.utils.field_mapping import RECORD_FIELD_MAPPING, coerce_record
from climate_mage.utils.rabbitmq_client import publish_record


@data_loader
def load_and_publish_to_rabbitmq(*args, **kwargs) -> pd.DataFrame:
    """
    Lee el CSV y publica una ráfaga limpia con la hora exacta actual en RabbitMQ.
    """
    csv_path = os.getenv(
        "CSV_SOURCE_PATH",
        "/home/src/data/global_climate_energy_2020_2024.csv",
    )
    max_rows = 100  # Envía 100 registros de golpe para pintar las líneas del tirón

    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"CSV no encontrado en {csv_path}") from exc

    columns = list(RECORD_FIELD_MAPPING.keys())
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes en CSV: {missing}")

    print(f"[⚙️ Sistema] Generando ráfaga de {max_rows} mensajes con marcas de tiempo actualizadas...")

    published = 0
    records_enviados = []

    for idx, row in df.iterrows():
        if published >= max_rows:
            break

        raw = row[columns].to_dict()
        record = coerce_record(raw)

        # 🎯 SOLUCIÓN AL GRÁFICO VACÍO: Estampa Año-Mes-Día + Hora:Minuto:Segundo UTC (Zulu)
        fecha_real_utc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        record['date'] = fecha_real_utc

        try:
            publish_record(record)
            published += 1
            records_enviados.append(record)
        except Exception as exc:
            print(f"[❌] Error RabbitMQ en fila {idx}: {exc}")

    print(f"[🚀 Éxito] Ráfaga completada: {published} mensajes enviados a RabbitMQ.")
    
    # Devolvemos el DataFrame de lo que se acaba de enviar para el histórico de Mage
    return pd.DataFrame(records_enviados)


@test
def test_output(output, *args) -> None:
    assert output is not None