"""
Bloque Transformer (Suscriptor): consume RabbitMQ de forma nativa,
limpia con Pandas, aplica precio de la luz y calcula ahorros.
"""

import os
import sys
import json
from pathlib import Path

import pandas as pd
import pika  # Conexión nativa a RabbitMQ

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs) -> pd.DataFrame:
    """
    Se conecta a RabbitMQ usando pika directamente, vacía la cola
    y genera el DataFrame enriquecido para Elasticsearch.
    """
    queue_name = os.getenv("RABBITMQ_QUEUE", "climate_records")
    max_messages = int(os.getenv("CONSUMER_BATCH_SIZE", "5000"))

    print(f"[Consumidor] Conectando directamente a RabbitMQ en localhost...")
    
    # Conexión nativa usando las credenciales por defecto de tu docker-compose
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="rabbitmq",  # Nombre del servicio en tu red docker
                credentials=pika.PlainCredentials("guest", "guest")
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
    except Exception as e:
        print(f"[Consumidor] Error crítico al conectar a RabbitMQ: {e}")
        return pd.DataFrame()

    records = []
    print("[Consumidor] Vaciando mensajes acumulados...")
    
    # Hacemos el barrido rápido de mensajes
    for _ in range(max_messages):
        method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=True)
        if method_frame:
            record = json.loads(body.decode("utf-8"))
            records.append(record)
        else:
            break

    # Cerramos la conexión limpia
    connection.close()

    print(f"[Consumidor] Se han extraído {len(records)} mensajes de la cola.")

    if not records:
        print("[Consumidor] Alerta: No se encontraron mensajes nuevos. Devolviendo estructura vacía.")
        return pd.DataFrame()

    # Convertir a DataFrame y procesar con Pandas
    df = pd.DataFrame(records)

    # Añadimos el precio fijo que acordamos
    pvpc_price = 0.12656
    print(f"[Consumidor] Aplicando precio de la energía fijo: {pvpc_price} €/kWh")

    # Cálculos analíticos de costes y ahorros
    df["energy_price_eur_kwh"] = pvpc_price
    df["energy_cost_eur"] = df["energy_consumption"] * df["energy_price_eur_kwh"]
    df["potential_savings_eur"] = df["energy_cost_eur"] * 0.15

    print(f"[Consumidor] Enriquecimiento completado con éxito. Filas procesadas: {len(df)}")
    return df


@test
def test_output(output, *args) -> None:
    assert output is not None