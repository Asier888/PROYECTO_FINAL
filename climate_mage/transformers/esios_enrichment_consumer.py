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
    Se conecta a RabbitMQ de forma síncrona, consume la cola por completo
    y enriquece los datos con los costes financieros del PVPC.
    """
    queue_name = os.getenv("RABBITMQ_QUEUE", "climate_records")
    max_messages = int(os.getenv("CONSUMER_BATCH_SIZE", "5000"))

    print(f"[Consumidor] Conectando a la cola '{queue_name}' de RabbitMQ...")
    
    try:
        # Intento de conexión principal (Red interna Docker de Mage)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="rabbitmq",
                credentials=pika.PlainCredentials("guest", "guest")
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
    except Exception as e:
        # Intento de respaldo (Por si ejecutáis localmente fuera del puente Docker)
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="host.docker.internal", port=5672)
            )
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
        except Exception as e_inner:
            print(f"[❌ Error Crítico] No se pudo conectar a RabbitMQ: {e_inner}")
            return pd.DataFrame()

    records = []
    print("[Consumidor] Extrayendo ráfaga de mensajes de la cola...")
    
    # Bucle de lectura masiva rápida
    for _ in range(max_messages):
        method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=True)
        if method_frame:
            record = json.loads(body.decode("utf-8"))
            records.append(record)
        else:
            # Si ya no quedan más mensajes en RabbitMQ, rompemos el bucle
            break

    connection.close()
    print(f"[Consumidor] Se han descargado {len(records)} mensajes nuevos.")

    if not records:
        print("[⚠️ Alerta] La cola estaba vacía. No hay datos nuevos para transformar.")
        return pd.DataFrame()

    # Convertimos los JSONs extraídos en el DataFrame oficial de Pandas
    df = pd.DataFrame(records)

    # Lógica de negocio y enriquecimiento energético
    pvpc_price = 0.12656
    print(f"[Consumidor] Enriqueciendo datos con tarifa PVPC Fija: {pvpc_price} €/kWh")

    df["energy_price_eur_kwh"] = pvpc_price
    df["energy_cost_eur"] = df["energy_consumption"] * df["energy_price_eur_kwh"]
    df["potential_savings_eur"] = df["energy_cost_eur"] * 0.15  # Estimación del 15% de ahorro potencial

    print(f"[📊 Éxito] DataFrame listo para Elasticsearch. Filas estructuradas: {len(df)}")
    return df


@test
def test_output(output, *args) -> None:
    assert output is not None