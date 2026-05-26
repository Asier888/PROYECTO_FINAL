"""Cliente RabbitMQ (topic exchange) con pika."""

import json
import os
from typing import Any, Callable, Optional

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

DEFAULT_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "climate.iot")
DEFAULT_QUEUE = os.getenv("RABBITMQ_QUEUE", "climate.sensor.records")
DEFAULT_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY", "sensor.record")


def _connection_params() -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "guest"),
            os.getenv("RABBITMQ_PASSWORD", "guest"),
        ),
        heartbeat=600,
        blocked_connection_timeout=300,
    )


def setup_topology(channel: BlockingChannel) -> None:
    """Declara exchange topic, cola durable y binding."""
    exchange = DEFAULT_EXCHANGE
    queue = DEFAULT_QUEUE
    routing_key = DEFAULT_ROUTING_KEY

    channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
    channel.queue_declare(queue=queue, durable=True)
    channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key)


def publish_record(record: dict[str, Any], routing_key: Optional[str] = None) -> None:
    """Publica un registro JSON en el topic exchange."""
    routing_key = routing_key or DEFAULT_ROUTING_KEY
    try:
        connection = pika.BlockingConnection(_connection_params())
        channel = connection.channel()
        setup_topology(channel)
        channel.basic_publish(
            exchange=DEFAULT_EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(record, default=str),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
            ),
        )
        connection.close()
    except pika.exceptions.AMQPError as exc:
        raise ConnectionError(f"Error publicando en RabbitMQ: {exc}") from exc


def consume_records(
    callback: Callable[[dict[str, Any]], None],
    max_messages: Optional[int] = None,
    timeout_seconds: int = 30,
) -> int:
    """
    Consume mensajes de la cola y los pasa al callback.
    Devuelve el número de mensajes procesados.
    """
    processed = 0

    try:
        connection = pika.BlockingConnection(_connection_params())
        channel = connection.channel()
        setup_topology(channel)
        channel.basic_qos(prefetch_count=1)

        def on_message(
            ch: BlockingChannel,
            method: Basic.Deliver,
            _properties: BasicProperties,
            body: bytes,
        ) -> None:
            nonlocal processed
            try:
                payload = json.loads(body.decode("utf-8"))
                callback(payload)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                processed += 1
                if max_messages and processed >= max_messages:
                    ch.stop_consuming()
            except (json.JSONDecodeError, ValueError) as exc:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                print(f"Mensaje inválido descartado: {exc}")

        channel.basic_consume(queue=DEFAULT_QUEUE, on_message_callback=on_message, auto_ack=False)

        if max_messages:
            connection.process_data_events(time_limit=timeout_seconds)
            while processed < max_messages:
                connection.process_data_events(time_limit=1)
                if not channel.is_open:
                    break
        else:
            channel.start_consuming()

        if channel.is_open:
            channel.stop_consuming()
        connection.close()
    except pika.exceptions.AMQPError as exc:
        raise ConnectionError(f"Error consumiendo de RabbitMQ: {exc}") from exc

    return processed


def drain_queue_to_list(timeout_seconds: int = 120) -> list[dict[str, Any]]:
    """Vacía mensajes disponibles en la cola hasta timeout."""
    records: list[dict[str, Any]] = []
    idle_rounds = 0
    max_idle = 3

    try:
        connection = pika.BlockingConnection(_connection_params())
        channel = connection.channel()
        setup_topology(channel)

        while idle_rounds < max_idle:
            method, _properties, body = channel.basic_get(queue=DEFAULT_QUEUE, auto_ack=True)
            if method is None:
                idle_rounds += 1
                connection.sleep(1)
                continue
            idle_rounds = 0
            records.append(json.loads(body.decode("utf-8")))

        connection.close()
    except pika.exceptions.AMQPError as exc:
        raise ConnectionError(f"Error leyendo cola RabbitMQ: {exc}") from exc

    return records
