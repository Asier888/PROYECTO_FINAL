"""Cliente Elasticsearch para Azure o local."""

import os
from typing import Any, Iterable

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError, ConnectionError as ESConnectionError

from climate_mage.utils.field_mapping import elasticsearch_index_mapping


def get_elasticsearch_client() -> Elasticsearch:
    """Crea cliente según variables de entorno."""
    hosts = os.getenv("ELASTICSEARCH_HOSTS", "http://elasticsearch:9200").split(",")
    user = os.getenv("ELASTICSEARCH_USER")
    password = os.getenv("ELASTICSEARCH_PASSWORD")
    api_key = os.getenv("ELASTICSEARCH_API_KEY")
    cloud_id = os.getenv("ELASTICSEARCH_CLOUD_ID")

    kwargs: dict[str, Any] = {"hosts": hosts, "request_timeout": 60}
    if cloud_id:
        kwargs = {"cloud_id": cloud_id, "request_timeout": 60}
    if api_key:
        kwargs["api_key"] = api_key
    elif user and password:
        kwargs["basic_auth"] = (user, password)

    return Elasticsearch(**kwargs)


def ensure_index(client: Elasticsearch, index_name: str) -> None:
    """Crea el índice con mapping si no existe."""
    try:
        if client.indices.exists(index=index_name):
            return
        client.indices.create(index=index_name, body=elasticsearch_index_mapping())
    except (ApiError, ESConnectionError) as exc:
        raise ConnectionError(f"No se pudo crear/verificar índice {index_name}: {exc}") from exc


def bulk_index_documents(
    documents: Iterable[dict[str, Any]],
    index_name: str | None = None,
) -> dict[str, int]:
    """Indexa documentos en Elasticsearch."""
    index_name = index_name or os.getenv("ELASTICSEARCH_INDEX", "climate_energy_iot")
    client = get_elasticsearch_client()

    try:
        ensure_index(client, index_name)
    except ConnectionError:
        raise

    success = 0
    errors = 0
    for doc in documents:
        doc_id = f"{doc.get('date')}_{doc.get('country')}_{doc.get('esios_timestamp', '')}"
        try:
            client.index(index=index_name, id=doc_id, document=doc)
            success += 1
        except (ApiError, ESConnectionError) as exc:
            errors += 1
            print(f"Error indexando documento {doc_id}: {exc}")

    return {"indexed": success, "errors": errors}
