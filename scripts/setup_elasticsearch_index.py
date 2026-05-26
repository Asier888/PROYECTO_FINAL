#!/usr/bin/env python3
"""Crea el índice Elasticsearch con el mapping definido."""

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

MAPPING_PATH = ROOT / "config" / "elasticsearch_mapping.json"
INDEX = os.getenv("ELASTICSEARCH_INDEX", "climate_energy_iot")


def main() -> int:
    hosts = os.getenv("ELASTICSEARCH_HOSTS", "http://localhost:9200").split(",")
    user = os.getenv("ELASTICSEARCH_USER", "elastic")
    password = os.getenv("ELASTICSEARCH_PASSWORD", "changeme")
    cloud_id = os.getenv("ELASTICSEARCH_CLOUD_ID")
    api_key = os.getenv("ELASTICSEARCH_API_KEY")

    if cloud_id and api_key:
        client = Elasticsearch(cloud_id=cloud_id, api_key=api_key)
    else:
        client = Elasticsearch(hosts=hosts, basic_auth=(user, password))

    mapping = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))

    try:
        if client.indices.exists(index=INDEX):
            print(f"Índice '{INDEX}' ya existe.")
            return 0
        client.indices.create(index=INDEX, body=mapping)
        print(f"Índice '{INDEX}' creado correctamente.")
        return 0
    except ApiError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
