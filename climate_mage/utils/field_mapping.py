"""Mapeo explícito de campos para serialización JSON y Elasticsearch."""

from datetime import datetime
from typing import Any

# Mapping explícito: tipos lógicos por campo
RECORD_FIELD_MAPPING = {
    "date": "date",
    "country": "keyword",
    "avg_temperature": "float",
    "humidity": "float",
    "co2_emission": "float",
    "energy_consumption": "float",
    "renewable_share": "float",
    "urban_population": "float",
    "industrial_activity_index": "float",
    "energy_price": "float",
}

ENRICHED_FIELD_MAPPING = {
    **RECORD_FIELD_MAPPING,
    "pvpc_esios": "float",
    "potential_savings": "float",
    "esios_timestamp": "date",
    "alert_high_consumption_price": "boolean",
}


def coerce_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Aplica el mapping de tipos al registro de sensor."""
    record: dict[str, Any] = {}
    for field, field_type in RECORD_FIELD_MAPPING.items():
        value = raw.get(field)
        if value is None or (isinstance(value, float) and value != value):
            record[field] = None
            continue
        if field_type == "date":
            if isinstance(value, datetime):
                record[field] = value.date().isoformat()
            else:
                record[field] = str(value)[:10]
        elif field_type == "keyword":
            record[field] = str(value)
        elif field_type == "float":
            record[field] = float(value)
        else:
            record[field] = value
    return record


def elasticsearch_index_mapping() -> dict:
    """Mapping del índice Elasticsearch."""
    properties = {}
    for field, field_type in ENRICHED_FIELD_MAPPING.items():
        if field_type == "date":
            properties[field] = {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd"}
        elif field_type == "keyword":
            properties[field] = {"type": "keyword"}
        elif field_type == "boolean":
            properties[field] = {"type": "boolean"}
        else:
            properties[field] = {"type": "float"}
    return {"mappings": {"properties": properties}}
