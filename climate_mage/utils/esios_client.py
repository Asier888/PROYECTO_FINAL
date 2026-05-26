"""Cliente REST para la API ESIOS - Modo Simulado con Datos Reales Capturados."""

import os
from typing import Any, Optional

ESIOS_BASE_URL = os.getenv("ESIOS_API_URL", "https://api.esios.ree.es")
ESIOS_INDICATOR_ID = os.getenv("ESIOS_INDICATOR_ID", "1001")


def _headers() -> dict[str, str]:
    api_key = os.getenv("ESIOS_API_KEY", "")
    return {
        "Accept": "application/json; application/vnd.esios-api-v1+json",
        "Content-Type": "application/json",
        "x-api-key": api_key,
    }


def fetch_pvpc_for_timestamp(record_date: str) -> dict[str, Any]:
    """
    Devuelve los datos reales que has extraído de la web de ESIOS.
    Evita fallos por falta de API KEY o problemas con peticiones OPTIONS.
    """
    # Tomamos el primer valor de tu JSON real: 126.56 €/MWh
    raw_mwh = 126.56
    
    # Conversión matemática a €/kWh (126.56 / 1000 = 0.12656)
    pvpc_kwh = round(raw_mwh / 1000.0, 6)
    ts = "2026-05-26T00:00:00.000+02:00"

    return {
        "pvpc_esios": pvpc_kwh,
        "esios_timestamp": ts,
        "esios_raw_eur_mwh": raw_mwh,
    }


def compute_potential_savings(energy_price: float, pvpc: Optional[float], consumption: float) -> float:
    """
    Ahorro potencial si el precio del CSV supera al PVPC de ESIOS.
    Unidades: (€/kWh diff) * kWh consumidos.
    """
    if pvpc is None:
        return 0.0
    diff = max(0.0, float(energy_price) - float(pvpc))
    return round(diff * float(consumption), 4)