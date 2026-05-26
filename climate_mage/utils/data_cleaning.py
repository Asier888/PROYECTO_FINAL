"""Limpieza y filtrado de outliers con Pandas."""

from typing import Any

import pandas as pd

REQUIRED_COLUMNS = [
    "date",
    "country",
    "avg_temperature",
    "humidity",
    "co2_emission",
    "energy_consumption",
    "renewable_share",
    "urban_population",
    "industrial_activity_index",
    "energy_price",
]


def clean_sensor_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina nulos y filtra outliers físicamente imposibles."""
    if df.empty:
        return df

    working = df.copy()
    for col in REQUIRED_COLUMNS:
        if col not in working.columns:
            working[col] = None

    working = working[REQUIRED_COLUMNS]
    working = working.dropna(how="any")

    working["avg_temperature"] = pd.to_numeric(working["avg_temperature"], errors="coerce")
    working["humidity"] = pd.to_numeric(working["humidity"], errors="coerce")
    working["energy_consumption"] = pd.to_numeric(working["energy_consumption"], errors="coerce")
    working["energy_price"] = pd.to_numeric(working["energy_price"], errors="coerce")

    mask = (
        (working["avg_temperature"] <= 60)
        & (working["avg_temperature"] >= -40)
        & (working["humidity"] >= 0)
        & (working["humidity"] <= 100)
    )
    return working.loc[mask].reset_index(drop=True)


def enrich_record(record: dict[str, Any], pvpc_data: dict[str, Any], p90_consumption: float) -> dict[str, Any]:
    """Añade campos de cruce ESIOS y alertas."""
    energy_price = float(record.get("energy_price", 0))
    consumption = float(record.get("energy_consumption", 0))
    pvpc = pvpc_data.get("pvpc_esios")

    enriched = {**record, **pvpc_data}
    enriched["potential_savings"] = (
        round(max(0.0, (energy_price - pvpc) * consumption), 4) if pvpc is not None else 0.0
    )

    high_price_threshold = float(pd.Series([pvpc or 0]).quantile(0.75)) if pvpc else 0.15
    enriched["alert_high_consumption_price"] = (
        consumption >= p90_consumption and pvpc is not None and pvpc >= high_price_threshold
    )

    if enriched["alert_high_consumption_price"]:
        print(
            f"[ALERTA] Consumo P90+ ({consumption}) con PVPC alto ({pvpc}) "
            f"— país={record.get('country')} fecha={record.get('date')}"
        )

    return enriched
