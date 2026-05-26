# Dashboard Kibana — Visualizaciones

Índice de datos: `climate_energy_iot` (crear Data View con campo de tiempo `date`).

## 1. Serie temporal: consumo energético vs. PVPC ESIOS

**Tipo:** Line chart (Lens)

| Configuración | Valor |
|---------------|-------|
| Eje X | `date` (Date histogram, intervalo: auto/día) |
| Serie A | `average(energy_consumption)` — etiqueta: Consumo (kWh) |
| Serie B | `average(pvpc_esios)` — etiqueta: PVPC ESIOS (€/kWh) |
| Filtro opcional | `country: "Spain"` |

**KQL alternativo en Discover:**
```
country: "Spain" and pvpc_esios: *
```

## 2. Mapa de calor: emisiones CO₂ por actividad industrial

**Tipo:** Heat map (Lens)

| Configuración | Valor |
|---------------|-------|
| Eje X | `industrial_activity_index` (histogram, 10 bins) |
| Eje Y | `country` (Top 10) |
| Color | `average(co2_emission)` |
| Métrica | Promedio de `co2_emission` |

Interpretación: celdas más intensas = mayor emisión media para un nivel de actividad industrial.

## 3. Sistema de alertas: consumo P90 + PVPC alto

### Visualización (Metric / Data table)

| Configuración | Valor |
|---------------|-------|
| Métrica | `count()` |
| Filtro | `alert_high_consumption_price: true` |

### Regla de alerta (Stack Management → Rules)

```yaml
Nombre: Alto consumo con PVPC elevado
Tipo: Elasticsearch query
Índice: climate_energy_iot
Consulta:
  alert_high_consumption_price: true
Condición: count() > 0 en los últimos 15 min
Acción: Index into .alerts-iot-climate o enviar email/webhook
Mensaje: "Consumo supera P90 y PVPC ESIOS es alto"
```

El bloque Transformer ya genera logs `[ALERTA]` en consola cuando se cumple la condición.

## Creación rápida del dashboard

1. Abrir Kibana: http://localhost:5601 (local) o URL de Elastic Cloud (Azure).
2. **Stack Management → Data Views → Create** → índice `climate_energy_iot`, time field `date`.
3. **Analytics → Dashboard → Create** → añadir las 3 visualizaciones anteriores.
4. Guardar como `IoT Climate Energy Dashboard`.
