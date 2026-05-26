#!/usr/bin/env bash
# Despliegue de Elasticsearch en Azure mediante Elastic Cloud (recomendado)
# Requiere: Azure CLI (az) y cuenta en https://cloud.elastic.co

set -euo pipefail

RESOURCE_GROUP="${RESOURCE_GROUP:-rg-iot-climate}"
LOCATION="${LOCATION:-westeurope}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-iot-climate-es}"

echo "==> 1. Crear grupo de recursos en Azure"
az group create --name "$RESOURCE_GROUP" --location "$LOCATION"

echo "==> 2. Registrar proveedor Elastic (si no está registrado)"
az provider register --namespace Microsoft.Elastic --wait

echo "==> 3. Crear monitor Elastic (integración Azure ↔ Elastic Cloud)"
# Documentación: https://learn.microsoft.com/azure/partner-solutions/elastic/
az elastic monitor create \
  --name "$DEPLOYMENT_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku name="standard" tier="standard" \
  --user-info firstName="IoT" lastName="Pipeline" emailAddress="admin@example.com" \
  --generate-api-key

echo "==> 4. Obtener endpoint y credenciales del despliegue"
az elastic monitor show \
  --name "$DEPLOYMENT_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "{endpoint:properties.endpoints[0].endpoint,version:properties.version}" \
  -o json

echo ""
echo "==> Alternativa: Elastic Cloud manual + variables en .env"
echo "  1. Crear deployment en https://cloud.elastic.co (región Azure: West Europe)"
echo "  2. Copiar ELASTICSEARCH_CLOUD_ID y ELASTICSEARCH_API_KEY al archivo .env"
echo "  3. Levantar pipeline: docker compose -f docker-compose.azure.yml up -d"
echo ""
echo "==> Crear índice con mapping"
echo "  python scripts/setup_elasticsearch_index.py"
