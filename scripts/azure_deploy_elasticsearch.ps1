# Despliegue Elasticsearch en Azure (Elastic Cloud / Microsoft.Elastic)
# Ejecutar: .\scripts\azure_deploy_elasticsearch.ps1

$ErrorActionPreference = "Stop"

$ResourceGroup = if ($env:RESOURCE_GROUP) { $env:RESOURCE_GROUP } else { "rg-iot-climate" }
$Location = if ($env:LOCATION) { $env:LOCATION } else { "westeurope" }
$DeploymentName = if ($env:DEPLOYMENT_NAME) { $env:DEPLOYMENT_NAME } else { "iot-climate-es" }

Write-Host "==> Crear grupo de recursos"
az group create --name $ResourceGroup --location $Location

Write-Host "==> Registrar proveedor Microsoft.Elastic"
az provider register --namespace Microsoft.Elastic --wait

Write-Host "==> Crear monitor Elastic en Azure"
az elastic monitor create `
  --name $DeploymentName `
  --resource-group $ResourceGroup `
  --location $Location `
  --sku name="standard" tier="standard" `
  --user-info firstName="IoT" lastName="Pipeline" emailAddress="admin@example.com" `
  --generate-api-key

Write-Host "==> Detalles del despliegue"
az elastic monitor show `
  --name $DeploymentName `
  --resource-group $ResourceGroup `
  -o json

Write-Host @"

Siguiente paso:
  1. Copiar ELASTICSEARCH_CLOUD_ID y ELASTICSEARCH_API_KEY a .env
  2. docker compose -f docker-compose.azure.yml --env-file .env up -d
  3. python scripts/setup_elasticsearch_index.py

"@
