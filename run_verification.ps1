# run_verification.ps1
# Verificación automatizada contra el stack Docker Compose (puerto 8081)
$ErrorActionPreference = "Stop"

Write-Host "================================================================"
Write-Host " VERIFICACIÓN DE DESPLIEGUE DOCKER COMPOSE — CAFETERÍA KAFKA"
Write-Host "================================================================"

# 1. Comprobar que los contenedores estén activos
$frontendContainer = docker ps --filter "name=cafeteria-frontend" --format "{{.Status}}"
$kafkaContainer = docker ps --filter "name=kafka" --format "{{.Status}}"

if (-not $frontendContainer -or -not $kafkaContainer) {
    Write-Host "Levantando servicios con docker compose..."
    docker compose up -d --build
    Start-Sleep -Seconds 5
} else {
    Write-Host "Contenedores Docker verificados:"
    Write-Host "  - kafka: $kafkaContainer"
    Write-Host "  - cafeteria-frontend: $frontendContainer"
}

# 2. Ejecutar la suite de pruebas de integración contra http://localhost:8081
$env:TEST_URL = "http://localhost:8081"
Write-Host "`nEjecutando suite test_integration.js contra http://localhost:8081..."
node test_integration.js
