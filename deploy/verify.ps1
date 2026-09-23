$ErrorActionPreference = 'Stop'
Push-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
try {
    docker compose up --build -d --wait
    if ($LASTEXITCODE -ne 0) { throw 'Compose no pudo arrancar o alcanzar readiness.' }
    python deploy/demo.py
    if ($LASTEXITCODE -ne 0) { throw 'La demo no paso.' }
    python deploy/showcase.py
    if ($LASTEXITCODE -ne 0) { throw 'El showcase no paso.' }
    Write-Host 'Scheduler verificado; los servicios siguen activos en http://127.0.0.1:8080'
} finally {
    Pop-Location
}
