<#
Instala/actualiza el servicio de Windows "ProfitSyncer" que ejecuta syncer.exe
directamente en el host (sin Docker), usando nssm.

Uso:
  1. Compilar el binario:  go build -o syncer.exe ./cmd/syncer
  2. Ejecutar este script como administrador:  .\scripts\install-windows-service.ps1
#>

$ServiceName = "ProfitSyncer"
$RepoRoot    = Split-Path -Parent $PSScriptRoot
$Nssm        = Join-Path $RepoRoot "nssm.exe"
$App         = Join-Path $RepoRoot "syncer.exe"
$LogsDir     = Join-Path $RepoRoot "logs"

if (-not (Test-Path $Nssm)) {
    throw "No se encontro nssm.exe en $Nssm"
}
if (-not (Test-Path $App)) {
    throw "No se encontro syncer.exe en $App. Compila el proyecto antes de instalar el servicio."
}
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

$existing = & $Nssm status $ServiceName 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "El servicio '$ServiceName' ya existe, actualizando configuracion..."
} else {
    & $Nssm install $ServiceName $App
}

& $Nssm set $ServiceName AppDirectory $RepoRoot
& $Nssm set $ServiceName AppStdout (Join-Path $LogsDir "out.log")
& $Nssm set $ServiceName AppStderr (Join-Path $LogsDir "err.log")
& $Nssm set $ServiceName Start SERVICE_AUTO_START
& $Nssm set $ServiceName DisplayName $ServiceName

Write-Host "Servicio '$ServiceName' configurado. Inicia con: nssm start $ServiceName"
