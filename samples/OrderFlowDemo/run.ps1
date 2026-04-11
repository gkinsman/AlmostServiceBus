# Run the OrderFlow Demo (Emulator + OrderApi + FulfillmentWorker)
# Each process gets its own terminal window for clean log output.
# Close any terminal window to stop that process.
#
# Usage:
#   .\run.ps1              # Warning-level logs (default)
#   .\run.ps1 -LogLevel Information   # More verbose

param(
    [ValidateSet("Debug", "Information", "Warning", "Error")]
    [string]$LogLevel = "Warning"
)

$ErrorActionPreference = "Stop"

$scriptDir = $PSScriptRoot
$repoRoot  = Resolve-Path "$scriptDir\..\.."

$connStr = "Endpoint=sb://localhost:5672;SharedAccessKeyName=OrderFlowDemo;SharedAccessKey=emulator"

# Build the Vue dashboard so changes are picked up
Write-Host "Building Vue dashboard..." -ForegroundColor Cyan
npm ci --prefix "$scriptDir\OrderFlowDemo.OrderApi\ClientApp"
npm run build --prefix "$scriptDir\OrderFlowDemo.OrderApi\ClientApp"

# Build all projects upfront to avoid concurrent build conflicts
Write-Host "Building projects..." -ForegroundColor Cyan
dotnet build "$repoRoot\src\AlmostServiceBus.Host" --nologo -v quiet
dotnet build "$scriptDir\OrderFlowDemo.OrderApi" --nologo -v quiet
dotnet build "$scriptDir\OrderFlowDemo.FulfillmentWorker" --nologo -v quiet

Write-Host ""
Write-Host "Starting processes in separate terminals (LogLevel=$LogLevel)..." -ForegroundColor Green

# Environment block shared by the demo apps (Serilog reads from config, emulator uses MS logging)
$envBlock = @(
    "set ConnectionStrings__servicebus=$connStr",
    "set Serilog__MinimumLevel=$LogLevel",
    "set Logging__LogLevel__Default=$LogLevel"
) -join " && "

# Emulator
Start-Process -FilePath "cmd.exe" -ArgumentList "/k", "title Emulator && set Logging__LogLevel__Default=$LogLevel && dotnet run --no-build --project `"$repoRoot\src\AlmostServiceBus.Host`""

# OrderApi
Start-Process -FilePath "cmd.exe" -ArgumentList "/k", "title OrderApi && $envBlock && dotnet run --no-build --project `"$scriptDir\OrderFlowDemo.OrderApi`""

# FulfillmentWorker
Start-Process -FilePath "cmd.exe" -ArgumentList "/k", "title FulfillmentWorker && $envBlock && dotnet run --no-build --project `"$scriptDir\OrderFlowDemo.FulfillmentWorker`""

Write-Host ""
Write-Host "OrderFlow Demo is running in separate windows:" -ForegroundColor Green
Write-Host "  Emulator:          localhost:5672" -ForegroundColor Cyan
Write-Host "  OrderApi:          http://localhost:5200" -ForegroundColor Cyan
Write-Host "  FulfillmentWorker: running" -ForegroundColor Cyan
Write-Host ""
Write-Host "Close any terminal window to stop that process." -ForegroundColor Yellow
