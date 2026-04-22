param(
    [string]$OutputPath = ".\\tools\\k6-tcp.exe",
    [string]$ExtensionRef = "github.com/NAlexandrov/xk6-tcp@latest"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command xk6 -ErrorAction SilentlyContinue)) {
    throw "xk6 not found in PATH. Install it first: go install go.k6.io/xk6/cmd/xk6@latest"
}

$outDir = Split-Path -Parent $OutputPath
if ($outDir -and -not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
}

Write-Host "[k6-build] output: $OutputPath"
Write-Host "[k6-build] extension: $ExtensionRef"

xk6 build --with $ExtensionRef --output $OutputPath
