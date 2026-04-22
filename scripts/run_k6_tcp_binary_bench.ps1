param(
    [string]$K6Path = ".\\tools\\k6-tcp.exe",
    [string]$Host = "127.0.0.1",
    [int]$Port = 9090,
    [int]$VUs = 32,
    [string]$Duration = "30s",
    [string]$Phases = "ping,sql,mixed",
    [string]$Sql = "SELECT * FROM case_basic_users WHERE id = 2;",
    [string]$SqlFile = ".\\scripts\\k6_tcp_queries.txt",
    [string]$OutRoot = ".\\artifacts\\k6_tcp",
    [int]$DashboardPort = -1,
    [switch]$OpenDashboard
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $K6Path)) {
    throw "k6 binary not found: $K6Path"
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outDir = Join-Path $OutRoot $timestamp
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$phaseList = $Phases.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }

Write-Host "[k6-tcp] output dir: $outDir"
Write-Host "[k6-tcp] phases: $($phaseList -join ', ')"

$phaseRows = @()

foreach ($phase in $phaseList) {
    $phaseDir = Join-Path $outDir $phase
    $htmlPath = Join-Path $phaseDir "dashboard.html"
    $summaryPath = Join-Path $phaseDir "summary.json"
    $logPath = Join-Path $phaseDir "run.log"
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    New-Item -ItemType Directory -Force -Path $phaseDir | Out-Null

    Write-Host ""
    Write-Host "=== phase: $phase ==="
    Write-Host "start: $(Get-Date -Format o)"

    $env:K6_TCP_HOST = $Host
    $env:K6_TCP_PORT = "$Port"
    $env:K6_TCP_MODE = $phase
    $env:K6_TCP_SQL = $Sql
    $env:K6_TCP_SQL_FILE = $SqlFile
    $env:K6_VUS = "$VUs"
    $env:K6_DURATION = $Duration
    $env:K6_WEB_DASHBOARD = "true"
    $env:K6_WEB_DASHBOARD_EXPORT = $htmlPath
    $env:K6_WEB_DASHBOARD_PORT = "$DashboardPort"
    $env:K6_WEB_DASHBOARD_OPEN = $(if ($OpenDashboard) { "true" } else { "false" })

    & $K6Path run --summary-export $summaryPath .\scripts\k6_tcp_binary_bench.js 2>&1 |
        Tee-Object -FilePath $logPath

    $stopwatch.Stop()

    $phaseRows += [pscustomobject]@{
        phase      = $phase
        elapsed_ms = [math]::Round($stopwatch.Elapsed.TotalMilliseconds, 2)
        summary    = $summaryPath
        dashboard  = $htmlPath
        log        = $logPath
    }

    Write-Host "end: $(Get-Date -Format o)"
    Write-Host "elapsed_ms: $([math]::Round($stopwatch.Elapsed.TotalMilliseconds, 2))"
    Write-Host "dashboard: $htmlPath"
}

$phaseRows | ConvertTo-Json -Depth 3 | Set-Content -Path (Join-Path $outDir "phases.json")
$phaseRows | Format-Table -AutoSize | Out-String | Set-Content -Path (Join-Path $outDir "phases.txt")
$phaseRows | Format-Table -AutoSize
