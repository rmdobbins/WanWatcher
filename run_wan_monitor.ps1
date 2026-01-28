$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvPython = Join-Path $root "venv\Scripts\python.exe"
if (Test-Path $venvPython) {
    $python = $venvPython
} else {
    $python = "python"
}

$tracerouteArgs = @(
    "net_traceroute_logger.py",
    "--db", "netstats.db",
    "--target", "72.14.198.150",
    "--interval", "10",
    "--keep-days", "7"
)

$loggerArgs = @(
    "net_logger.py",
    "--db", "netstats.db",
    "--interval", "10",
    "--pernic",
    "--ping-count", "5",
    "--throughput-every", "6",
    "--keep-days", "7",
    "--iperf", "iperf.he.net",
    "--iperf-duration", "8"
)

$tracerouteProc = Start-Process -FilePath $python -ArgumentList $tracerouteArgs -PassThru -NoNewWindow
$loggerProc = Start-Process -FilePath $python -ArgumentList $loggerArgs -PassThru -NoNewWindow

Write-Host "Started traceroute logger (PID $($tracerouteProc.Id))."
Write-Host "Started net logger (PID $($loggerProc.Id))."
Write-Host "Press Ctrl+C to stop both."

try {
    Wait-Process -Id $tracerouteProc.Id, $loggerProc.Id
} finally {
    foreach ($proc in @($tracerouteProc, $loggerProc)) {
        if ($proc -and -not $proc.HasExited) {
            Stop-Process -Id $proc.Id
        }
    }
}
