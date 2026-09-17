# Soak-test the OrderFlow demo against the emulator.
#
# Runs Emulator + OrderApi + FulfillmentWorker headless (logs to files), then loops the
# chosen scenario for the requested duration. After every scenario run it snapshots the
# OrderApi stats, the emulator's queue counters and process diagnostics, and counts the
# failure signatures in the three logs. Everything lands in one output directory:
#
#   soak-out/<timestamp>/
#     emulator.log, orderapi.log, worker.log     raw process logs
#     cycles.csv                                 one row per scenario run
#     summary.md                                 totals + per-cycle table
#     amqp-frames-*.log                          last frames before a connection closed
#                                                with an error (emulator TRACE_AMQP=ring)
#
# Usage:
#   .\soak.ps1                          # black-friday, 3 hours
#   .\soak.ps1 -Hours 1 -Scenario steady-state
#   .\soak.ps1 -Cycles 2                # a fixed number of runs instead of a duration
#
# Requires ports 5672 / 5300 / 15672 / 5200 to be free (stop run.ps1 windows first).

param(
    [double]$Hours = 3,
    [int]$Cycles = 0,
    [string]$Scenario = "black-friday",
    [ValidateSet("Debug", "Information", "Warning", "Error")]
    [string]$LogLevel = "Warning",
    [string]$OutDir = ""
)

$ErrorActionPreference = "Stop"
$scriptDir = $PSScriptRoot
$repoRoot  = (Resolve-Path "$scriptDir\..\..").Path
if (-not $OutDir) { $OutDir = Join-Path $scriptDir "soak-out\$(Get-Date -Format 'yyyyMMdd-HHmmss')" }
New-Item -ItemType Directory -Force $OutDir | Out-Null
$OutDir = (Resolve-Path $OutDir).Path

$connStr = "Endpoint=sb://localhost:5672;SharedAccessKeyName=OrderFlowDemo;SharedAccessKey=emulator;UseDevelopmentEmulator=true"
$orderApi = "http://localhost:5200"
$emuDash  = "http://localhost:15672"

function Log($msg) { Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $msg" }

# ── Build ────────────────────────────────────────────────────────────────────
Log "Building projects..."
dotnet build "$repoRoot\src\AlmostServiceBus.Host" --nologo -v quiet
dotnet build "$scriptDir\OrderFlowDemo.OrderApi" --nologo -v quiet
dotnet build "$scriptDir\OrderFlowDemo.FulfillmentWorker" --nologo -v quiet
if ($LASTEXITCODE -ne 0) { throw "build failed" }

# ── Start processes ──────────────────────────────────────────────────────────
function Start-Logged($title, $project, $envVars) {
    # Redirect at the OS level via cmd.exe. Capturing stdout through PowerShell's
    # OutputDataReceived events silently drops lines under load (~35% missing in a
    # black-friday run), which would corrupt the failure counts this script exists for.
    $log = Join-Path $OutDir "$title.log"
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "cmd.exe"
    $psi.Arguments = "/c dotnet run --no-build --project `"$project`" > `"$log`" 2>&1"
    $psi.WorkingDirectory = $project
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    foreach ($k in $envVars.Keys) { $psi.Environment[$k] = $envVars[$k] }
    $p = [System.Diagnostics.Process]::Start($psi)
    Log "Started $title (pid $($p.Id)) -> $log"
    return $p
}

$common = @{
    "ConnectionStrings__servicebus" = $connStr
    "Serilog__MinimumLevel"         = $LogLevel
    "Logging__LogLevel__Default"    = $LogLevel
    "ASPNETCORE_ENVIRONMENT"        = "Production"   # serve the built dashboard, no Vite dev server
}

$emulator = Start-Logged "emulator" "$repoRoot\src\AlmostServiceBus.Host" @{
    "Logging__LogLevel__Default"               = $LogLevel
    "Logging__LogLevel__AlmostServiceBus.Amqp" = "Warning"
    "TRACE_AMQP"                               = "ring"
    "AMQP_TRACE_DIR"                           = $OutDir
    "ASPNETCORE_ENVIRONMENT"                   = "Production"
}
Start-Sleep -Seconds 4
$api    = Start-Logged "orderapi" "$scriptDir\OrderFlowDemo.OrderApi" ($common + @{ "ASPNETCORE_URLS" = $orderApi })
$worker = Start-Logged "worker"   "$scriptDir\OrderFlowDemo.FulfillmentWorker" $common
$procs = @($emulator, $api, $worker)

function Stop-All {
    # The app processes are grandchildren (cmd -> dotnet run -> app); kill by name/command line.
    Get-Process -Name "AlmostServiceBus.Host","OrderFlowDemo.OrderApi","OrderFlowDemo.FulfillmentWorker" -ErrorAction SilentlyContinue |
        Stop-Process -Force -ErrorAction SilentlyContinue
    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -eq "dotnet.exe" -and $_.CommandLine -match "AlmostServiceBus\.Host|OrderFlowDemo\." } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    foreach ($p in $procs) {
        try { if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } } catch {}
    }
}

function Get-Json($url) {
    try { return Invoke-RestMethod -Uri $url -TimeoutSec 10 } catch { return $null }
}

# Wait for the OrderApi to answer.
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
    if (Get-Json "$orderApi/api/dashboard/stats") { $ready = $true; break }
    Start-Sleep -Seconds 2
}
if (-not $ready) { Stop-All; throw "OrderApi did not come up — see $OutDir\orderapi.log" }
Log "All processes up. Output: $OutDir"

# ── Failure signatures ───────────────────────────────────────────────────────
$signatures = [ordered]@{
    "emu_conn_closed_error" = @("emulator", "AMQP connection closed with error")
    "emu_not_found"         = @("emulator", "amqp:not-found")
    "emu_illegal_state"     = @("emulator", "illegal-state")
    "emu_ondetach"          = @("emulator", "OnDetach")
    "emu_unhandled"         = @("emulator", "Unhandled exception")
    "api_saga_not_accepted" = @("orderapi", "Not accepted in state")
    "api_channel_not_found" = @("orderapi", "cannot be found in connection")
    "api_socket"            = @("orderapi", "SocketException")
    "api_lock_lost"         = @("orderapi", "LockLost")
    "api_error_other"       = @("orderapi", " ERR] ", "R-FAULT")
    "wrk_channel_not_found" = @("worker",   "cannot be found in connection")
    "wrk_socket"            = @("worker",   "SocketException")
    "wrk_session_lock_lost" = @("worker",   "SessionLockLost")
    "wrk_lock_lost"         = @("worker",   "MessageLockLost")
    "wrk_error_other"       = @("worker",   " ERR] ", "R-FAULT")
    "wrk_consumer_fault"    = @("worker",   "R-FAULT")           # expected: scenario's ShippingFailureProbability
}

function Count-Signatures {
    $counts = [ordered]@{}
    foreach ($name in $signatures.Keys) {
        $file, $pattern, $exclude = $signatures[$name]
        $path = Join-Path $OutDir "$file.log"
        $n = 0
        if (Test-Path $path) {
            $hits = Select-String -Path $path -SimpleMatch -Pattern $pattern -ErrorAction SilentlyContinue
            if ($exclude) { $hits = $hits | Where-Object { $_.Line -notlike "*$exclude*" } }
            $n = ($hits | Measure-Object).Count
        }
        $counts[$name] = $n
    }
    return $counts
}

# ── Cycle loop ───────────────────────────────────────────────────────────────
$csv = Join-Path $OutDir "cycles.csv"
$queueNames = @("PickOrder","ReserveInventory","ProcessPayment","logistics-dispatch","OrderShipped","GenerateInvoice","OrderState","logistics-dispatch_error","OrderState_error")
$header = @("cycle","time","elapsed_min","total","completed","failed","inFlight","workingSetMB","threads") +
          ($queueNames | ForEach-Object { "$($_)_active","$($_)_dlq","$($_)_total","$($_)_consumed" }) +
          $signatures.Keys + @("frame_dumps")
($header -join ",") | Set-Content $csv

$start = Get-Date
$cycle = 0
$rows = @()
$prevCounts = Count-Signatures

try {
    while ($true) {
        $elapsed = (Get-Date) - $start
        if ($Cycles -gt 0) { if ($cycle -ge $Cycles) { break } }
        elseif ($elapsed.TotalHours -ge $Hours) { break }
        $cycle++

        Log "Cycle $cycle — starting scenario '$Scenario' (elapsed $([int]$elapsed.TotalMinutes) min)"
        Invoke-RestMethod -Method Post -Uri "$orderApi/api/scenarios/$Scenario/start" | Out-Null

        # Wait for the scenario to finish generating orders.
        do {
            Start-Sleep -Seconds 5
            $active = Get-Json "$orderApi/api/scenarios/active"
            if ($null -eq $active) { Log "  OrderApi not answering"; }
        } while ($null -ne $active -and $null -ne $active.scenario)

        # Drain: wait until in-flight stops changing (or 3 minutes).
        $lastInFlight = -1; $stable = 0; $waited = 0
        do {
            Start-Sleep -Seconds 5; $waited += 5
            $stats = Get-Json "$orderApi/api/dashboard/stats"
            $inFlight = if ($stats) { [int]$stats.inFlight } else { -1 }
            if ($inFlight -eq $lastInFlight) { $stable += 5 } else { $stable = 0 }
            $lastInFlight = $inFlight
        } while ($stable -lt 30 -and $waited -lt 180)

        $stats    = Get-Json "$orderApi/api/dashboard/stats"
        $entities = Get-Json "$emuDash/api/dashboard/namespaces/OrderFlowDemo/entities"
        $diag     = Get-Json "$emuDash/api/dashboard/diagnostics"
        $counts   = Count-Signatures
        $dumps    = (Get-ChildItem $OutDir -Filter "amqp-frames-*.log" -ErrorAction SilentlyContinue | Measure-Object).Count

        $row = [ordered]@{
            cycle = $cycle; time = (Get-Date -Format "HH:mm:ss"); elapsed_min = [int]((Get-Date) - $start).TotalMinutes
            total = $stats.total; completed = $stats.completed; failed = $stats.failed; inFlight = $stats.inFlight
            workingSetMB = $diag.process.workingSetMB; threads = $diag.threadPool.threadCount
        }
        foreach ($q in $queueNames) {
            $e = $entities.queues | Where-Object { $_.name -eq $q } | Select-Object -First 1
            $row["$($q)_active"]   = if ($e) { $e.messageCount } else { "" }
            $row["$($q)_dlq"]      = if ($e) { $e.deadLetterCount } else { "" }
            $row["$($q)_total"]    = if ($e) { $e.totalMessageCount } else { "" }
            $row["$($q)_consumed"] = if ($e) { $e.consumedCount } else { "" }
        }
        foreach ($k in $counts.Keys) { $row[$k] = $counts[$k] }
        $row["frame_dumps"] = $dumps
        $rows += $row
        (($header | ForEach-Object { $row[$_] }) -join ",") | Add-Content $csv

        $delta = ($signatures.Keys | Where-Object { $counts[$_] -ne $prevCounts[$_] } | ForEach-Object { "$_ +$($counts[$_] - $prevCounts[$_])" }) -join ", "
        $prevCounts = $counts
        $ld = $entities.queues | Where-Object { $_.name -eq "logistics-dispatch" }
        Log ("  done: orders={0} completed={1} failed={2} inFlight={3} | logistics-dispatch active={4} consumed={5}/{6} | dumps={7} | {8}" -f `
            $stats.total, $stats.completed, $stats.failed, $stats.inFlight, $ld.messageCount, $ld.consumedCount, $ld.totalMessageCount, $dumps, ($(if ($delta) { $delta } else { "no new failure signatures" })))

        foreach ($p in $procs) {
            if ($p.HasExited) { throw "process $($p.Id) exited with code $($p.ExitCode) — see logs in $OutDir" }
        }
    }
}
finally {
    Log "Stopping processes..."
    Stop-All
    Start-Sleep -Seconds 2

    # ── Summary ──
    $final = Count-Signatures
    $last = $rows | Select-Object -Last 1
    $md = @()
    $md += "# Soak summary — $Scenario, $cycle cycle(s), $([int]((Get-Date) - $start).TotalMinutes) min"
    $md += ""
    $md += "Output: ``$OutDir``"
    $md += ""
    if ($last) {
        $md += "## Final OrderApi stats"
        $md += ""
        $md += "| total | completed | failed | inFlight |"
        $md += "|---|---|---|---|"
        $md += "| $($last.total) | $($last.completed) | $($last.failed) | $($last.inFlight) |"
        $md += ""
    }
    $md += "## Failure signatures (whole run)"
    $md += ""
    $md += "| signature | count |"
    $md += "|---|---|"
    foreach ($k in $final.Keys) { $md += "| $k | $($final[$k]) |" }
    $md += "| frame dumps (emulator connection closed with error) | $((Get-ChildItem $OutDir -Filter 'amqp-frames-*.log' -ErrorAction SilentlyContinue | Measure-Object).Count) |"
    $md += ""
    if ($rows.Count -gt 0) {
        $md += "## Per cycle"
        $md += ""
        $cols = @("cycle","time","total","completed","failed","inFlight","workingSetMB","logistics-dispatch_active","logistics-dispatch_consumed","logistics-dispatch_total","OrderState_error_active","emu_conn_closed_error","api_saga_not_accepted","wrk_channel_not_found","frame_dumps")
        $md += "| " + ($cols -join " | ") + " |"
        $md += "|" + (($cols | ForEach-Object { "---" }) -join "|") + "|"
        foreach ($r in $rows) { $md += "| " + (($cols | ForEach-Object { $r[$_] }) -join " | ") + " |" }
    }
    $md -join "`n" | Set-Content (Join-Path $OutDir "summary.md")
    Log "Summary written to $OutDir\summary.md"
}
