<#
Local Dagster for the museum archive, on this machine (no Docker). Called by run_dagster.cmd in the repo root:

  run_dagster.cmd            start Dagster if needed (installs dagster/.venv the first time), then open the UI
  run_dagster.cmd stop       stop Dagster (the run history in dagster/home is kept); refuses while a run is in progress
                             (queued runs stay queued: they start when Dagster starts again)
  run_dagster.cmd status     up or down, the runs in progress and the queued ones
  run_dagster.cmd setup      (re)install dagster/.venv from dagster/requirements.txt

-NoBrowser: do not open the UI (used by scripts). -Force: stop even with runs in progress (they die).
Dagster runs hidden in the background until stopped (or until Windows restarts); its log is dagster/home/logs
(kept 30 days).
#>
param([ValidateSet('start', 'stop', 'status', 'setup')][string]$Command = 'start', [switch]$NoBrowser, [switch]$Force)
$ErrorActionPreference = 'Continue'

$Dir = $PSScriptRoot
$Venv = Join-Path $Dir '.venv'
$DagsterHome = Join-Path $Dir 'home'
$Port = 3080   # scrape-cars has 3070, scrape-properties 3071, askgreece 3075
$Url = "http://127.0.0.1:$Port"
$PidFile = Join-Path $DagsterHome 'dagster.pid'
$LogDir = Join-Path $DagsterHome 'logs'
$Queued = ' (QUEUED|NOT_STARTED)$'   # the end of a Get-ActiveRuns line of a run that has not started

$ArchiveVenv = Join-Path $Dir '..\archive\.venv'

function Find-Python {
    # The machine's Python, to create the venvs. Not the Store stub, not a venv of this repo.
    $found = Get-Command python -CommandType Application -All -ErrorAction SilentlyContinue |
        Where-Object { $_.Source -notlike '*WindowsApps*' -and $_.Source -notlike "$Venv*" -and
                       $_.Source -notlike "*archive\.venv*" } | Select-Object -First 1
    if (-not $found) { throw 'No Python on PATH.' }
    return $found.Source
}

function Install-Venv {
    $py = Find-Python
    if (-not (Test-Path (Join-Path $Venv 'Scripts\dagster.exe'))) {
        Write-Host "Creating dagster/.venv with $py ..."
        & $py -m venv $Venv
        if ($LASTEXITCODE) { throw 'python -m venv failed' }
    }
    & (Join-Path $Venv 'Scripts\python.exe') -m pip install -q -r (Join-Path $Dir 'requirements.txt')
    if ($LASTEXITCODE) { throw 'pip install -r dagster/requirements.txt failed' }
    # archive/.venv: the Python of the archive commands (DuckDB, requests, google-cloud-storage)
    if (-not (Test-Path (Join-Path $ArchiveVenv 'Scripts\python.exe'))) {
        Write-Host "Creating archive/.venv with $py ..."
        & $py -m venv $ArchiveVenv
        if ($LASTEXITCODE) { throw 'python -m venv (archive) failed' }
    }
    & (Join-Path $ArchiveVenv 'Scripts\python.exe') -m pip install -q -r (Join-Path $Dir '..\archive\requirements.txt')
    if ($LASTEXITCODE) { throw 'pip install -r archive/requirements.txt failed' }
    # scraper_framework.dagster, the Dagster glue shared with scrape-cars, is part of scraper-framework: the clone in
    # SCRAPER_FRAMEWORK_DIR (the environment, else the repo's .env.local), else next to this repo.
    $fw = $env:SCRAPER_FRAMEWORK_DIR
    if (-not $fw) {
        $line = Get-Content (Join-Path $Dir '..\.env.local') -ErrorAction SilentlyContinue |
            Where-Object { $_ -like 'SCRAPER_FRAMEWORK_DIR=*' } | Select-Object -First 1
        if ($line) { $fw = $line.Substring('SCRAPER_FRAMEWORK_DIR='.Length).Trim().Trim('"') }
    }
    if (-not $fw) { $fw = Join-Path $Dir '..\..\scraper-framework' }
    if (-not (Test-Path (Join-Path $fw 'scraper_framework\dagster'))) { throw "scraper-framework (with scraper_framework.dagster) not found at ${fw}: clone it and set SCRAPER_FRAMEWORK_DIR in .env.local" }
    & (Join-Path $Venv 'Scripts\python.exe') -m pip install -q -e "$fw[dagster]"
    if ($LASTEXITCODE) { throw 'pip install of scraper-framework[dagster] failed' }
}

function Test-Up {
    try { return (Invoke-WebRequest "$Url/server_info" -UseBasicParsing -TimeoutSec 3).StatusCode -eq 200 } catch { return $false }
}

function Get-DagsterProcesses {
    # This install's processes, however they were started: dagster dev, its UI and daemon, the code servers
    # and the run workers' parents. They can outlive the UI: a code server ends only once its runs have ended.
    # Only Dagster's own commands count: a program that merely runs on this venv's Python (an editor's
    # formatter or language server, for example) is not Dagster, so "dagster" must appear past the venv path.
    # Call it as @(Get-DagsterProcesses): a single process comes back unwrapped, and a CimInstance has no Count.
    try { $all = Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='dagster.exe'" -ErrorAction Stop }
    catch { throw "Cannot list the processes, so cannot tell whether Dagster still runs: $($_.Exception.Message)" }
    $all | Where-Object { $_.CommandLine -like "*$Venv*" -and $_.ProcessId -ne $PID -and
                          ("$($_.CommandLine)" -replace [regex]::Escape($Venv), '') -like '*dagster*' }
}

function Show-DagsterProcesses($list) {
    $list | ForEach-Object { Write-Host "  still running: $($_.ProcessId) $("$($_.CommandLine)".Trim() -replace '^(.{120}).+', '$1 ...')" }
}

function Stop-OwnDagster {
    # Kill the process tree this script started, and only that: after a restart Windows may have given the
    # recorded PID to another program, which then started after the PID file was written.
    if (-not (Test-Path $PidFile)) { return }
    $id = [int](Get-Content $PidFile)
    $p = Get-Process -Id $id -ErrorAction SilentlyContinue
    if ($p -and $p.ProcessName -eq 'cmd' -and $p.StartTime -le (Get-Item $PidFile).LastWriteTime) {
        & taskkill /PID $id /T /F | Out-Null
    }
    Remove-Item $PidFile -ErrorAction SilentlyContinue
}

function Get-ActiveRuns {
    # "job run_id status" per run in progress or waiting (none when Dagster is down).
    if (-not (Test-Up)) { return @() }
    $query = '{ runsOrError(filter: {statuses: [QUEUED, NOT_STARTED, STARTING, STARTED, CANCELING]}) { ... on Runs { results { runId jobName status } } } }'
    try {
        $answer = Invoke-RestMethod "$Url/graphql" -Method Post -ContentType 'application/json' -Body (@{ query = $query } | ConvertTo-Json)
        return @($answer.data.runsOrError.results | ForEach-Object { "$($_.jobName) $($_.runId) $($_.status)" })
    } catch { return @("(could not ask Dagster: $($_.Exception.Message))") }
}

switch ($Command) {
    'setup' { Install-Venv; Write-Host 'dagster/.venv is ready.' }
    'start' {
        if (-not (Test-Up) -and @(Get-DagsterProcesses).Count) {
            # Another start is under way, or the UI stopped while runs go on: never a second copy, and never
            # the sweep below (it would mark those runs failed).
            Write-Host 'Dagster processes are running: waiting for the UI ...'
            foreach ($i in 1..90) {
                if ((Test-Up) -or -not @(Get-DagsterProcesses).Count) { break }
                Start-Sleep -Seconds 2
            }
            $left = @(Get-DagsterProcesses)
            if (-not (Test-Up) -and $left.Count) {
                Show-DagsterProcesses $left
                throw 'Dagster processes are still running but the UI does not answer (a run may still be in progress). Wait for it, or: run_dagster.cmd stop -Force'
            }
        }
        if (Test-Up) {
            Write-Host "Dagster is up: $Url"
        } else {
            if (-not (Test-Path (Join-Path $Venv 'Scripts\dagster.exe'))) { Install-Venv }
            New-Item -ItemType Directory -Force $LogDir | Out-Null
            Get-ChildItem $LogDir -Filter 'dagster-*.log' | Where-Object LastWriteTime -lt (Get-Date).AddDays(-30) |
                Remove-Item -ErrorAction SilentlyContinue
            $env:DAGSTER_HOME = $DagsterHome
            # The archive commands' Python: PIPELINE_PYTHON (environment or .env.local) wins, else archive/.venv
            # (core.archive_python); nothing is set here, so a chosen one is kept.
            # Without PYTHONLEGACYWINDOWSSTDIO Dagster does not capture a step's output on Windows (the UI's
            # stdout tab stays empty); the pipeline's own commands run without it (core.pipeline_env).
            $env:PYTHONLEGACYWINDOWSSTDIO = '1'
            $env:PYTHONIOENCODING = 'utf-8'   # Dagster's own output (step logs, reports) in any language
            # Runs left STARTED by a stop mid-run or a restart: marked failed, their slots freed; old step logs
            # and the commands' temp folders removed. Safe: none of this install's processes is left.
            & (Join-Path $Venv 'Scripts\python.exe') (Join-Path $Dir 'sweep_dead_runs.py')
            if ($LASTEXITCODE) { throw 'sweep_dead_runs.py failed' }
            $log = Join-Path $LogDir ("dagster-" + (Get-Date -Format 'yyyyMMdd-HHmmss') + '.log')
            $exe = Join-Path $Venv 'Scripts\dagster.exe'
            # cmd does the redirect, so the window can stay hidden; the PID is cmd's (stop kills its tree)
            $proc = Start-Process cmd.exe -ArgumentList '/c', "`"`"$exe`" dev -w workspace.yaml -h 127.0.0.1 -p $Port > `"$log`" 2>&1`"" `
                -WorkingDirectory $Dir -WindowStyle Hidden -PassThru
            Set-Content $PidFile $proc.Id
            Write-Host "Starting Dagster (log: $log) ..."
            $up = $false
            foreach ($i in 1..90) {
                Start-Sleep -Seconds 2
                if (Test-Up) { $up = $true; break }
                if ($proc.HasExited) { break }
            }
            if (-not $up) {
                Stop-OwnDagster
                Get-Content $log -Tail 40
                throw "Dagster did not start; see $log"
            }
            Write-Host "Dagster is up: $Url"
        }
        if (-not $NoBrowser) { Start-Process $Url }
    }
    'stop' {
        $runs = Get-ActiveRuns
        $running = @($runs | Where-Object { $_ -notmatch $Queued })
        if ($running.Count -and -not $Force) {
            $running | ForEach-Object { Write-Host "  in progress: $_" }
            throw 'Not stopping while a run is in progress (it would be killed). Wait, or terminate it in the UI, or use -Force.'
        }
        $left = @(Get-DagsterProcesses)
        if (-not (Test-Up) -and $left.Count -and -not $Force) {
            Show-DagsterProcesses $left
            throw 'The UI does not answer but Dagster processes still run (a run may be in progress: it would be killed). Wait, or use -Force.'
        }
        Stop-OwnDagster
        # What the PID file does not reach: a second copy, or code servers and runs left by a UI that stopped
        Get-DagsterProcesses | ForEach-Object { & taskkill /PID $_.ProcessId /T /F 2>&1 | Out-Null }
        Start-Sleep -Seconds 2
        if (Test-Up) { throw "Dagster still answers at $Url (started some other way?): stop it there." }
        @($runs | Where-Object { $_ -match $Queued }) | ForEach-Object {
            Write-Host "  queued: $_ (it starts when Dagster starts again; terminate it in the UI, Runs > Queued, if not wanted)"
        }
        Write-Host 'Dagster stopped. Start it again with run_dagster.cmd'
    }
    'status' {
        if (Test-Up) {
            Write-Host "Dagster is up: $Url"
        } else {
            $left = @(Get-DagsterProcesses)
            if ($left.Count) {
                Write-Host "The UI does not answer, but Dagster processes still run ($($left.Count)): it is starting, or runs outlived a UI that stopped."
            } else {
                Write-Host 'Dagster is down (start: run_dagster.cmd).'
            }
        }
        $runs = Get-ActiveRuns
        foreach ($run in $runs) { if ($run -match $Queued) { Write-Host "queued: $run" } else { Write-Host "in progress: $run" } }
        if (-not $runs.Count -and (Test-Up)) { Write-Host 'No run in progress.' }
    }
}
