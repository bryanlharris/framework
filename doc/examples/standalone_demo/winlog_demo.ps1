<#
Standalone demo script: pull recent Windows Security log events from this PC
and land them as a single JSON file, ready to load into a Databricks Unity
Catalog volume — either automatically via the CLI, or by hand.

No file_router, no Auto Loader, no streaming, no new catalog/schema needed —
point -VolumePath at any volume you already have write access to.

Usage:
    databricks configure   # once, if the CLI isn't already set up
    .\winlog_demo.ps1 -VolumePath "/Volumes/<catalog>/<schema>/<volume>"

    # Or, if the CLI isn't set up (e.g. a work laptop): produce the local
    # file only, then drag-and-drop it into the volume via the Databricks UI.
    .\winlog_demo.ps1 -SkipUpload
#>
param(
    [string]$VolumePath,   # e.g. /Volumes/winlog/bronze/landing — required unless -SkipUpload
    [int]$MaxEvents = 200,
    [switch]$SkipUpload
)

if (-not $SkipUpload -and -not $VolumePath) {
    Write-Error "-VolumePath is required unless -SkipUpload is set."
    exit 1
}

# Get-WinEvent on the Security log requires an elevated session.
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error "This script reads the Security event log, which requires an elevated (Run as Administrator) PowerShell session."
    exit 1
}

$downloads = "$env:USERPROFILE\Downloads"
$localFile = "$downloads\winlog_demo_security_log.json"
# Named so it does NOT match the live pipeline's "security*.json" Auto
# Loader glob (settings/bronze/winlog.security.json) if -VolumePath points
# at the same landing volume — keeps this scratch file out of the real table.
if (-not $SkipUpload) {
    $dest = "dbfs:$VolumePath/winlog_demo_security_log.json"
}

Write-Host "Reading up to $MaxEvents events from the Security log..."

try {
    $events = Get-WinEvent -LogName Security -MaxEvents $MaxEvents -ErrorAction Stop
} catch {
    Write-Error "Failed to read Security log: $_"
    exit 1
}

if (-not $events -or $events.Count -eq 0) {
    Write-Warning "No events found. Nothing to upload."
    exit 0
}

Write-Host "Fetched $($events.Count) events."

# One record per line (NDJSON) — spark.read.json() reads this natively in
# the notebook, no format options needed.
$records = $events | ForEach-Object {
    [PSCustomObject]@{
        record_id          = $_.RecordId
        time_created        = $_.TimeCreated.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        event_id            = $_.Id
        level               = $_.LevelDisplayName
        provider_name       = $_.ProviderName
        task_display_name   = $_.TaskDisplayName
        keywords            = ($_.KeywordsDisplayNames -join ",")
        machine_name        = $_.MachineName
        user_id             = if ($_.UserId) { $_.UserId.Value } else { $null }
        message             = $_.Message
    }
}

if (Test-Path $localFile) { Remove-Item $localFile -Force }

$records |
    ForEach-Object { $_ | ConvertTo-Json -Compress -Depth 5 } |
    Set-Content -Path $localFile -Encoding utf8

if ($SkipUpload) {
    Write-Host "Done: wrote $($records.Count) Security log events to $localFile"
    Write-Host "Drag-and-drop this file into your target volume via the Databricks UI, or upload it with 'databricks fs cp' once the CLI is configured."
    exit 0
}

Write-Host "Uploading to $dest..."
& databricks fs cp $localFile $dest --overwrite
if ($LASTEXITCODE -ne 0) {
    Write-Error "databricks fs cp failed"
    exit 1
}

Remove-Item $localFile

Write-Host "Done: uploaded $($records.Count) Security log events to $VolumePath/winlog_demo_security_log.json"
