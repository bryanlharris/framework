param(
    [int]$MaxEvents = 500
)

# Get-WinEvent on the Security log requires an elevated session.
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error "This script reads the Security event log, which requires an elevated (Run as Administrator) PowerShell session."
    exit 1
}

$downloads = "$env:USERPROFILE\Downloads"
$inbox     = "dbfs:/Volumes/utility/file_router/inbox"
$localFile = "$downloads\winlog_security.json"
$dest      = "$inbox/winlog_bronze_landing_security.json"

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

# Flatten to one record per line (NDJSON) so it lands the same shape the
# bronze settings (multiLine: false) expect. Timestamps are formatted as
# UTC ISO 8601 so Spark's default to_timestamp() parses them at silver.
$records = $events | ForEach-Object {
    [PSCustomObject]@{
        record_id         = $_.RecordId
        time_created       = $_.TimeCreated.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        event_id           = $_.Id
        level              = $_.LevelDisplayName
        provider_name      = $_.ProviderName
        task_display_name  = $_.TaskDisplayName
        keywords           = ($_.KeywordsDisplayNames -join ",")
        machine_name       = $_.MachineName
        user_id            = if ($_.UserId) { $_.UserId.Value } else { $null }
        message            = $_.Message
    }
}

if (Test-Path $localFile) { Remove-Item $localFile -Force }

$records |
    ForEach-Object { $_ | ConvertTo-Json -Compress -Depth 5 } |
    Set-Content -Path $localFile -Encoding utf8

Write-Host "Uploading to $dest..."
& databricks fs cp $localFile $dest
if ($LASTEXITCODE -ne 0) {
    Write-Error "databricks fs cp failed"
    exit 1
}

Remove-Item $localFile

Write-Host "Done: uploaded $($records.Count) Security log events."
