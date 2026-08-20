param(
    [string]$Since
)

if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    Write-Error @"
gcloud not found. Install the Google Cloud SDK:
  winget:    winget install Google.CloudSDK
  Installer: https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe
"@
    exit 1
}

$downloads  = "$env:USERPROFILE\Downloads"
$stateFile  = "$downloads\osv\last_run.txt"
$date       = Get-Date -Format "yyyyMMdd"
$deltaRoot  = "$downloads\osv\delta"
$inbox      = "dbfs:/Volumes/utility/file_router/inbox"

function Get-SinceDate {
    param([string]$SinceArg)

    if ($SinceArg) {
        if ($SinceArg -match '^(\d+)\s*(hour|hours|day|days|week|weeks|month|months|year|years)$') {
            $n    = [int]$Matches[1]
            $unit = $Matches[2].ToLower()
            $now  = (Get-Date).ToUniversalTime()
            switch -Regex ($unit) {
                'hour'  { return $now.AddHours(-$n) }
                'day'   { return $now.AddDays(-$n) }
                'week'  { return $now.AddDays(-7 * $n) }
                'month' { return $now.AddMonths(-$n) }
                'year'  { return $now.AddYears(-$n) }
            }
        }
        try {
            return ([datetime]::Parse($SinceArg)).ToUniversalTime()
        } catch {
            Write-Error "Could not parse -Since value '$SinceArg'. Use a relative form like '35days', '1month', '2weeks', or an absolute date."
            exit 1
        }
    }

    if (Test-Path $stateFile) {
        return [datetime]::Parse((Get-Content $stateFile -Raw).Trim()).ToUniversalTime()
    }

    Write-Warning "No prior run recorded and -Since not specified; defaulting to last 24 hours."
    return (Get-Date).ToUniversalTime().AddDays(-1)
}

$sinceDate = Get-SinceDate -SinceArg $Since
$since     = $sinceDate.ToString("yyyy-MM-ddTHH:mm:ss")

Write-Host "Using since = $since UTC"

$ecosystems = @("PyPI", "CRAN")

if (Test-Path $deltaRoot) { Remove-Item $deltaRoot -Recurse -Force }

$allSucceeded = $true

foreach ($eco in $ecosystems) {
    $manifestDir = "$downloads\osv\manifest\$eco"
    $deltaDir    = "$downloads\osv\delta\$date\osv_bronze_landing_$eco"

    # gsutil rsync always tries to stamp the local mtime after each download,
    # which throws [Errno 22] on Windows for some objects. Plain cp just writes
    # the file with the current local time instead, so use cp exclusively —
    # both for the manifest and for fetching the delta files below.
    New-Item -ItemType Directory -Path $manifestDir -Force | Out-Null
    $manifestFile = "$manifestDir\modified_id.csv"
    if (Test-Path $manifestFile) { Remove-Item $manifestFile -Force }
    gsutil cp "gs://osv-vulnerabilities/$eco/modified_id.csv" $manifestFile
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to fetch current modified_id.csv for $eco"
        $allSucceeded = $false
        continue
    }

    $deltaIds = Get-Content $manifestFile |
        Where-Object { $_.Split(',')[0] -gt $since } |
        ForEach-Object { $_.Split(',')[1] }

    Write-Host "Found $($deltaIds.Count) changed files for $eco"

    if ($deltaIds.Count -eq 0) {
        Write-Host "Nothing to upload for $eco"
        continue
    }

    New-Item -ItemType Directory -Path $deltaDir -Force | Out-Null

    # gsutil cp -I (reading the source list from stdin) silently stops after
    # only 2 files on this machine, regardless of -m or how stdin is fed to it.
    # Passing URLs directly as arguments is the reliable code path, so batch
    # them to stay well under the Windows command-line length limit.
    $batchSize  = 100
    $urls       = $deltaIds | ForEach-Object { "gs://osv-vulnerabilities/$eco/$_.json" }
    $batchCount = [math]::Ceiling($urls.Count / $batchSize)
    $fetchOk    = $true

    for ($i = 0; $i -lt $batchCount; $i++) {
        $batch = $urls[($i * $batchSize)..([math]::Min($i * $batchSize + $batchSize, $urls.Count) - 1)]

        $maxAttempts  = 2
        $attempt      = 0
        $batchFetchOk = $false
        do {
            $attempt++
            Write-Host "Fetching batch $($i + 1)/$batchCount for $eco (attempt $attempt)..."
            gsutil -m cp @batch $deltaDir
            $batchFetchOk = ($LASTEXITCODE -eq 0)
            if (-not $batchFetchOk -and $attempt -lt $maxAttempts) {
                Write-Warning "Batch $($i + 1)/$batchCount failed for $eco (attempt $attempt); retrying..."
            }
        } while (-not $batchFetchOk -and $attempt -lt $maxAttempts)

        if (-not $batchFetchOk) {
            Write-Warning "Batch $($i + 1)/$batchCount could not be fetched for $eco after $attempt attempt(s)."
            $fetchOk = $false
        }
    }

    if (-not $fetchOk) {
        Write-Warning "Could not fetch all delta files for $eco. Uploading whatever was fetched; last-run state will not advance so this window is retried next time."
        $allSucceeded = $false
    }

    $total = (Get-ChildItem $deltaDir -File).Count
    if ($total -eq 0) {
        Write-Host "Nothing to upload for $eco"
        continue
    }

    $dest     = "$inbox/osv_bronze_landing_$eco"
    $uploaded = 0

    Write-Progress -Activity $eco -Status "Uploading 0 / $total files..." -PercentComplete 0
    & databricks fs cp $deltaDir $dest -r 2>&1 | ForEach-Object {
        $uploaded++
        $pct = [int](($uploaded / $total) * 100)
        Write-Progress -Activity $eco -Status "Uploading $uploaded / $total files..." -PercentComplete $pct
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "databricks fs cp failed for $eco"
        $allSucceeded = $false
    }
    Write-Progress -Activity $eco -Completed
    Write-Host "Done: $eco"
}

if ($allSucceeded) {
    (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss") | Set-Content $stateFile
    Write-Host "Recorded last run time to $stateFile"
} else {
    Write-Warning "One or more ecosystems failed; not updating last-run state so the next run retries this window."
}
