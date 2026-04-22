if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    Write-Error @"
gcloud not found. Install the Google Cloud SDK:
  winget:    winget install Google.CloudSDK
  Installer: https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe
"@
    exit 1
}

$downloads  = "$env:USERPROFILE\Downloads"
$since      = (Get-Date).AddDays(-1).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")
$date       = Get-Date -Format "yyyyMMdd"
$deltaRoot  = "$downloads\osv\delta"
$inbox      = "dbfs:/Volumes/utility/file_router/inbox"

$ecosystems = @("PyPI", "CRAN")

if (Test-Path $deltaRoot) { Remove-Item $deltaRoot -Recurse -Force }

foreach ($eco in $ecosystems) {
    $fullDir  = "$downloads\osv\full\osv_bronze_landing_$eco"
    $deltaDir = "$downloads\osv\delta\$date\osv_bronze_landing_$eco"

    Write-Host "Syncing $eco..."
    gsutil -m rsync -r -c -x "all\.zip" "gs://osv-vulnerabilities/$eco/" $fullDir

    $deltaIds = Get-Content "$fullDir\modified_id.csv" |
        Where-Object { $_.Split(',')[0] -gt $since } |
        ForEach-Object { $_.Split(',')[1] }

    Write-Host "Found $($deltaIds.Count) changed files for $eco"

    New-Item -ItemType Directory -Path $deltaDir -Force | Out-Null
    foreach ($id in $deltaIds) {
        $src = "$fullDir\$id.json"
        if (Test-Path $src) { Copy-Item $src $deltaDir }
    }

    $dest     = "$inbox/osv_bronze_landing_$eco"
    $total    = (Get-ChildItem $deltaDir -File).Count
    $uploaded = 0

    Write-Progress -Activity $eco -Status "Uploading 0 / $total files..." -PercentComplete 0
    & databricks fs cp $deltaDir $dest -r 2>&1 | ForEach-Object {
        $uploaded++
        $pct = [int](($uploaded / $total) * 100)
        Write-Progress -Activity $eco -Status "Uploading $uploaded / $total files..." -PercentComplete $pct
    }
    Write-Progress -Activity $eco -Completed
    Write-Host "Done: $eco"
}
