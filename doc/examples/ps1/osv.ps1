if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    Write-Error @"
gcloud not found. Install the Google Cloud SDK:
  winget:    winget install Google.CloudSDK
  Installer: https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe
"@
    exit 1
}

$downloads = "$env:USERPROFILE\Downloads"

$files = @(
    @{ url = "https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip"; name = "PyPI" },
    @{ url = "https://osv-vulnerabilities.storage.googleapis.com/CRAN/all.zip"; name = "CRAN" }
)

$inbox = "dbfs:/Volumes/utility/file_router/inbox"

foreach ($f in $files) {
    $zip    = "$downloads\osv_$($f.name)_all.zip"
    $outDir = "$downloads\osv_bronze_landing_$($f.name)"

    Write-Progress -Activity $f.name -Status "Downloading..." -PercentComplete 0
    Invoke-WebRequest -Uri $f.url -OutFile $zip

    if (Test-Path $outDir) { Remove-Item $outDir -Recurse -Force }

    Write-Progress -Activity $f.name -Status "Extracting..." -PercentComplete 33
    Expand-Archive -Path $zip -DestinationPath $outDir
    Remove-Item $zip

    $dest      = "$inbox/osv_bronze_landing_$($f.name)"
    $total     = (Get-ChildItem $outDir -Recurse -File).Count
    $uploaded  = 0

    Write-Progress -Activity $f.name -Status "Uploading 0 / $total files..." -PercentComplete 34

    & databricks fs cp $outDir $dest -r 2>&1 | ForEach-Object {
        $uploaded++
        $pct = 34 + [int](($uploaded / $total) * 66)
        Write-Progress -Activity $f.name -Status "Uploading $uploaded / $total files..." -PercentComplete $pct
    }

    Write-Progress -Activity $f.name -Completed
    Write-Host "Done: $($f.name)"
}
