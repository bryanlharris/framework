$downloads = "$env:USERPROFILE\Downloads"

$files = @(
    @{ url = "https://osv-vulnerabilities.storage.googleapis.com/PyPI/all.zip"; name = "PyPI" },
    @{ url = "https://osv-vulnerabilities.storage.googleapis.com/CRAN/all.zip"; name = "CRAN" }
)

$inbox = "dbfs:/Volumes/utility/file_router/inbox"

foreach ($f in $files) {
    $zip    = "$downloads\osv_$($f.name)_all.zip"
    $outDir = "$downloads\osv_bronze_landing_$($f.name)"

    Write-Host "Downloading $($f.name)..."
    Invoke-WebRequest -Uri $f.url -OutFile $zip

    if (Test-Path $outDir) { Remove-Item $outDir -Recurse -Force }

    Write-Host "Extracting $($f.name)..."
    Expand-Archive -Path $zip -DestinationPath $outDir

    Remove-Item $zip

    $dest = "$inbox/osv_bronze_landing_$($f.name)"
    Write-Host "Uploading $($f.name) to $dest..."
    databricks fs cp $outDir $dest -r

    Write-Host "Done: $($f.name)"
}
