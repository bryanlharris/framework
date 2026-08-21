param(
    [string]$SeriesId = "FEDFUNDS"
)

$downloads = "$env:USERPROFILE\Downloads"
$inbox     = "dbfs:/Volumes/utility/file_router/inbox"
$url       = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=$SeriesId"

$localFile = "$downloads\fedfunds.csv"
$dest      = "$inbox/fred_bronze_landing_fedfunds.csv"

Write-Host "Downloading $SeriesId from FRED..."

# Invoke-WebRequest gets its connection reset by fred.stlouisfed.org (works fine
# in a browser and via curl.exe, so it's specific to that client) — use curl.exe.
curl.exe -sf -o $localFile $url
if ($LASTEXITCODE -ne 0) {
    Write-Error "FRED download failed (curl exit code $LASTEXITCODE)"
    exit 1
}

Write-Host "Uploading to $dest..."
& databricks fs cp $localFile $dest
if ($LASTEXITCODE -ne 0) {
    Write-Error "databricks fs cp failed"
    exit 1
}

Remove-Item $localFile

Write-Host "Done: uploaded FRED series $SeriesId"
