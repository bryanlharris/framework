param(
    [string]$Repdte = "20260331"
)

$downloads = "$env:USERPROFILE\Downloads"
$inbox     = "dbfs:/Volumes/utility/file_router/inbox"
$url       = "https://api.fdic.gov/banks/financials"
$fields    = "CERT,REPDTE,ASSET,DEP,NETINC,EQ,ROA,ROE"
$limit     = 500

$localFile = "$downloads\fdic_bronze_landing_financials.json"
$dest      = "$inbox/fdic_bronze_landing_financials.json"

Write-Host "Fetching REPDTE=$Repdte from FDIC financials API..."

try {
    $response = Invoke-RestMethod -Uri $url -Method Get -Body @{
        filters = "REPDTE:$Repdte"
        fields  = $fields
        limit   = $limit
        format  = "json"
    }
} catch {
    Write-Error "FDIC API request failed: $_"
    exit 1
}

# The API wraps each record as {"data": {...fields...}, "score": N} inside a
# top-level "data" array. Unwrap to one flattened record per line (NDJSON) so
# it lands the same shape the bronze settings (multiLine: false) expect.
$records = $response.data | ForEach-Object { $_.data }

if (-not $records -or $records.Count -eq 0) {
    Write-Warning "No records returned for REPDTE=$Repdte. Nothing to upload."
    exit 0
}

Write-Host "Fetched $($records.Count) records."

if (Test-Path $localFile) { Remove-Item $localFile -Force }

$records |
    ForEach-Object { $_ | ConvertTo-Json -Compress -Depth 10 } |
    Set-Content -Path $localFile -Encoding utf8

Write-Host "Uploading to $dest..."
& databricks fs cp $localFile $dest
if ($LASTEXITCODE -ne 0) {
    Write-Error "databricks fs cp failed"
    exit 1
}

Remove-Item $localFile

Write-Host "Done: uploaded $($records.Count) records for REPDTE=$Repdte"
