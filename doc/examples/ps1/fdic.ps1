param(
    [string]$Repdte = "20260331",
    [Alias("Profile", "p")]
    [ArgumentCompleter({
        param($cmd, $param, $word, $ast, $bound)
        # Profile names come from the CLI config file; they're case-sensitive.
        $cfg = if ($env:DATABRICKS_CONFIG_FILE) { $env:DATABRICKS_CONFIG_FILE } else { "$HOME\.databrickscfg" }
        Select-String -Path $cfg -Pattern '^\[(.+)\]\s*$' -ErrorAction SilentlyContinue |
            ForEach-Object { $_.Matches[0].Groups[1].Value } |
            Where-Object { $_ -ne "__settings__" -and $_ -like "$word*" }
    })]
    [string]$DatabricksProfile,  # Databricks CLI profile; empty uses DEFAULT
    [Alias("c")]
    [ArgumentCompleter({
        param($cmd, $param, $word, $ast, $bound)
        $p = if ($bound.DatabricksProfile) { @("--profile", $bound.DatabricksProfile) } else { @() }
        databricks catalogs list @p -o json 2>$null | ConvertFrom-Json |
            Where-Object { $_.name -like "*utility*" -and $_.name -like "$word*" } |
            ForEach-Object { $_.name }
    })]
    [string]$Catalog = "utility"  # catalog holding raw.inbox, e.g. prod_utility at work
)

$dbArgs = if ($DatabricksProfile) { @("--profile", $DatabricksProfile) } else { @() }

$downloads = "$env:USERPROFILE\Downloads"
$inbox     = "dbfs:/Volumes/$Catalog/raw/inbox"
$url       = "https://api.fdic.gov/banks/financials"
$fields    = "CERT,REPDTE,ASSET,DEP,NETINC,EQ,ROA,ROE"
$limit     = 500

# Fail before downloading anything if the inbox isn't reachable. Show the CLI's
# own error, since a wrong profile name (case-sensitive) also fails here.
$lsOut = & databricks fs ls $inbox @dbArgs 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Cannot reach inbox $inbox (profile: $(if ($DatabricksProfile) { $DatabricksProfile } else { 'DEFAULT' })): $(($lsOut | Out-String).Trim())"
    exit 1
}
Write-Host "Uploading to $inbox"

$localFile = "$downloads\fdic_financials.json"
$dest      = "$inbox/fdic_financials.json"

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
& databricks fs cp $localFile $dest @dbArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "databricks fs cp failed"
    exit 1
}

Remove-Item $localFile

Write-Host "Done: uploaded $($records.Count) records for REPDTE=$Repdte"
