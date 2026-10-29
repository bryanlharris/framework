param(
    [string]$SeriesId = "FEDFUNDS",
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
$url       = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=$SeriesId"

# Fail before downloading anything if the inbox isn't reachable. Show the CLI's
# own error, since a wrong profile name (case-sensitive) also fails here.
$lsOut = & databricks fs ls $inbox @dbArgs 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Cannot reach inbox $inbox (profile: $(if ($DatabricksProfile) { $DatabricksProfile } else { 'DEFAULT' })): $(($lsOut | Out-String).Trim())"
    exit 1
}
Write-Host "Uploading to $inbox"

$localFile = "$downloads\fedfunds.csv"
$dest      = "$inbox/fred_fedfunds.csv"

Write-Host "Downloading $SeriesId from FRED..."

# Invoke-WebRequest gets its connection reset by fred.stlouisfed.org (works fine
# in a browser and via curl.exe, so it's specific to that client) — use curl.exe.
curl.exe -sf -o $localFile $url
if ($LASTEXITCODE -ne 0) {
    Write-Error "FRED download failed (curl exit code $LASTEXITCODE)"
    exit 1
}

# FRED sometimes answers 200 with an HTML maintenance page; uploading that
# adds a bogus column to the bronze schema. Only upload the real CSV.
$header = (Get-Content $localFile -TotalCount 1).Trim()
if ($header -ne "observation_date,$SeriesId") {
    Write-Error "FRED did not return the expected CSV (first line: '$header'); not uploading. See $localFile"
    exit 1
}

Write-Host "Uploading to $dest..."
& databricks fs cp $localFile $dest @dbArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "databricks fs cp failed"
    exit 1
}

Remove-Item $localFile

Write-Host "Done: uploaded FRED series $SeriesId"
