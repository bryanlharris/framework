param(
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

$files = @(
    @{ url = "https://www.edsm.net/dump/powerPlay.json.gz";                   name = "powerPlay"                   },
    @{ url = "https://www.edsm.net/dump/systemsWithCoordinates7days.json.gz"; name = "systemsWithCoordinates7days" },
    @{ url = "https://www.edsm.net/dump/systemsWithoutCoordinates.json.gz";   name = "systemsWithoutCoordinates"   }
)

$inbox = "dbfs:/Volumes/$Catalog/raw/inbox"

# Fail before downloading anything if the inbox isn't reachable. Show the CLI's
# own error, since a wrong profile name (case-sensitive) also fails here.
$lsOut = & databricks fs ls $inbox @dbArgs 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Cannot reach inbox $inbox (profile: $(if ($DatabricksProfile) { $DatabricksProfile } else { 'DEFAULT' })): $(($lsOut | Out-String).Trim())"
    exit 1
}
Write-Host "Uploading to $inbox"

foreach ($f in $files) {
    $gz   = "$downloads\$($f.name).json.gz"
    $json = "$downloads\edsm_$($f.name).json"

    Write-Progress -Activity $f.name -Status "Downloading..." -PercentComplete 0
    Invoke-WebRequest -Uri $f.url -OutFile $gz

    if (Test-Path $json) { Remove-Item $json }

    Write-Progress -Activity $f.name -Status "Decompressing..." -PercentComplete 33

    $in   = [System.IO.File]::OpenRead($gz)
    $out  = [System.IO.File]::Create($json)
    $gzip = New-Object System.IO.Compression.GZipStream($in, [System.IO.Compression.CompressionMode]::Decompress)
    $gzip.CopyTo($out)
    $gzip.Close(); $out.Close(); $in.Close()

    Remove-Item $gz

    $dest = "$inbox/edsm_$($f.name).json"

    Write-Progress -Activity $f.name -Status "Uploading..." -PercentComplete 66

    & databricks fs cp $json $dest @dbArgs 2>&1 | Out-Null

    Remove-Item $json

    Write-Progress -Activity $f.name -Completed
    Write-Host "Done: $($f.name)"
}
