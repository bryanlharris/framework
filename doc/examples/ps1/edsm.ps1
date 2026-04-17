$downloads = "$env:USERPROFILE\Downloads"

$files = @(
    @{ url = "https://www.edsm.net/dump/powerPlay.json.gz";                   name = "powerPlay"                   },
    @{ url = "https://www.edsm.net/dump/systemsWithCoordinates7days.json.gz"; name = "systemsWithCoordinates7days" },
    @{ url = "https://www.edsm.net/dump/systemsWithoutCoordinates.json.gz";   name = "systemsWithoutCoordinates"   }
)

foreach ($f in $files) {
    $gz   = "$downloads\$($f.name).json.gz"
    $json = "$downloads\edsm_bronze_landing_$($f.name).json"

    Write-Host "Downloading $($f.name)..."
    Invoke-WebRequest -Uri $f.url -OutFile $gz

    if (Test-Path $json) { Remove-Item $json }

    $in   = [System.IO.File]::OpenRead($gz)
    $out  = [System.IO.File]::Create($json)
    $gzip = New-Object System.IO.Compression.GZipStream($in, [System.IO.Compression.CompressionMode]::Decompress)
    $gzip.CopyTo($out)
    $gzip.Close(); $out.Close(); $in.Close()

    Remove-Item $gz
    Write-Host "Done: $json"
}

start $downloads
