$url  = "https://www.edsm.net/dump/powerPlay.json.gz"
$gz   = "$env:USERPROFILE\Downloads\powerPlay.json.gz"
$json = "$env:USERPROFILE\Downloads\edsm_bronze_landing_powerPlay.json"

Invoke-WebRequest -Uri $url -OutFile $gz
if (Test-Path $json) { Remove-Item $json }

$in   = [System.IO.File]::OpenRead($gz)
$out  = [System.IO.File]::Create($json)
$gzip = New-Object System.IO.Compression.GZipStream($in, [System.IO.Compression.CompressionMode]::Decompress)
$gzip.CopyTo($out)
$gzip.Close(); $out.Close(); $in.Close()

Remove-Item $gz
Write-Host "Done: $json"

start c:\users\sqltest\Downloads
