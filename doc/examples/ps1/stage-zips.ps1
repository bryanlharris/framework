param(
    [string]$f
)

$inbox = "s3://your-bucket/your-prefix"

# Regex: name_yyyyMM[_optional_suffix].zip
$pattern = '^(.+?)_(\d{4})(\d{2})(_.+)?\.zip$'

if ($f) {
    $zips = @(Get-Item $f -ErrorAction Stop)
} else {
    $zips = @(Get-ChildItem -Filter "*.zip")
}

if ($zips.Count -eq 0) {
    Write-Host "No zip files found."
    exit 1
}

$invalid = $zips | Where-Object { $_.Name -notmatch $pattern }
if ($invalid) {
    Write-Host "The following files do not match the expected pattern (name_yyyyMM[_suffix].zip):"
    $invalid | ForEach-Object { Write-Host "  $($_.Name)" }
    exit 1
}

Write-Host "--- WARNING WARNING WARNING ---"
Write-Host "--- WARNING WARNING WARNING ---"
Write-Host "--- WARNING WARNING WARNING ---"
Write-Host ""
Write-Host "The following zip files will be moved and extracted:"
$zips | ForEach-Object { Write-Host "  $($_.Name)" }
Write-Host ""
Write-Host "Each zip will be moved into a name\yyyy\MM\ subfolder,"
Write-Host "then extracted into a folder of the same name (without .zip)."
Write-Host ""
Write-Host "--- WARNING WARNING WARNING ---"
Write-Host "--- WARNING WARNING WARNING ---"
Write-Host "--- WARNING WARNING WARNING ---"
Write-Host ""
$confirm = Read-Host "(y/n)"
if ($confirm -ne 'y') {
    Write-Host "Aborted."
    exit 0
}
Write-Host ""

$total    = $zips.Count
$index    = 0
$commands = @()

foreach ($zip in $zips) {
    $index++
    $name = $zip.Name

    $null = $name -match $pattern
    $prefix = $Matches[1]
    $year   = $Matches[2]
    $month  = $Matches[3]
    $stem   = [System.IO.Path]::GetFileNameWithoutExtension($name)

    $targetDir  = Join-Path (Split-Path $zip.FullName) "$prefix\$year\$month"
    $targetZip  = Join-Path $targetDir $name
    $extractDir = Join-Path $targetDir $stem
    $prefixDir  = Join-Path (Split-Path $zip.FullName) $prefix

    $activity = "$name ($index of $total)"

    Write-Progress -Activity $activity -Status "Creating folders..." -PercentComplete 0

    New-Item -ItemType Directory -Path $targetDir  -Force | Out-Null
    New-Item -ItemType Directory -Path $extractDir -Force | Out-Null

    Write-Progress -Activity $activity -Status "Moving zip..." -PercentComplete 20

    Move-Item -Path $zip.FullName -Destination $targetZip -Force

    Write-Progress -Activity $activity -Status "Extracting..." -PercentComplete 40

    Expand-Archive -Path $targetZip -DestinationPath $extractDir -Force

    Write-Progress -Activity $activity -Completed
    Write-Host "Done: $stem"

    $commands += "aws s3 sync ```n    `"$prefixDir`" ```n    $inbox/$prefix"
}

Write-Host ""
Write-Host "--- Upload commands ---"
Write-Host ""
$commands | ForEach-Object { Write-Host $_ }
Write-Host ""
