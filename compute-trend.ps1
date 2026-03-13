# ------------------------------------------------------------
# compute-trend.ps1
# Computes 3-hour moving-average trend for all Maui County stations
# Uses REAL USGS 3-hour history (no local history.json needed)
# Also outputs history.json for inspection
# ------------------------------------------------------------

# Paths
$latestPath   = Join-Path $PSScriptRoot "latest.json"
$outputPath   = Join-Path $PSScriptRoot "latest_with_trend.json"
$historyOut   = Join-Path $PSScriptRoot "history.json"

# ------------------------------------------------------------
# Load latest.json (current readings)
# ------------------------------------------------------------
if (-not (Test-Path $latestPath)) {
    Write-Host "ERROR: latest.json not found."
    exit 1
}

$latest = Get-Content $latestPath | ConvertFrom-Json

# ------------------------------------------------------------
# Fetch 3-hour USGS history for Maui County (15009)
# ------------------------------------------------------------
$historyUrl = "https://waterservices.usgs.gov/nwis/iv/?format=json&countyCd=15009&period=PT3H&siteStatus=active"

try {
    $usgsHistory = Invoke-RestMethod -Uri $historyUrl -TimeoutSec 30
}
catch {
    Write-Host "ERROR: Unable to fetch USGS history."
    exit 1
}

# ------------------------------------------------------------
# Build lookup: SiteCode → list of { Timestamp, Value }
# ------------------------------------------------------------
$historyBySite = @{}

foreach ($ts in $usgsHistory.value.timeSeries) {

    $siteCode = $ts.sourceInfo.siteCode[0].value

    $entries = @()
    foreach ($v in $ts.values[0].value) {
        $entries += [PSCustomObject]@{
            Timestamp = $v.dateTime
            Value     = [double]$v.value   # <-- FIXED
        }
    }

    $historyBySite[$siteCode] = $entries
}

# ------------------------------------------------------------
# Save history.json (for your inspection)
# ------------------------------------------------------------
$historyBySite | ConvertTo-Json -Depth 10 | Out-File $historyOut -Encoding utf8

# ------------------------------------------------------------
# Compute moving-average trend for each site
# ------------------------------------------------------------
foreach ($item in $latest) {

    $siteCode = $item.SiteCode
    $current = [double]$item.Value

    if (-not $historyBySite.ContainsKey($siteCode)) {
        $trend = "N/A"
    }
    else {
        $entries = $historyBySite[$siteCode]

        if ($entries.Count -lt 2) {
            $trend = "N/A"
        }
        else {
            # Compute 3-hour average
            $avg = ($entries.Value | Measure-Object -Average).Average

            # Compare current to average
            $delta = $current - $avg

            # Tolerance to avoid jitter
            $tolerance = 0.02

            # Determine arrow
            if ([math]::Abs($delta) -lt $tolerance) {
                $arrow = "--"
            }
            elseif ($delta -gt 0) {
                $arrow = "↑"
            }
            else {
                $arrow = "↓"
            }

            # Absolute numeric change, 2 decimals
            $absDelta = [math]::Abs($delta)
            $formattedDelta = "{0:0.00}" -f $absDelta

            # Final trend string
            $trend = "$arrow $formattedDelta"
        }
    }

    # Add Trend to the item
    $item | Add-Member -NotePropertyName Trend -NotePropertyValue $trend -Force
}

# ------------------------------------------------------------
# Save updated JSON with Trend
# ------------------------------------------------------------
$latest | ConvertTo-Json -Depth 10 | Out-File $outputPath -Encoding utf8

Write-Host "Trend calculation complete using USGS 3-hour history."
Write-Host "history.json written for inspection."
