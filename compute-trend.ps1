# ---------------------------------------------
# Load latest.json (current readings)
# ---------------------------------------------
$latestPath = Join-Path $PSScriptRoot "latest.json"
$latest = Get-Content $latestPath | ConvertFrom-Json

# ---------------------------------------------
# Load previous.json (last run's readings)
# ---------------------------------------------
$previousPath = Join-Path $PSScriptRoot "previous.json"

if (Test-Path $previousPath) {
    $previous = Get-Content $previousPath | ConvertFrom-Json
} else {
    $previous = @()
}

# Convert previous readings into a lookup table by SiteCode
$prevLookup = @{}
foreach ($p in $previous) {
    $prevLookup[$p.SiteCode] = $p
}

# ---------------------------------------------
# Compute Trend for each station
# ---------------------------------------------
foreach ($item in $latest) {

    $current = [double]$item.Value

    if ($prevLookup.ContainsKey($item.SiteCode)) {
        $prevValue = [double]$prevLookup[$item.SiteCode].Value
    } else {
        $prevValue = $null
    }

    # Determine trend
    if ($prevValue -eq $null) {
        $trend = "N/A"
    }
    elseif ($current -gt $prevValue) {
        $trend = "Rising"
    }
    elseif ($current -lt $prevValue) {
        $trend = "Falling"
    }
    else {
        $trend = "Steady"
    }

    # Add fields
    $item | Add-Member -NotePropertyName PreviousValue -NotePropertyValue $prevValue
    $item | Add-Member -NotePropertyName Trend -NotePropertyValue $trend
}

# ---------------------------------------------
# Save updated JSON with Trend
# ---------------------------------------------
$trendOutput = Join-Path $PSScriptRoot "latest_with_trend.json"
$latest | ConvertTo-Json -Depth 10 | Out-File $trendOutput -Encoding utf8

# ---------------------------------------------
# Save current readings as previous.json for next run
# ---------------------------------------------
$latest | ConvertTo-Json -Depth 10 | Out-File $previousPath -Encoding utf8

Write-Host "Trend calculation complete. Output written to latest_with_trend.json"
