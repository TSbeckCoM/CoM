# ------------------------------------------------------------
# compute-trend.ps1
# Computes 2-hour moving-average trend for all stations
# ------------------------------------------------------------

# Paths
$latestPath   = Join-Path $PSScriptRoot "latest.json"
$historyPath  = Join-Path $PSScriptRoot "history.json"
$outputPath   = Join-Path $PSScriptRoot "latest_with_trend.json"

# ------------------------------------------------------------
# Load latest.json (current readings)
# ------------------------------------------------------------
if (-not (Test-Path $latestPath)) {
    Write-Host "ERROR: latest.json not found."
    exit 1
}

$latest = Get-Content $latestPath | ConvertFrom-Json

# ------------------------------------------------------------
# Load or initialize history.json
# ------------------------------------------------------------
if (Test-Path $historyPath) {
    $history = Get-Content $historyPath | ConvertFrom-Json
} else {
    $history = @()
}

# Convert to a mutable list
$historyList = New-Object System.Collections.Generic.List[Object]
$historyList.AddRange($history)

# ------------------------------------------------------------
# Append current readings to history
# ------------------------------------------------------------
foreach ($item in $latest) {
    $historyList.Add([PSCustomObject]@{
        SiteCode  = $item.SiteCode
        Value     = [double]$item.Value
        Timestamp = $item.Timestamp
    })
}

# ------------------------------------------------------------
# Trim history to last 2 hours
# ------------------------------------------------------------
$cutoff = (Get-Date).ToUniversalTime().AddHours(-2)

$historyList = $historyList | Where-Object {
    (Get-Date $_.Timestamp) -ge $cutoff
}

# ------------------------------------------------------------
# Compute moving-average trend for each site
# ------------------------------------------------------------
foreach ($item in $latest) {

    $current = [double]$item.Value
    $siteCode = $item.SiteCode

    # Get all history entries for this site in last 2 hours
    $siteHistory = $historyList | Where-Object { $_.SiteCode -eq $siteCode }

    if ($siteHistory.Count -eq 0) {
        $trend = "N/A"
    }
    else {
        # Compute average
        $avg = ($siteHistory | Measure-Object -Property Value -Average).Average

        # Compare current to average
        $delta = $current - $avg

        # Tolerance to avoid jitter
        $tolerance = 0.02

        if ([math]::Abs($delta) -lt $tolerance) {
            $trend = "Steady"
        }
        elseif ($delta -gt 0) {
            $trend = "Rising"
        }
        else {
            $trend = "Falling"
        }
    }

    # Add Trend to the item
    $item | Add-Member -NotePropertyName Trend -NotePropertyValue $trend -Force
}

# ------------------------------------------------------------
# Save updated JSON with Trend
# ------------------------------------------------------------
$latest | ConvertTo-Json -Depth 10 | Out-File $outputPath -Encoding utf8

# ------------------------------------------------------------
# Save updated history.json
# ------------------------------------------------------------
$historyList | ConvertTo-Json -Depth 10 | Out-File $historyPath -Encoding utf8

Write-Host "Trend calculation complete. Output written to latest_with_trend.json"
