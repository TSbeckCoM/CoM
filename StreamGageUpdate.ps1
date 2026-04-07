. "$PSScriptRoot/thresholds.ps1"

# ---------------------------------------------
# Pull USGS stream gage data
# ---------------------------------------------
$response = Invoke-RestMethod -Method GET -Uri `
    "https://waterservices.usgs.gov/nwis/iv/?format=json&countyCd=15009&indent=on&siteStatus=active&siteType=ST"

# Filter to gage height (00065)
$stations = $response.value.timeSeries |
    Where-Object { $_.variable.variableCode[0].value -eq "00065" }

# ---------------------------------------------
# Process each station
# ---------------------------------------------
$results = foreach ($ts in $stations) {

    $siteCode  = $ts.sourceInfo.siteCode[0].value
    $siteName  = $ts.sourceInfo.siteName
    $value     = $ts.values[0].value[0].value
    $dateTime  = $ts.values[0].value[0].dateTime
    $varDesc   = $ts.variable.variableDescription
    $latitude  = $ts.sourceInfo.geoLocation.geogLocation.latitude
    $longitude = $ts.sourceInfo.geoLocation.geogLocation.longitude
    
    # Lookup Minor/Major thresholds
    $stages = $floodStages[$siteCode]
    $minor = if ($stages) { $stages.Minor } else { $null }
    $major = if ($stages) { $stages.Major } else { $null }
    
    # Lookup Kona max (per station)
    $kona = $konaMax[$siteCode]
    $kona1 = if ($kona) { $kona.Kona1 } else { $null }
    $kona2 = if ($kona) { $kona.Kona2 } else { $null }

    # Build clean output object
    [PSCustomObject]@{
        SiteName            = $siteName
        SiteCode            = $siteCode
        VariableDescription = $varDesc
        Value               = [double]$value
        #Value               = $valFeet
        #ValueUnit           = $unitCode
        Timestamp           = $dateTime
        MinorFlood          = $minor
        MajorFlood          = $major
        Latitude            = $latitude
        Longitude           = $longitude   
        Kona1Max            = $kona1
        Kona2Max            = $kona2
    }
}

# ---------------------------------------------
# Color-coded output
# ---------------------------------------------
foreach ($r in $results | Sort-Object SiteCode) {

    $value = [double]$r.Value
    $minor = $r.MinorFlood
    $major = $r.MajorFlood

    # Determine color
    if (-not $minor -and -not $major) {
        # No thresholds available
        $color = "Black"
    }
    elseif ($major -and $value -ge $major) {
        # Major flooding
        $color = "Red"
    }
    elseif ($minor -and ($minor - $value) -le 1 -and $value -lt $minor) {
        # Within 1 foot of Minor threshold
        $color = "Yellow"
    }
    elseif ($minor -and $value -ge $minor) {
        # Minor flooding
        $color = "DarkYellow"   # closest to Orange
    }
    else {
        # Normal
        $color = "Green"
    }

    # Print formatted line
    Write-Host ("{0,-10} {1,-8} {2,-25} {3,-8} {4,-8} {5}" -f `
        $r.SiteCode, $value, $r.SiteName, $minor, $major, $r.Timestamp) -ForegroundColor $color
}

# Write latest.json cleanly
$outputPath = Join-Path -Path $PWD -ChildPath "latest.json"
$results | ConvertTo-Json -Depth 10 | Out-File -FilePath $outputPath -Encoding utf8

Write-Host "Dashboard JSON written to GitHub repo."










