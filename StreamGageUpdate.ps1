# ---------------------------------------------
# Flood stage lookup table (Minor & Major only)
# ---------------------------------------------
$floodStages = @{
    "16415600" = @{ Minor = 10;   Major = $null }
    "16400000" = @{ Minor = 9.5;  Major = 19.9 }
    "16638500" = @{ Minor = 6;    Major = 8.61 }
    "16620000" = @{ Minor = 6;    Major = 7.9 }
    "16618000" = @{ Minor = 6;    Major = 12.4 }
    "16614000" = @{ Minor = 6;    Major = 11 }
    "16604500" = @{ Minor = 8.5;  Major = $null }
    "16605500" = @{ Minor = $null; Major = $null }
    "16587000" = @{ Minor = 3.5;  Major = 7.3 }
    "16552800" = @{ Minor = 5.4;  Major = 8.2 }
    "16518000" = @{ Minor = 10;   Major = 13.5 }
    "16508000" = @{ Minor = 7.5;  Major = 12.1 }
    "16501200" = @{ Minor = 7;    Major = 12.4 }
}

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

    $siteCode = $ts.sourceInfo.siteCode[0].value
    $siteName = $ts.sourceInfo.siteName
    $value    = $ts.values[0].value[0].value
    $dateTime = $ts.values[0].value[0].dateTime
    $varDesc  = $ts.variable.variableDescription

    # Lookup Minor/Major thresholds
    $stages = $floodStages[$siteCode]

    $minor = if ($stages) { $stages.Minor } else { $null }
    $major = if ($stages) { $stages.Major } else { $null }

    # Build clean output object
    [PSCustomObject]@{
        SiteName            = $siteName
        SiteCode            = $siteCode
        VariableDescription = $varDesc
        Value               = [double]$value
        Timestamp           = $dateTime
        MinorFlood          = $minor
        MajorFlood          = $major
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

# Path to your synced SharePoint folder
# C:\Users\tmschi\OneDrive - County of Maui\Documents
$outputPath = Join-Path -Path $PWD -ChildPath "latest.json"
Set-Content -Path $outputPath -Value $json

# Convert your results to JSON and write to file
$results | ConvertTo-Json -Depth 10 | Out-File -FilePath $outputPath -Encoding utf8

Write-Output "Working directory: $PWD"
Write-Host "Dashboard JSON written to GitHub repo."



