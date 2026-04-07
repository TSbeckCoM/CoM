# thresholds.ps1
# Canonical lookup tables shared by StreamGageUpdate.ps1 and compute-trend.ps1

# Flood stage lookup table (Minor & Major)
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

