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

# ---------------------------------------------
# Kona max lookup table (per station, by SiteCode)
# ---------------------------------------------
$konaMax = @{
    "16415600" = @{ Kona1 = 12.3; Kona2 = $null }
    "16400000" = @{ Kona1 = 9.3; Kona2 = $null }
    "16638500" = @{ Kona1 = 3.5; Kona2 = $null }
    "16620000" = @{ Kona1 = 6.1;  Kona2 = $null }
    "16618000" = @{ Kona1 = 5.3;  Kona2 = $null }
    "16614000" = @{ Kona1 = 6.7; Kona2 = $null }
    "16604500" = @{ Kona1 = 8.3;  Kona2 = 4.0 }
    "16605500" = @{ Kona1 = 13.4; Kona2 = 13.5 } 
    "16587000" = @{ Kona1 = 2.5;  Kona2 = $null }
    "16552800" = @{ Kona1 = 6.5;  Kona2 = $null }
    "16518000" = @{ Kona1 = 9.0; Kona2 = $null }
    "16508000" = @{ Kona1 = 8.8;  Kona2 = $null }
    "16501200" = @{ Kona1 = 9.4;  Kona2 = $null }
    "16501200" = @{ Kona1 = 9.4;  Kona2 = $null }
    
    "16641000" = @{ Kona1 = 15.2;  Kona2 = $null }
    "16527000" = @{ Kona1 = 8.2;  Kona2 = $null }
    "16647900" = @{ Kona1 = 6.5;  Kona2 = $null }
    "16415000" = @{ Kona1 = 6.4;  Kona2 = $null }
    "16611500" = @{ Kona1 = 6.1;  Kona2 = $null }
    "16414200" = @{ Kona1 = 5.8;  Kona2 = $null }
    "16545000" = @{ Kona1 = 5.4;  Kona2 = $null }
    "16570000" = @{ Kona1 = 4.8;  Kona2 = $null }
    "16536000" = @{ Kona1 = 4.0;  Kona2 = $null }
    "16623300" = @{ Kona1 = 1.7;  Kona2 = $null }
}
