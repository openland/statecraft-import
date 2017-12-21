#!/bin/sh
set -e
mkdir -p tmp
mkdir -p downloads

# Download Building Permits
# wget -O "./tmp/Building_Permits.csv" "https://data.sfgov.org/api/views/i98e-djp9/rows.csv?accessType=DOWNLOAD"
# wget -O "./tmp/Building_Permits.csv" "https://storage.googleapis.com/files-statecraft/Building_Permits_2017_08_02.csv"
# wget -O "./tmp/Building_Permits.csv" "https://storage.googleapis.com/files-statecraft/Building_Permits_2017_12_10.csv"
wget -O "./tmp/Building_Permits.csv" "https://storage.googleapis.com/files-statecraft/Building_Permits_2017_12_15.csv"

mv ./tmp/Building_Permits.csv ./downloads/Building_Permits.csv