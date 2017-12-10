#!/bin/sh
mkdir -p tmp
mkdir -p downloads

# Download Building Permits
wget -O "./tmp/Building_Permits.csv" "https://data.sfgov.org/api/views/i98e-djp9/rows.csv?accessType=DOWNLOAD"
cp yes | cp -rf ./tmp/Building_Permits.csv ./downloads/Building_Permits.csv