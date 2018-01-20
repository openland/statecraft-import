#!/bin/bash
set -e
mkdir -p tmp
mkdir -p downloads

wget -O "./tmp/Parcels.csv" "https://storage.googleapis.com/files-statecraft/Parcels_2017_05_10.csv"

# wget -O "./tmp/CURRENT" "https://storage.googleapis.com/files-statecraft/CURRENT"
# DATE=`cat ./tmp/CURRENT`
# DATE="${DATE//-/_}"
# wget -O "./tmp/Building_Permits.csv" "https://storage.googleapis.com/files-statecraft/Building_Permits_${DATE}.csv"
# mv ./tmp/CURRENT ./downloads/CURRENT

mv ./tmp/Parcels.csv ./downloads/Parcels.csv