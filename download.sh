#!/bin/bash
set -e
mkdir -p tmp
mkdir -p downloads

wget -O "./tmp/CURRENT" "https://storage.googleapis.com/files-statecraft/CURRENT"
DATE=`cat ./tmp/CURRENT`
DATE="${DATE//-/_}"
wget -O "./tmp/Building_Permits.csv" "https://storage.googleapis.com/files-statecraft/Building_Permits_${DATE}.csv"

mv ./tmp/CURRENT ./downloads/CURRENT
mv ./tmp/Building_Permits.csv ./downloads/Building_Permits.csv