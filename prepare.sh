#!/bin/sh
set -e
apt-get update -y
apt-get install -y python3 wget curl
python3 -m pip install quilt
rm -fr tmp