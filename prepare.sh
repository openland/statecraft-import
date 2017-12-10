#!/bin/sh
set -e
apt-get update -y
apt-get install -y python3 wget curl
apt-get install python3-setuptools
easy_install3 pip
python3 -m pip install quilt
rm -fr tmp