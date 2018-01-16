#!/bin/bash
set -e
apt-get update -y
apt-get install -y python3 wget curl python3-pip
pip3 install quilt
rm -fr tmp
