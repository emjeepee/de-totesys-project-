#!/usr/bin/env bash

set -euo pipefail

echo "Upgrading pip and installing pip-tools..."
python -m pip install --upgrade pip
python -m pip install pip-tools

echo "Compiling layer requirements..."
cd requirements
pip-compile GHA_layer_requirements.in
cd ..

echo "Installing layer dependencies..."
mkdir -p layer/python/lib/python3.12/site-packages
python -m pip install \
  -r requirements/GHA_layer_requirements.txt \
  -t layer/python/lib/python3.12/site-packages

echo "Zipping the layer..."
cd layer
zip -r ../zipped_files/layer.zip python