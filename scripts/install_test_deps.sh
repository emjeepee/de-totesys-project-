#!/usr/bin/env bash

set -euo pipefail

echo "Upgrading pip..."
python -m pip install --upgrade pip

echo "Installing test dependencies..."
python -m pip install -r requirements/GHA_test_requirements.txt

echo "Test dependencies installed successfully."