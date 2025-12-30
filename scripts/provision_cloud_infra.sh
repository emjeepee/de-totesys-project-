#!/usr/bin/env bash
set -e

terraform init
terraform plan
terraform apply -auto-approve

