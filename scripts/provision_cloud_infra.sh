#!/usr/bin/env bash
set -e

cd terraform/root-module

terraform init
terraform plan
terraform apply -auto-approve

