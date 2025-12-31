#!/usr/bin/env bash
set -e

cd terraform/root-module

terraform init -input=false

terraform plan -detailed-exitcode || PLAN_EXIT=$?

if [ "${PLAN_EXIT:-0}" -eq 2 ]; then
  terraform apply -auto-approve
else
  echo "No infrastructure changes"
fi
