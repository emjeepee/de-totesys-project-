#!/usr/bin/env bash

set -euo pipefail

echo "Zipping Lambda functions..."

mkdir -p zipped_files

zip -r zipped_files/first_lambda.zip src/first_lambda
zip -r zipped_files/second_lambda.zip src/second_lambda
zip -r zipped_files/third_lambda.zip src/third_lambda

echo "Zipping complete."