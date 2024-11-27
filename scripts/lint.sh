#!/usr/bin/env bash

set -euo pipefail

if ! [[ "$0" =~ scripts/lint.sh ]]; then
  echo "must be run from repository root"
  exit 255
fi

go install -v github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.2
golangci-lint run --config .golangci.yml

echo "ALL SUCCESS!"
