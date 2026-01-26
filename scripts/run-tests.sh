#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONTAINER_NAME="mythix-orm-postgresql-test"
COVERAGE=false

# Parse arguments
ARGS=()
for arg in "$@"; do
    if [[ "$arg" == "--coverage" ]]; then
        COVERAGE=true
    else
        ARGS+=("$arg")
    fi
done

cd "$PROJECT_DIR"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH" >&2
    exit 1
fi

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Starting PostgreSQL container..."
    docker compose up -d --wait
fi

# Run tests with correct port
if [[ "$COVERAGE" == true ]]; then
    PGPORT=5439 node ./node_modules/.bin/nyc ./node_modules/.bin/jasmine "${ARGS[@]}"
else
    PGPORT=5439 node ./node_modules/.bin/jasmine "${ARGS[@]}"
fi
