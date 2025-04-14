#!/bin/bash

# Default to development if no environment specified
ENVIRONMENT=${1:-development}

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(development|testing|production)$ ]]; then
    echo "Invalid environment: $ENVIRONMENT"
    echo "Usage: $0 [development|testing|production]"
    exit 1
fi

echo "Starting application in $ENVIRONMENT environment..."
ENVIRONMENT=$ENVIRONMENT docker compose up --build