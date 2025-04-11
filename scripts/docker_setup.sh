#!/bin/bash

echo "Logging in to Docker..."
docker login
if [ $? -ne 0 ]; then
    echo "Docker login failed. Exiting."
    exit 1
fi

echo "Building the Docker image..."
docker build -t airflow-desafio8 .
if [ $? -ne 0 ]; then
    echo "Docker build failed. Exiting."
    exit 1
fi

echo "Starting Docker Compose..."
docker-compose up -d
if [ $? -ne 0 ]; then
    echo "Docker Compose failed. Exiting."
    exit 1
fi

echo "All steps completed successfully!"