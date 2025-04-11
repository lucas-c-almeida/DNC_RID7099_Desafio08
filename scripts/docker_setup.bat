@echo off
echo Logging in to Docker...
docker login
if %ERRORLEVEL% NEQ 0 (
    echo Docker login failed. Exiting.
    exit /b 1
)

echo Building the Docker image...
docker build -t airflow-desafio8 .
if %ERRORLEVEL% NEQ 0 (
    echo Docker build failed. Exiting.
    exit /b 1
)

echo Starting Docker Compose...
docker-compose up -d
if %ERRORLEVEL% NEQ 0 (
    echo Docker Compose failed. Exiting.
    exit /b 1
)

echo All steps completed successfully!