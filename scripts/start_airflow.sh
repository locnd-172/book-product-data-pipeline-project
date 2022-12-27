#!/bin/bash

echo "Start Docker..."
sudo service docker start &

wait $!

echo "Starting up Airflow in detached mode..."
sudo docker-compose up -d

sudo chown -R locnd172 ./dags

echo "Airflow started successfully!"