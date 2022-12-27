#!/bin/bash

echo "Start Docker..."
sudo service docker start &

wait  $!

echo "Init database for Airflow metadata..."
sudo docker-compose up airflow-init

echo "Starting up airflow in detached mode..."
sudo docker-compose up -d

sudo chown -R locnd172 ./dags

echo "Airflow started successfully!"