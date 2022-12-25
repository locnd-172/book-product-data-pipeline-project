# DATA PIPELINES WITH AIRFLOW PROJECT

## Preparation
- Install Docker, Docker-compose on Ubuntu Distro - WSL2
- Initialize Airflow and Postgres in Docker
```Bash
sudo docker compose up airflow-init
sudo docker-compose up -d
sudo docker ps
```
- Access Airflow UI at [localhost:8080](localhost:8080), username: `airflow` and password: `airflow`
- Open pgAdmin at [localhost:5050](localhost), email: `lc.nguyendang123@gmail.com` and password: `admin`
- Register server: 
<p align="center">
    <img src="./assets/img/Postgres%20-%20Server%20Register.png">
</p>

