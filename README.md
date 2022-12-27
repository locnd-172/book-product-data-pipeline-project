# DATA PIPELINE WITH AIRFLOW PROJECT

## Preparation
- Install Docker, Docker-compose on Ubuntu Distro - WSL2
- Initialize Airflow and Postgres in Docker
    + Run at the first time
    ```Bash
    sh ./scripts/setup_airflow.sh
    ```
    + Next times just need to run
    ```Bash
    sh ./scripts/start_airflow.sh
    ```

- Install dependency module (`pandas`: `v1.3.5`, `psycopg2`: `v2.9.5`)
    + Build extended docker image: `sudo docker build . --tag extending_airflow:latest`
    + Modify `docker-compose.yaml` file: `image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.5.0}` into `image: ${AIRFLOW_IMAGE_NAME:-extending_airflow:latest}`
    + Rebuild airflow webserver, airflow scheduler: `sudo docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler`
    + Repeat these steps whenever want to install a new dependency module.

- Access Airflow UI at `localhost:8080`, username: `airflow` and password: `airflow`
- Open pgAdmin at `localhost:5050`, email: `lc.nguyendang123@gmail.com` and password: `admin`
- Register server: 
<p align="center">
    <img src="./assets/img/Postgres%20-%20Server%20Register.png">
</p>