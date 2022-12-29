# DATA PIPELINE WITH AIRFLOW PROJECT

> A ETL data pipeline with Airflow, PostgreSQL, Docker, IBM DB2 on Cloud, IBM Cognos Dashboard Embedded.

## Project Overview

### Description

This is a learning project. The purpose of this project is to build an ETL pipeline that will be able to extract book data from TIKI, an ecommerce website. Then transform and load the data to a data warehouse on cloud and store as a data source. The data source can be used to connect or intergrate with BI tools that will help grasp better understand about the book product from ecommercial resources. 

### Problem statement

Firstly, book product data in the form of a JSON file from an online website, such as detail information about a book, storage, products' reviews, etc., needs to be crawled and stored in Postgres, utilized as the staging database. After that, data is read and processed using Python with Pandas library to handle missing values, format the data to make it more readable. The final steps are loading all of the data to IBM DB2 on Cloud and checking the data quality stored in the data warehouse. The entire process of data ingestion is required to be automated with a process automation tool - Airflow.

### Tech Stacks
- OS: `Ubuntu 22.04.1 LTS on WSL2`
- Containerization: `Docker 20.10.22`
- Automate Data Pipelines: `Airflow 2.5.0`
- Staging Database: `PostgreSQL 15.1`
- Data Warehouse: `IBM DB2 on Cloud`
- Building Dashboad: `IBM Cognos Dashboard Embedded`
- Language: `Python 3.10.6`

### Data Platform Architecture
<p align="center">
    <img src="./assets/img/Data%20Platform%20Architecture.jpg">
</p>

## Implementation 

### Preparation
- Install Docker, Docker-compose on Ubuntu Distro - WSL2
- Initialize Airflow and Postgres in Docker
    + For the first time, run 
    ```Bash
    sh ./scripts/setup_airflow.sh
    ```
    + Next times, just need to run
    ```Bash
    sh ./scripts/start_airflow.sh
    ```

- To install dependency modules (e.g: `pandas`, `psycopg2`, `ibm_db`), state the module name in file `requirements.txt` and run: 
    ```Bash
    sh ./scripts/install_python_modules`
    ```
    Explanation of the script:
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


### Database schema design

Using star scheme

**1. Staging Tables**
- staging.book_product_id
    ```
    product_id
    ```
- staging.book_product_data
    ```
    product_id 
    name 
    sku 
    price 
    original_price 
    discount 
    discount_rate 
    image_url 
    author 
    quantity_sold 
    publisher 
    manufacturer 
    number_of_pages 
    translator 
    publication_date 
    book_cover 
    width 
    height 
    category 
    category_id
    ```
- staging.book_product_review
    ```
    product_id 
    rating_average
    reviews_count
    count_1_star
    percent_1_star
    count_2_star
    percent_2_star
    count_3_star
    percent_3_star
    count_4_star
    percent_4_star
    count_5_star
    percent_5_star
    ```

**2. Fact Table**
- factbookproduct
    ```
    id AUTO INCREMENT
    product_id REFERENCES dimbook(product_id)
    category_id REFERENCES dimcategory(category_id)
    sku
    image_url
    quantity_sold
    price
    original_price
    discount
    discount_rate
    ```

**3. Dimension Table**
- dimbook
    ```
    product_id
    name
    author
    publisher
    manufacturer
    number_of_pages
    translator
    publication_date
    book_cover
    width
    height
    ```

- dimcategory
    ```
    category_id
    category
    ```

- dimreview
    ```
    product_id REFERENCES factbookproduct(id)
    rating_average
    reviews_count
    count_1_star
    percent_1_star
    count_2_star
    percent_2_star
    count_3_star
    percent_3_star
    count_4_star
    percent_4_star
    count_5_star
    percent_5_star
    ```

### Data pipeline
The graph view for of data pipeline displayed below describe the task dependencies and the workflow of ETL process:
<p align="center">
    <img src="./assets/img/Data%20Pipeline.png">
</p>

## Project Results
The ETL data pipeline to scrape and store Tiki's book data is successfully built. Airflow helps automate tasks in the process and schedule the time to run the jobs. All of the tasks in the pipeline were run correctly without any errors or interruptions.

The data stored in the DB2 data warehouse after running the pipeline can be used to do some EDA and make visualizations to drive insights.

### Report Dashboard

Data in data warehouse is used to make a simple dashboard in IBM Cognos Dashboard Embedded as shown in the image
> [Link to the Report Dashboard](https://dataplatform.cloud.ibm.com/dashboards/03accf9e-4a89-47c0-802f-7e063fe4c46d/view/0563de1c069462d56accf2e4079a28577431775ab3bb8b03d4d17b490d672597a96910c2c8271f0b8b180036faeb4308ce)

<p align="center">
    <img src="./assets/img/Book%20Data%20Report%20Dashboard.png">
</p>

### Some screenshots
<p align="center">
    <p>FactBookProduct Table</p>
    <img src="./assets/img/FactBookProduct%20Table.png">
</p>
<p align="center">
    <p>DimBook Table</p>
    <img src="./assets/img/DimBook%20Table.png">
</p>
<p align="center">
    <p>DimCategory Table</p>
    <img src="./assets/img/DimCategory%20Table.png">
</p>
<p align="center">
    <p>DimReview Table</p>
    <img src="./assets/img/DimReview%20Table.png">
</p>


## To-do
- [ ] Fix connection to IBM DB2.
- [ ] Modify scheme design: add `DimProduct` table
- [ ] Improve the data quality checks.
- [ ] Implement self-customized operator to perform data extraction and loading. 
- [ ] Refactor code to load data incrementally instead of full refresh (traditional *"drop and create"*)
- [ ] Implement Shopee/Fahasa web crawler using Scrapy and Splash. 
- [ ] Develop more insightful visualization


