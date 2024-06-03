# Sales Pipeline

## Problem Statement

## Data Source

* Customers master data from API
* Orders data from CSV
* Weather data from API

# Tools and Technologies

* Data storage : MySQL
* Data processing : Python Pandas
* Scheduler and workflow management : Apache Airflow
* Data visualization : PowerBI

# Sales Data Pipeline

## Setup environment

prequiresite: WSL/Linux
 
* Clone the repository.
* cp .env.example .env
* docker network create --driver bridge local_network
* docker compose up

## run pipelines
* open localhost:8080/ in browser(username: airflow, password=airflow)
* run pipeline from airflow UI(pipeline name: sales_pipeline)
* it will run the pipeline and load data into database.
## Folder Sturecture

* .env.example
* .gitignore
* docker-compose.yml
* sales_pipelines_db.pbix (powerbi Report file)
* screenshot/ (all screenshot referenced in readme.md file)
* readme.md
* airflow_data/dags/sales_pipeline.py (airflow dag file)
* airflow_data/dags/sales_data.csv (sales data)


## Pipeline snapshot

### pipeline graph
![pipeline graph](screenshot/pipeline_graph.png)

### pipeline detail
![pipeline tree](screenshot/pipeline_detail.png)


## Visualization 
* connection with MySQL database
* open PowerBI report file in PowerBI desktop
* refresh the data

### report snapshot

#### Report Summary
![Summary](screenshot/report_summary.png) 

#### Report Top Selling
![Report Top Selling](screenshot/report_top_selling.png) 

#### Report Trends Monthly / Quarterly
![Report Trends](screenshot/report_trends.png) 

#### Report AVG Order
![Report AVG Order](screenshot/report_avg_order.png) 

#### Report based on Weather Conditions
![Report Weather](screenshot/report_weather.png)

# Logical Schema Model 


```mermaid
erDiagram

    bronze_orders {
        int order_id PK "order_id"
        int customer_id FK "bronze_customer.id"
        int product_id
        int quantity
        float price
        datetime order_date
    }

    bronze_customers {
        int id PK "customer_id"
        string name
        string username
        string email
        string phone
        string website            
        string company_name
        string company_catchPhrase
        string company_bs
        string address_street
        string address_suite
        string address_city
        string address_zipcode
        float address_geo_lat
        float address_geo_lng
    }


    bronze_weather {
        int customer_id FK "bronze_customer.id"
        string base
        int visibility
        int dt
        int timezone
        int id
        string name
        int cod
        float coord_lon
        float coord_lat
        int weather_id
        string weather_main
        string weather_description
        string weather_icon
        float main_temp
        float main_feels_like
        float main_temp_min
        float main_temp_max
        int main_pressure
        int main_humidity
        int main_sea_level
        int main_grnd_level
        float wind_speed
        int wind_deg
        float wind_gust
        int clouds_all
        int sys_sunrise
        int sys_sunset
        float rain_1h
    }

    silver_sales {
        int order_id
        int customer_id FK
        int product_id
        int quantity
        float price
        datetime order_date
        varchar(255) customer_name
        text address_geo_lat
        text address_geo_lng
        varchar(255) weather_description 
        float main_temp
        float main_feels_like
        int main_humidity
        float wind_speed
        int clouds_all
}
 gold_avg_order_quantity_per_product {
    int product_id
    double quantity
 }

gold_sales_per_weather_condition {
    text weather_description
    double total_sales
    }

gold_sales_trends_monthly {
    int order_year
    int order_month
    bigint quantity
    double total_sales
}


gold_sales_trends_quarterly {
    int order_year
    int order_quarter
    bigint quantity
    double total_sales
}


gold_top_selling_customers {
    bigint customer_id
    bigint quantity
}

gold_top_selling_products {
    bigint product_id
    bigint quantity
}

gold_total_sales_by_customer {
    bigint customer_id FK "bronze_customer.id"
    double total_sales
}


bronze_customers ||--o{ bronze_orders : "ordered"
bronze_customers ||--|| bronze_weather : "Positioned at"
bronze_customers ||--o{ silver_sales : "saled"
bronze_customers ||--o{ gold_total_sales_by_customer : "total sales"
bronze_customers ||--o{ gold_top_selling_customers : "total selling customers"

```