"""
This script defines a DAG (Directed Acyclic Graph) for a sales pipeline in Apache Airflow.
The DAG consists of several tasks that extract data from an API, transform it, load it into a MySQL database,
and perform various data processing and analysis tasks.

The DAG tasks include:
- test_connection: Tests the connection to the MySQL database.
- fetch_customer_from_api: Fetches customer data from an API and writes it to the database.
- fetch_weather_from_api: Fetches weather data for each customer from an API and writes it to the database.
- load_order: Loads order data from a CSV file and writes it to the database.
- etl_sales: Performs ETL (Extract, Transform, Load) operations on the order data.
- process_total_sales: Calculates the total sales amount per customer.
- process_avg_order_quantity: Determines the average order quantity per product.
- process_top_selling_products: Identifies the top-selling products.
- process_top_selling_customers: Identifies the top-selling customers.
- process_sales_trends_monthly: Analyzes sales trends over time (monthly sales).
- process_sales_trends_quarterly: Analyzes sales trends over time (quarterly sales).
- process_sales_per_weather_condition: Calculates the average sales amount per weather condition.

The DAG is scheduled to run adhoc.

"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from sqlalchemy import create_engine
import os

# Constants
TARGET_CONNECTION = 'pipelines_data'
DAG_PATH = "/opt/airflow/dags"
DB_CONNECTION_STR = 'mysql+pymysql://my_user:my_password@mysql:3306/pipelines_data'
API_KEY = '35304cb8e4392a03ff3d3226c5cce2c6'  # Replace with your OpenWeatherMap API key

TABLES_TO_CREATE = {
    "bronze_customers": "bronze_customers",
    "bronze_orders": "bronze_orders",
    "bronze_weather": "bronze_weather",
    "silver_sales": "silver_sales"
}

# Helper functions

def fetch_users():
    """
    Fetches user data from an API.

    Returns:
        pandas.DataFrame: User data.
    """
    user_response = requests.get('https://jsonplaceholder.typicode.com/users')
    users = pd.json_normalize(user_response.json())
    return users

def normalize_columns(df):
    """
    Normalizes column names in a DataFrame.

    Args:
        df (pandas.DataFrame): Input DataFrame.

    Returns:
        pandas.DataFrame: DataFrame with normalized column names.
    """
    print(df.columns)
    df.columns = [c.replace('.', '_') for c in df.columns]
    print(df.columns)
    print(df.head())
    return df

def write_to_db(df, table, if_exists='replace', index=False, index_labels=None):
    """
    Writes a DataFrame to a MySQL database table.

    Args:
        df (pandas.DataFrame): DataFrame to write.
        table (str): Table name.
        if_exists (str, optional): Action to take if the table already exists. Defaults to 'replace'.
        index (bool, optional): Whether to write the DataFrame index. Defaults to False.
        index_labels (list, optional): Labels for the index columns. Defaults to None.
    """
    engine = create_engine(DB_CONNECTION_STR)
    df.to_sql(table, engine, if_exists=if_exists, index=index, index_label=index_labels)

def get_weather(id, lat, lon):
    """
    Fetches weather data for a given location.

    Args:
        id (int): Customer ID.
        lat (float): Latitude of the location.
        lon (float): Longitude of the location.

    Returns:
        dict: Weather data.
    """
    url = f'http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        data["customer_id"] = id
        return data
    return None


def fetch_transform_write():
    """
    Fetches users data, normalizes columns, sets index, and writes the transformed data to the database.

    Returns:
        None
    """
    df = fetch_users()
    df = normalize_columns(df)
    df = df.set_index(["id"])
    write_to_db(df, TABLES_TO_CREATE["bronze_customers"], index=True)



def load_from_db(sql):
    """
    Loads data from the database using a SQL query.

    Args:
        sql (str): SQL query.

    Returns:
        pandas.DataFrame: Query result.
    """
    engine = create_engine(DB_CONNECTION_STR)
    return pd.read_sql(sql, con=engine)


def test_connection():
    """
    Test the connection to the database by executing a sample query.

    Returns:
        None
    """
    df = load_from_db("SELECT 1")
    print(df.head())



def load_csv():
    """
    Load CSV data into a DataFrame and write it to a database table.

    Returns:
        None
    """
    print(f"listing dags path : {DAG_PATH}")
    print(os.listdir(DAG_PATH))
    print("Loading CSV")
    df = pd.read_csv(os.path.join(DAG_PATH,'sales_data.csv'))
    df = df.set_index(['customer_id'])
    write_to_db(df, TABLES_TO_CREATE["bronze_orders"], index=True)
    print("CSV Loaded")

def fetch_weather():
    """
    Fetches weather data for customers from the database, processes it, and writes it to another table.

    Returns:
        None
    """
    df = load_from_db(f"SELECT * FROM {TABLES_TO_CREATE['bronze_customers']}")
    df['weather'] = df.apply(lambda row: get_weather(row["id"], row['address_geo_lat'], row['address_geo_lng']), axis=1)
    weather_list = filter(None, df["weather"].to_list())
    weather_list1 = []
    for d in weather_list:
        d["weather"] = d["weather"][0]
        weather_list1.append(d)
    weather_df = pd.json_normalize(weather_list1)
    weather_df = normalize_columns(weather_df)
    weather_df = weather_df.set_index(["customer_id"])
    write_to_db(weather_df, TABLES_TO_CREATE["bronze_weather"], index=True)


def etl_sales():
    """
    Extracts sales data from the database, performs transformations, and loads it into the silver_sales table.

    Returns:
        None
    """
    query = """select o.*, c.name as customer_name, c.address_geo_lat, c.address_geo_lng, w.weather_description, w.main_temp, w.main_feels_like, w.main_humidity, 
    w.wind_speed, w.clouds_all
    from bronze_orders o 
    inner join bronze_customers c on c.id=o.customer_id
    inner join bronze_weather w on w.customer_id=o.customer_id
    """
    df = load_from_db(query)
    df = df.set_index(['customer_id', 'product_id'])
    write_to_db(df, TABLES_TO_CREATE["silver_sales"], index=True)



def process_total_sales():
    """
    Process the total sales by customer.

    This function loads data from the database, calculates the total sales for each customer,
    and writes the results to the 'gold_total_sales_by_customer' table in the database.

    Returns:
        None
    """
    df = load_from_db(f"SELECT * FROM {TABLES_TO_CREATE['silver_sales']}")
    df["total_sales"] = df["quantity"] * df["price"]
    df = df.groupby(["customer_id"]).agg({"total_sales": "sum"}).reset_index()
    write_to_db(df, 'gold_total_sales_by_customer')



def process_avg_order_quantity():
    """
    Calculate the average order quantity per product.

    Returns:
        None
    """
    df = load_from_db(f"SELECT * FROM {TABLES_TO_CREATE["silver_sales"]}")
    df = df.groupby(["product_id"]).agg({"quantity": "mean"}).reset_index()
    write_to_db(df, 'gold_avg_order_quantity_per_product')



def process_top_selling_products():
    """
    Process the top selling products from the silver_sales table and store the result in the gold_top_selling_products table.

    Returns:
        None
    """
    df = load_from_db(f"SELECT * FROM {TABLES_TO_CREATE['silver_sales']}")
    df = df.groupby(["product_id"]).agg({"quantity": "sum"}).reset_index()
    df = df.sort_values(by="quantity", ascending=False)
    write_to_db(df, 'gold_top_selling_products')


# Identify the top-selling customers
def process_top_selling_customers():
    """
    Process the top selling customers by aggregating the quantity of sales for each customer,
    sorting them in descending order, and writing the result to the 'gold_top_selling_customers' table.

    Returns:
        None
    """
    df = load_from_db(f"SELECT * FROM {TABLES_TO_CREATE['silver_sales']}")
    df = df.groupby(["customer_id"]).agg({"quantity": "sum"}).reset_index()
    df = df.sort_values(by="quantity", ascending=False)
    df = df.set_index(['customer_id'])
    write_to_db(df, 'gold_top_selling_customers', index=True)
    


def process_sales_trends_monthly():
    """
    Process the monthly sales trends.

    Returns:
        None
    """
    df = load_from_db(f"SELECT * FROM {TABLES_TO_CREATE['silver_sales']}")
    df["total_sales"] = df["quantity"] * df["price"]
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_month"] = df["order_date"].dt.month
    df["order_year"] = df["order_date"].dt.year
    df = df.groupby(["order_year", "order_month"]).agg({"quantity": "sum", "total_sales": "sum"}).reset_index()
    write_to_db(df, 'gold_sales_trends_monthly')


# Analyze sales trends over time  quarterly sales.
def process_sales_trends_quarterly():
    """
    Process sales trends quarterly.

    Returns:
        None
    """
    df = load_from_db(f"SELECT * FROM {TABLES_TO_CREATE['silver_sales']}")
    df["total_sales"] = df["quantity"] * df["price"]
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_quarter"] = df["order_date"].dt.quarter
    df["order_year"] = df["order_date"].dt.year
    df = df.groupby(["order_year", "order_quarter"]).agg({"quantity": "sum", "total_sales": "sum"}).reset_index()
    write_to_db(df, 'gold_sales_trends_quarterly')


# average sales amount per weather condition)
def process_sales_per_weather_condition():
    """
    Process sales per weather condition.

    This function loads data from the database, calculates the total sales by multiplying the quantity and price columns,
    groups the data by weather description, calculates the mean of total sales for each weather condition, and writes
    the result to the database.

    Returns:
        None
    """
    df = load_from_db(f"SELECT * FROM {TABLES_TO_CREATE['silver_sales']}")
    df["total_sales"] = df["quantity"] * df["price"]
    df = df.groupby(["weather_description"]).agg({"total_sales": "mean"}).reset_index()
    write_to_db(df, 'gold_sales_per_weather_condition')


dag = DAG('sales_pipeline', start_date=days_ago(1), schedule_interval=None)

test_connection = PythonOperator(dag=dag, task_id='test_connection', python_callable=test_connection)
# bronze layer
fetch_customer_from_api = PythonOperator(dag=dag, task_id='extract_customer_from_api', python_callable=fetch_transform_write)
fetch_weather_from_api = PythonOperator(dag=dag, task_id='extract_weather_from_api', python_callable=fetch_weather)
load_order = PythonOperator(dag=dag, task_id='load_orders_csv_file', python_callable=load_csv)

# silver layer
po_etl_sales = PythonOperator(dag=dag, task_id='etl_sales', python_callable=etl_sales)
# gold layer
po_process_total_sales = PythonOperator(dag=dag, task_id='process_total_sales', python_callable=process_total_sales)
po_process_avg_order_quantity = PythonOperator(dag=dag, task_id='process_avg_order_quantity', python_callable=process_avg_order_quantity)
po_process_top_selling_products = PythonOperator(dag=dag, task_id='process_top_selling_products', python_callable=process_top_selling_products)
po_process_top_selling_customers = PythonOperator(dag=dag, task_id='process_top_selling_customers', python_callable=process_top_selling_customers)
po_process_sales_trends_monthly = PythonOperator(dag=dag, task_id='process_sales_trends_monthly', python_callable=process_sales_trends_monthly)
po_process_sales_trends_quarterly = PythonOperator(dag=dag, task_id='process_sales_trends_quarterly', python_callable=process_sales_trends_quarterly)
po_process_sales_per_weather_condition = PythonOperator(dag=dag, task_id='process_sales_per_weather_condition', python_callable=process_sales_per_weather_condition)


test_connection >> fetch_customer_from_api >> fetch_weather_from_api >> load_order

load_order >> po_etl_sales >> [po_process_total_sales, po_process_avg_order_quantity, po_process_top_selling_products, po_process_top_selling_customers, po_process_sales_trends_monthly, po_process_sales_trends_quarterly, po_process_sales_per_weather_condition]
