from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from airflow.configuration import conf
from airflow.sensors.filesystem import FileSensor

#1) fetch amazon data (extract) 2) clean data (transform)

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

def get_amazon_data_laptops(num_laptops, ti):
    # Base URL of Amazon search results for gaming laptops
    base_url = f"https://www.amazon.com/s?k=gaming+laptops"

    laptops = []
    seen_titles = set()  # To keep track of seen titles

    page = 1

    while len(laptops) < num_laptops:
        url = f"{base_url}&page={page}"
        
        # Send a request to the URL
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the content of the request with BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Find laptop containers
            laptop_containers = soup.find_all("div", {"class": "s-result-item"})
            
            # Loop through the laptop containers and extract data
            for laptop in laptop_containers:
                title = laptop.find("span", {"class": "a-text-normal"})
                price = laptop.find("span", {"class": "a-offscreen"})
                rating = laptop.find("span", {"class": "a-icon-alt"})
                number_of_ratings = laptop.find("span", {"class": "a-size-base s-underline-text"})
                #purchase_history = laptop.find("div", {"class": "a-row a-size-base"})
                coupon = laptop.find("span",{"class": "s-coupon-clipped aok-hidden"})
                expected_delivery = laptop.find("span",{"class": "a-color-base a-text-bold"})
                remaining_stock = laptop.find("span",{"class": "a-size-base a-color-price"})
                
                # Check if essential elements are present
                if title and price and rating:
                    laptop_title = title.text.strip()
                    
                    # Check if title has been seen before
                    if laptop_title not in seen_titles:
                        seen_titles.add(laptop_title)
                        laptops.append({
                            "Title": laptop_title,
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                            "Number of Ratings": number_of_ratings.text.strip() if number_of_ratings else "N/A",
                            #"Purchase History": purchase_history.text.strip() if purchase_history else "N/A",
                            "Coupon": coupon.text.strip() if coupon else "N/A",
                            "Expected Delivery": expected_delivery.text.strip() if expected_delivery else "N/A",
                            "Remaining Stock": remaining_stock.text.strip() if remaining_stock else "N/A"
                        })
            
            # Increment the page number for the next iteration
            page += 1
        else:
            print("Failed to retrieve the page")
            break

    # Limit to the requested number of laptops
    laptops = laptops[:num_laptops]
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(laptops)
    
    # Remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)
    
    # Push the DataFrame to XCom
    ti.xcom_push(key='laptop_data', value=df.to_dict('records'))

#3) create and store data in table on postgres (load)
    
def insert_laptop_data_into_postgres(ti):
    laptop_data = ti.xcom_pull(key='laptop_data', task_ids='fetch_laptop_data')
    if not laptop_data:
        raise ValueError("No laptop data found")

    postgres_hook = PostgresHook(postgres_conn_id='laptops_connection')

    for laptop in laptop_data:
        query = """
        INSERT INTO laptops (title, price, rating, number_of_ratings, coupon, expected_delivery, remaining_stock, ins_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (title) DO UPDATE 
        SET price = EXCLUDED.price,
            rating = EXCLUDED.rating,
            number_of_ratings = EXCLUDED.number_of_ratings,
            coupon = EXCLUDED.coupon,
            expected_delivery = EXCLUDED.expected_delivery,
            remaining_stock = EXCLUDED.remaining_stock,
            upd_timestamp = CURRENT_TIMESTAMP;
        """
        postgres_hook.run(sql=query, parameters=(
            laptop['Title'], 
            laptop['Price'], 
            laptop['Rating'], 
            laptop['Number of Ratings'], 
            laptop['Coupon'], 
            laptop['Expected Delivery'], 
            laptop['Remaining Stock']
        ))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 29),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'fetch_and_store_amazon_laptops',
    default_args=default_args,
    description='A Simple DAG to fetch laptop data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1)
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_laptop_data_task = PythonOperator(
    task_id='fetch_laptop_data',
    python_callable=get_amazon_data_laptops,
    op_args=[2],  # Number of laptops to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='laptops_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS laptops (
        title TEXT PRIMARY KEY UNIQUE NOT NULL,
        price TEXT,
        rating TEXT,
        number_of_ratings TEXT,
        coupon TEXT,
        expected_delivery TEXT,
        remaining_stock TEXT,
        ins_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        upd_timestamp TIMESTAMP
    );
    """,##id SERIAL PRIMARY KEY,
    dag=dag,
)

insert_laptop_data_task = PythonOperator(
    task_id='insert_laptop_data',
    python_callable=insert_laptop_data_into_postgres,
    dag=dag,
)

# Helper function to count inserts and updates
def count_inserts_updates(ti):
    postgres_hook = PostgresHook(postgres_conn_id='laptops_connection')
    query = """
    SELECT
        COUNT(*) AS total,
        COUNT(CASE WHEN DATE_TRUNC('day', ins_timestamp) = CURRENT_DATE THEN 1 END) AS inserts,
        COUNT(CASE WHEN DATE_TRUNC('day', upd_timestamp) = CURRENT_DATE THEN 1 END) AS updates
    FROM laptops
    """
    records = postgres_hook.get_first(query)
    if records:
        ti.xcom_push(key='count_report', value={'total': records[0], 'inserts': records[1], 'updates': records[2]})
    else:
        ti.xcom_push(key='count_report', value={'total': 0, 'inserts': 0, 'updates': 0})

# Task to count inserts and updates
count_inserts_updates_task = PythonOperator(
    task_id='count_inserts_updates',
    python_callable=count_inserts_updates,
    dag=dag,
)

def save_report_to_file(ti):
    # Retrieve the report from XCom
    report = ti.xcom_pull(task_ids='count_inserts_updates', key='count_report') or {}
    
    # Create the report content
    report_content = f"""
    Amazon Laptop Data Load Report:
    Total Records Processed: {report.get('total', 0)}
    New Records Inserted: {report.get('inserts', 0)}
    Records Updated: {report.get('updates', 0)}
    """
    
    # Define the file path within the Airflow DAGs folder
    dags_folder = conf.get("core", "dags_folder")
    file_path = os.path.join(dags_folder, 'reports', 'amazon_laptop_data_report.txt')
    
    # Ensure the 'reports' directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Write the report to the file
    with open(file_path, 'w') as file:
        file.write(report_content)
    
    print(f"Report saved to {file_path}")

save_report_to_file_task = PythonOperator(
    task_id='save_report_to_file',
    python_callable=save_report_to_file,
    dag=dag,
)

# Define the file path within the Airflow DAGs folder (make sure it matches the save_report_to_file function)
dags_folder = conf.get("core", "dags_folder")
file_path = os.path.join(dags_folder, 'reports', 'amazon_laptop_data_report.txt')

# Add FileSensor to check for the existence of the report file
check_report_file_task = FileSensor(
    task_id='check_report_file',
    filepath=file_path,
    fs_conn_id='fs_default',  # Default filesystem connection, adjust if necessary
    poke_interval=10,         # Interval to check for the file (in seconds)
    timeout=600,              # Timeout (in seconds)
    mode='poke',              # `poke` mode, will repeatedly check until the file appears or times out
    dag=dag,
)

#dependencies

fetch_laptop_data_task >> create_table_task >> insert_laptop_data_task >> count_inserts_updates_task >> save_report_to_file_task >> check_report_file_task