# ETL_using_Apache_Airflow_Postgre

This Airflow DAG automates the ETL (Extract, Transform, Load) process for retrieving, transforming, and storing Amazon gaming laptop data in a PostgreSQL database. It also creates a summary report file and verifies its existence with a file sensor.

Workflow Steps:
Fetch Amazon Laptop Data (fetch_laptop_data):

Extraction: Scrapes gaming laptop data from Amazon's search results using requests and BeautifulSoup. The DAG fetches essential information, such as laptop title, price, rating, coupon status, expected delivery, and stock information.
Transformation: Cleans and deduplicates the data, converting it to a Pandas DataFrame, which is then pushed to Airflow's XCom for downstream tasks.
Create PostgreSQL Table (create_table):

A PostgresOperator creates a laptops table in a PostgreSQL database if it doesn't already exist. The table includes columns for title, price, rating, and other relevant laptop details, along with insertion and update timestamps.
Insert Data into PostgreSQL (insert_laptop_data):

Loading: The insert_laptop_data task pulls the cleaned data from XCom and inserts it into the PostgreSQL database. It uses ON CONFLICT to handle duplicates by updating existing records with new values, ensuring only the latest data is stored.
Count Inserts and Updates (count_inserts_updates):

Counts new records inserted and records updated on the current date. These counts are stored in XCom and used to generate a summary report.
Generate and Save Report (save_report_to_file):

Report Creation: Generates a report summarizing the total records processed, new records inserted, and records updated.
Save to File: Saves the report as a .txt file in the dags/reports/ directory. This task ensures the reports directory exists within the Airflow DAGs folder.
File Existence Check (check_report_file):

File Sensor: Uses a FileSensor to verify the existence of the generated report file (amazon_laptop_data_report.txt). The sensor checks every 10 seconds until it either confirms the file's existence or times out after 10 minutes.

Configuration and Execution:
The DAG uses default_args for Airflow settings, including the start date, retry logic, and retry delay.
conf.get("core", "dags_folder") fetches the Airflow DAGs folder path to store the report in a consistent location.
The file path for check_report_file in FileSensor aligns with the file path set in save_report_to_file.
This pipeline provides a complete ETL process, loading the transformed data into PostgreSQL and generating a file-based report, with validation by the FileSensor.
