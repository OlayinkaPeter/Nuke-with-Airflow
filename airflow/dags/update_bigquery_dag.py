from airflow import DAG
from airflow.operators.python import PythonOperator
import mysql.connector
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime, timedelta


DB_HOST = Variable.get("DB_HOST")
DB_USER = Variable.get("DB_USER")
DB_PASSWORD = Variable.get("DB_PASSWORD")
DB_DATABASE = Variable.get("DB_DATABASE")

GCP_PROJECT = Variable.get("GCP_PROJECT")
BQ_DATASET = Variable.get("BQ_DATASET")
BQ_DATASET_TABLE = Variable.get("BQ_DATASET_TABLE")


def update_employees_to_bigquery():
    # Connect to the local Employees' database already downloaded from https://dev.mysql.com/doc/employee/en/
    employees_db = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )
    mysql_cursor = employees_db.cursor()

    # initialise a BigQuery client
    client = bigquery.Client()

    # Calculate the timestamp for one day ago
    one_day_ago = datetime.now() - timedelta(days=1)
    one_day_ago_str = one_day_ago.strftime('%Y-%m-%d')

    # Query employees table and transform gender values
    mysql_query = f"SELECT emp_no, first_name, last_name, CASE gender WHEN 'M' THEN 'Male' WHEN 'F' " \
                  f"THEN 'Female' ELSE gender END AS gender, hire_date FROM employees " \
                  f"WHERE hire_date >= '{one_day_ago_str}'"
    mysql_cursor.execute(mysql_query)
    results = mysql_cursor.fetchall()

    data_to_load = [
        {
            "emp_no": str(emp_no),
            "birth_date": str(birth_date),
            "first_name": first_name,
            "last_name": last_name,
            "gender": gender,
            "hire_date": str(hire_date)
        }
        for emp_no, birth_date, first_name, last_name, gender, hire_date in results
    ]

    # Construct the fully-qualified BigQuery table name
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_DATASET_TABLE}"

    # Load data into BigQuery
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("emp_no", "INTEGER"),
        bigquery.SchemaField("birth_date", "DATE"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("hire_date", "DATE"),
    ], write_disposition="WRITE_TRUNCATE")

    # job = client.load_table_from_json(data_to_load, table_ref, job_config=job_config)
    # job.result()

    errors = client.insert_rows_json(table_ref, data_to_load)
    if not errors:
        print("Data loaded into BigQuery table for changes in the past day successfully.")


with DAG(dag_id="update_employees_to_bigquery",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:

    task_update_employees_to_bigquery = PythonOperator(
        task_id="update_employees_to_bigquery",
        python_callable=update_employees_to_bigquery)

task_update_employees_to_bigquery
