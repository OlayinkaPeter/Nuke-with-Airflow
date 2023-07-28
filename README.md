# Loading data from the employees' database at https://dev.mysql.com/doc/employee/en/ into BigQuery

The steps are as follows: 
 

1.   Connect to the local Employees' database already downloaded from https://dev.mysql.com/doc/employee/en/
2.   Initialise a BigQuery client
3.   Query employees table and transform gender values
4.   Construct the fully-qualified BigQuery table name
5.   Load data into BigQuery
6.   Build Dag
7.   Initialise a PythonOperator with the above function as a Task
8.   Declare the task


<br>


### What the database looks like
<p align="center">
<img src="https://github.com/OlayinkaPeter/airflow_/blob/main/images/db.png">
</p>

#### For the first dag, the query is
``` sql
SELECT 
    emp_no, birth_date, first_name, last_name, 
    CASE gender WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE gender END AS gender, 
    hire_date 
FROM employees
```

#### For the second dag to update bigquery daily, the query is
``` sql
SELECT 
    emp_no, birth_date, first_name, last_name, 
    CASE gender WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE gender END AS gender, 
    hire_date 
FROM employees
WHERE hire_date >= '{datetime.now() - timedelta(days=1).strftime('%Y-%m-%d')}'
```


### What my dag list looks like
<p align="center">
<img src="https://github.com/OlayinkaPeter/airflow_/blob/main/images/dags.png">
</p>
