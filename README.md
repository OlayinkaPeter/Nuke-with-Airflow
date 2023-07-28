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


### What my dag list looks like
<p align="center">
<img src="https://github.com/OlayinkaPeter/airflow_/blob/main/images/dag.png">
</p>
