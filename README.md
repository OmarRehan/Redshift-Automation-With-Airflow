# Redshift-Automation-With-Airflow
------
### Table of Contents:-
------
  1- Objective

  2- Prerequists

  3- Project Files & Packages

  4- DAG Steps/Components

  5- Setting up The Project

  6- Execution Steps

## 1. Objective:-
------
The Main objective of this Project are:
- Automate the Loading of redshift DWH created in this Project : https://github.com/OmarRehan/Redshift-DWH-IAC-and-Data-Pipeline, with minimum code changes. 
- Customize Airflow Components to generalize interfaces to handle the Execution of similar tasks.
- To use some of the beneficial Airflow functionalities, like Logging, web UI, Backfilling, retries, Code Encapsulation, Code Generalization, etc...

## 2. Prerequists:-
------
- Airflow Installed
- Redshift Cluster is Up, Schemas & Tables are created according to this Project : https://github.com/OmarRehan/Redshift-DWH-IAC-and-Data-Pipeline.

## 3. Project Files & Packages:-
------
- Sparkify.py : the Main DAG file 
- sparkify_queries.py : contains all SQL queries to load all the tables utilising the backfilling property in Airflow.
- custom_operators : A directory to conatin all custom operators
- copy_to_redshift_operator.py : Contains the main operator to load any JSON files from S# to redshift tables.
- load_fact.py : Contains the main operator to load fact tables. 
- load_dimension.py : Contains the main operator to load dimension tables. 
- data_quality.py : Contains a Generic Operator Checks data quality rules provided to it in a form of SQL.



## 4. DAG Steps/Components:-
---------
![Graph-View](https://user-images.githubusercontent.com/20134836/82099410-99419a00-9707-11ea-9118-7ad772680a9f.jpg)

- Begin_execution : A Dummy Operator to control the begining of the DAG.
- copy_songs_to_redshift : A copy_to_redshift_operator to load all songs files from S3 to redshift staging table.
- copy_logs_to_redshift : A copy_to_redshift_operator to load all logs files from S3 to redshift Staging table.
- dims_dag : A sub-DAG loads all the dimensions in Parallel using load_dimension operators.
![Sub-DAG](https://user-images.githubusercontent.com/20134836/82099439-abbbd380-9707-11ea-8d4a-5f7bf2fac136.png)

- Load songplay_fact : A load_fact task to load the Main Fact table of the schema.
- Quality_checks : A data_quality to check the Data quality rules provided to it in "data_quality_checks" Dinctionary in "sparkify_queries.py", then summarizes the output into the logs.



## 5. Setting up The Project:
--------------
1- Move "Sparkify.py" to the main directory from which Airflow imports its DAGs.

2- Move "sparkify_queries.py" to a directory with name "sparkify_sql" and add it to PYTHON_PATH.

3- create "custom_operators" directory in plugins directory of Airflow, and move all the custom operators files (copy_to_redshift_operator.py,load_fact.py,load_dimension.py,data_quality.py) in it.

4- Append the code in plugins.__init__.py to your __init__.py in plugins package if exists, if plugins package does not exist tthen move the whole directory to Airflow main directory.


## 6. Execution Steps:
check the default_args of the Main DAG adjust it to your requirements then exeute it using the Web UI of Airflow.
