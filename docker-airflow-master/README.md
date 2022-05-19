
Weather Data Assignment Using Airflow

In this assignment i am going to fetch the weather related data from 10 states across the india using API's and then store those data into Database
table. To perform all such operations Airflow is used which will create multiple task and execute as DAG based on given dependencies.


Python files
1. dags/store_DAG.py
Dag file with three tasks
t1 - fetch data from weather API and saves it to CSV file.
t2 - create weather table based on required schema.
t3 - insert data into table from csv file

2. dags/fetchDataAndStoreInCSV.py
fetch data from weather API
Gets data of 10 states and saves it in csv
Headers: state, description, temp, feels_like, min_temp, max_temp, humidity, clouds

CSV file location - weather_csv/

SQL script location - weather_sql/


Docker compose file
docker-compose-LocalExecutor.yml (type - LocalExecutor)


Services:
1. postgres
2. webserver - puckel/docker-airflow

