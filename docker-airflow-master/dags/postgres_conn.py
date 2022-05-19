from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


def get_data():
   
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    del_sql = "delete from weather"
    cur.execute(del_sql)
    conn.commit()

    copy_sql = """
            COPY weather FROM stdin WITH CSV HEADER DELIMITER AS ','
            """
    with open("/usr/local/airflow/weather_csv/weather_csv_file.csv", "r") as file:
        cur.copy_expert(sql=copy_sql, file=file)
    conn.commit()
    cur.close()