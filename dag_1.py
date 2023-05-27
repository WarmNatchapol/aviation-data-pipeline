# import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pytz


# Thai timezone
thai_tz = pytz.timezone("Asia/Bangkok")

# define extract data from MySQL database and load to PostgreSQL database
def mysql_to_postgres():
    # hook MySQL connection
    mysql = MySqlHook("mysql")

    # select all data from MySQL as a dataframe
    fleet_data = mysql.get_pandas_df(sql = "SELECT * FROM fleet;")
    route_data = mysql.get_pandas_df(sql = "SELECT * FROM route;")

    # hook Postgres connection
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    # get connection
    conn = postgres_hook.get_conn()
    # get cursor
    cur = conn.cursor()

    # SQL of insert data into fleet table
    fleet_sql = """
        INSERT INTO fleet (
            reg, 
            icao24, 
            aircraft_type, 
            aircraft_type_resign, 
            aircraft_name, 
            delivered, 
            age_years, 
            remark
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (reg) 
        DO UPDATE SET
            icao24 = EXCLUDED.icao24,
            aircraft_type = EXCLUDED.aircraft_type,
            aircraft_type_resign = EXCLUDED.aircraft_type_resign,
            aircraft_name = EXCLUDED.aircraft_name, 
            delivered = EXCLUDED.delivered, 
            age_years = EXCLUDED.age_years, 
            remark = EXCLUDED.remark
    """

    # SQL of insert data into route table
    route_sql = """
        INSERT INTO route (
            flight, 
            schedule, 
            dep_airport_icao, 
            dep_airport_name, 
            dep_airport_country, 
            arr_airport_icao, 
            arr_airport_name, 
            arr_airport_country, 
            expected_aircraft_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (flight)
        DO UPDATE SET
            schedule = EXCLUDED.schedule, 
            dep_airport_icao = EXCLUDED.dep_airport_icao, 
            dep_airport_name = EXCLUDED.dep_airport_name, 
            dep_airport_country = EXCLUDED.dep_airport_country, 
            arr_airport_icao = EXCLUDED.arr_airport_icao, 
            arr_airport_name = EXCLUDED.arr_airport_name, 
            arr_airport_country = EXCLUDED.arr_airport_country, 
            expected_aircraft_type = EXCLUDED.expected_aircraft_type
    """

    # for loop to execute SQL insert data into fleet table
    for row in fleet_data.values:
        cur.execute(fleet_sql, tuple(row))
    conn.commit()

    # for loop to execute SQL insert data into route table
    for row in route_data.values:
        cur.execute(route_sql, tuple(row))
    conn.commit()


# default arguments
default_args = {
    "owner": "WarmNatchapol",
    "start_date": datetime(2023, 5, 27, tzinfo=thai_tz)
}

# DAG
with DAG(
    "Aviation_DAG_1",
    default_args = default_args,
    schedule_interval = "@daily"
) as dag:

    # create tables in PosgreSQL database task
    create_table_postgres = PostgresOperator(
        task_id = "create_table_postgres",
        postgres_conn_id = "postgres",
        sql = """
            CREATE TABLE IF NOT EXISTS fleet (
            reg VARCHAR(6) PRIMARY KEY,
            icao24 CHAR(6),
            aircraft_type VARCHAR(20),
            aircraft_type_resign CHAR(4),
            aircraft_name VARCHAR(50),
            delivered DATE,
            age_years FLOAT(3),
            remark VARCHAR(20)
            );

            CREATE TABLE IF NOT EXISTS route (
            flight VARCHAR(7) PRIMARY KEY,
            schedule TIMESTAMP,
            dep_airport_icao CHAR(4),
            dep_airport_name VARCHAR(50),
            dep_airport_country VARCHAR(20),
            arr_airport_icao CHAR(4),
            arr_airport_name VARCHAR(50),
            arr_airport_country VARCHAR(20),
            expected_aircraft_type CHAR(4)
            );
        """,
    )

    # extract data from MySQL and load to PostgreSQL database task
    mysql_to_postgres = PythonOperator(
        task_id = "mysql_to_postgres",
        python_callable = mysql_to_postgres
    )

    # define depedencies
    create_table_postgres >> mysql_to_postgres
