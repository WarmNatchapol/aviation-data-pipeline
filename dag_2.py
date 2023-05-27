# import libraries
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
import requests
import json
from datetime import datetime
import pytz


# Thai timezone
thai_tz = pytz.timezone("Asia/Bangkok")

# define fetch data from API function
def fetch_data_api():
    # get API Key from variable
    api_key = Variable.get("API_KEY")
    # api url
    url = f"https://airlabs.co/api/v9/flights?api_key={api_key}&airline_icao=THA"
    # response from api
    response = requests.get(url)
    # convert json to dictionary
    result = response.json()
    # get live flight data
    live_flight = result["response"]

    # convert to json
    live_flight_json = json.dumps(live_flight)
    # current date
    current_date = datetime.now(thai_tz).strftime("%Y-%m-%d")
    # current datetime
    current_datetime = datetime.now(thai_tz).strftime("%Y%m%d_%H%M")

    # write raw data to json file
    with open(f"live_flight_{current_datetime}.json", "w") as f:
        f.write(live_flight_json)

    # get s3 hook (connect to MinIO)
    s3_hook = S3Hook(aws_conn_id="minio")
    # load raw data json file to MinIO
    s3_hook.load_file(
        f"live_flight_{current_datetime}.json",
        key=f"{current_date}/{current_datetime}.json",
        bucket_name="aviation-project",
        replace=True
    )


# define download raw data from MinIO function 
def download_raw_data(**context):
    # current date
    current_date = datetime.now(thai_tz).strftime("%Y-%m-%d")
    # current datetime
    current_datetime = datetime.now(thai_tz).strftime("%Y%m%d_%H%M")

    # get s3 hook (MinIO)
    s3_hook = S3Hook(aws_conn_id="minio")
    # download raw data
    filename = s3_hook.download_file(
        key=f"{current_date}/{current_datetime}.json",
        bucket_name="aviation-project"
    )

    # push filename to xcom
    context["ti"].xcom_push(key="filename", value=filename)


# define convert unix time to datetime with Thai timezone function
def convert_dt(unix_time):
    return datetime.fromtimestamp(unix_time, thai_tz).strftime("%Y-%m-%d %H:%M:%S")


# define transform data and load to MongoDB function
def transform_load_database(**context):
    # get PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    # get connection
    conn = postgres_hook.get_conn()
    # get cursor
    cur = conn.cursor()

    # pull filename from xcom
    filename = context["ti"].xcom_pull(key="filename")

    # execute SQL query - select reg, aircraft_type, and remark from fleet table in Postgres
    cur.execute("SELECT reg, aircraft_type, remark FROM fleet;")
    # fetch result
    fleet_result = cur.fetchall()
    # write result as a pandas dataframe
    fleet_df = pd.DataFrame(fleet_result, columns=["reg", "aircraft_type", "remark"])

    # execute SQL query - select flight, dep_airport_icao, dep_airport_name, dep_airport_country, arr_airport_icao, arr_airport_name, and arr_airport_country from route table in Postgres
    cur.execute("SELECT flight, dep_airport_icao, dep_airport_name, dep_airport_country, arr_airport_icao, arr_airport_name, arr_airport_country FROM route;")
    # fetch result
    route_result = cur.fetchall()
    # write result as a dataframe
    route_df = pd.DataFrame(route_result, columns=["flight", "dep_airport_icao", "dep_airport_name", "dep_airport_country", "arr_airport_icao", "arr_airport_name", "arr_airport_country"])

    # read raw data json file
    with open(filename, "r") as f:
        flight_data = json.load(f)

    # create empty live flight result dictionary
    result_dict = {}

    # for loop to get needed live data from raw data json file
    for index, flight in enumerate(flight_data):
        result_dict[index] = {
            "reg": flight["reg_number"],
            "flight": flight["flight_icao"],
            "status": flight["status"],
            "updated": convert_dt(flight["updated"])
        }

    # convert live flight result dictionary to pandas dataframe
    flight_df = pd.DataFrame(result_dict).transpose()

    # merge 3 tables - fleet dataframe left join with live flight dataframe and then left join with route dataframe
    merged_df = fleet_df.merge(flight_df, how="left", on="reg").merge(route_df, how="left", on="flight")
    # convert to dictionary
    merged_dict = merged_df.to_dict("reg")

    # get MongoDB hook
    hook = MongoHook(mongo_conn_id="mongo_default")
    # get connection
    client = hook.get_conn()
    # get database
    db = client["aviation-project"]
    # get collection
    flight=db["flight"]

    # if MongoDB is empty
    if db.flight.count_documents({}) == 0:
        # execute insert many to MongoDB
        flight.insert_many(merged_dict)
    # else - MongoDB already contains data
    else:
        # for loop to update them
        for row in merged_dict:
            query = {"reg": row["reg"]}
            values = {"$set": {
                "aircraft_type": row["aircraft_type"],
                "flight": row["flight"], 
                "status": row["status"],
                "updated": row["updated"],
                "dep_airport_icao": row["dep_airport_icao"], 
                "dep_airport_name": row["dep_airport_name"], 
                "dep_airport_country": row["dep_airport_country"], 
                "arr_airport_icao": row["arr_airport_icao"], 
                "arr_airport_name": row["arr_airport_name"], 
                "arr_airport_country": row["arr_airport_country"],
                "remark": row["remark"]
            }}
            flight.update_one(query, values)


# default arguments
default_args = {
    "owner": "WarmNatchapol",
    "start_date": datetime(2023, 5, 27, tzinfo=thai_tz)
}

# DAG
with DAG(
    "Aviation_DAG_2",
    default_args = default_args,
    schedule_interval = "@hourly"
) as dag:

    # fetch live flight data from API and load to MinIO
    fetch_data_api = PythonOperator(
        task_id="fetch_data_api",
        python_callable=fetch_data_api
    )

    # download raw data from MinIO
    download_raw_data = PythonOperator(
        task_id="download_raw_data",
        python_callable=download_raw_data
    )

    # Fetch data from Postgres and merge with live flight data, then load to MongoDB
    transform_load_database = PythonOperator(
        task_id="transform_load_database",
        python_callable=transform_load_database
    )

    # define dependencies
    fetch_data_api >> download_raw_data >> transform_load_database
