# import libraries
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import mysql.connector


# get mysql connection data from environment variables
class Config:
    load_dotenv()

    mysql_host = os.environ.get("MYSQL_HOST")
    mysql_database = os.environ.get("MYSQL_DATABASE")
    mysql_username = os.environ.get("MYSQL_USERNAME")
    mysql_password = os.environ.get("MYSQL_PASSWORD")
    mysql_port = os.environ.get("MYSQL_PORT")


# define read data from csv file function
def read_data(name):
    # get current date
    current_date = datetime.now().strftime("%Y%m%d")
    # read csv file
    df = pd.read_csv(f"{name}_{current_date}.csv")

    return df


# define create fleet and route table in MySQL function
def create_table_mysql():
    # connect to MySQL
    conn = mysql.connector.connect(
        host = Config.mysql_host,
        database = Config.mysql_database,
        user = Config.mysql_username,
        password = Config.mysql_password
    )
    # get cursor
    cur = conn.cursor()

    # SQL of create fleet table
    create_fleet_table = """
        CREATE TABLE IF NOT EXISTS fleet (
            reg VARCHAR(6),
            icao24 CHAR(6),
            aircraft_type VARCHAR(20),
            aircraft_type_resign CHAR(4),
            aircraft_name VARCHAR(50),
            delivered DATE,
            age_years FLOAT(3),
            remark VARCHAR(20),
            PRIMARY KEY (reg)
        )
    """

    # SQL of create route table
    create_route_table = """
        CREATE TABLE IF NOT EXISTS route (
            flight VARCHAR(7),
            schedule DATETIME,
            dep_airport_icao CHAR(4),
            dep_airport_name VARCHAR(50),
            dep_airport_country VARCHAR(20),
            arr_airport_icao CHAR(4),
            arr_airport_name VARCHAR(50),
            arr_airport_country VARCHAR(20),
            expected_aircraft_type CHAR(4),
            PRIMARY KEY (flight)
        )
    """

    # execute create fleet table SQL
    cur.execute(create_fleet_table)
    conn.commit()

    # execute create route table SQL
    cur.execute(create_route_table)
    conn.commit()

    # close cursor
    cur.close()
    # close connection
    conn.close()


# define insert data into MySQL function
def insert_to_mysql(fleet, route):
    # connect to MySQL
    conn = mysql.connector.connect(
        host = Config.mysql_host,
        database = Config.mysql_database,
        user = Config.mysql_username,
        password = Config.mysql_password
    )
    # get cursor
    cur = conn.cursor()

    # SQL insert data into fleet table
    insert_fleet_data = """
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
        ON DUPLICATE KEY UPDATE
            icao24 = icao24, 
            aircraft_type = aircraft_type, 
            aircraft_type_resign = aircraft_type_resign, 
            aircraft_name = aircraft_name, 
            delivered = delivered, 
            age_years = age_years, 
            remark = remark
    """

    # SQL insert data into route table
    insert_route_data = """
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
        ON DUPLICATE KEY UPDATE
            schedule = schedule, 
            dep_airport_icao = dep_airport_icao, 
            dep_airport_name = dep_airport_name, 
            dep_airport_country = dep_airport_country, 
            arr_airport_icao = arr_airport_icao, 
            arr_airport_name = arr_airport_name, 
            arr_airport_country = arr_airport_country, 
            expected_aircraft_type = expected_aircraft_type
    """

    # for loop to execute SQL insert data into fleet table
    for row in fleet.values:
        cur.execute(insert_fleet_data, tuple(row))
    conn.commit()

    # for loop to execute SQL insert data into route table
    for row in route.values:
        cur.execute(insert_route_data, tuple(row))
    conn.commit()

    # close cursor
    cur.close()
    # close connection
    conn.close()


# call read data function
fleet = read_data("fleet")
route = read_data("route")

# convert data type to datetime
fleet["DELIVERED"] = pd.to_datetime(fleet["DELIVERED"])
route["SCHEDULE"] = pd.to_datetime(route["SCHEDULE"])

# call create table function
create_table_mysql()
# call insert data to MySQL function
insert_to_mysql(fleet=fleet, route=route)
