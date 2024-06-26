from dataclasses import dataclass
import logging

import pymysql
from pymysql.connections import Connection
from pymysql.cursors import DictCursor
from ColoredOutput import ColoredOutput
from contextlib import closing
from apscheduler.schedulers.background import BackgroundScheduler
from time import time_ns

from models.future_data import FutureData
from models.tax_data import TaxData
from exchanges import Exchange


logging.basicConfig(format='[%(asctime)s] %(name)s %(levelname)-8s %(message).10240s', level='INFO')


# import time

# def timeit(func):
#     def wrapper(*args, **kwargs):
#         start_time = time.time()
#         result = func(*args, **kwargs)
#         end_time = time.time()
#         print(f"Function '{func.__name__}' executed in {end_time - start_time} seconds.")
#         return result
#     return wrapper


def create_spot_db_connection(db_config, table_name):
    return create_db_connection(db_config, table_name, table_structure=f"""
                                                                            CREATE TABLE IF NOT EXISTS {table_name} (
                                                                                token VARCHAR(32),
                                                                                bidPrice FLOAT,
                                                                                askPrice FLOAT,
                                                                                volume_24h BIGINT,
                                                                                time BIGINT
                                                                                )""")


def create_futures_db_connection(db_config, table_name):
    return create_db_connection(db_config, table_name, table_structure=f"""
                                                                            CREATE TABLE IF NOT EXISTS {table_name} (
                                                                                token VARCHAR(32),
                                                                                funding_annual_percent FLOAT, 
                                                                                nextFundingTime BIGINT,
                                                                                funding_period INT,
                                                                                bidPrice FLOAT,
                                                                                askPrice FLOAT,
                                                                                volume_24h BIGINT,
                                                                                time_funding_refresh BIGINT,
                                                                                time_bid_ask_refresh BIGINT,
                                                                                time_insert BIGINT,
                                                                                openInterest float,
                                                                                time_openInterest_refresh bigint
                                                                                )""")


def create_db_connection(db_config, table_name, table_structure):
    logging.info(str(table_structure))
    # return

    connection = pymysql.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        db=db_config['db']
    )

    with closing(connection.cursor()) as cursor:
        cursor.execute(table_structure)
        connection.commit()

    ColoredOutput.green(
        f"DB connection {db_config['user']}:{db_config['host']}/{db_config['db']} table: {table_name} has been established!")

    return connection


def insert_or_update_spot_thread(connection, table_name, data):
    # Create an instance of BackgroundScheduler
    scheduler = BackgroundScheduler()

    # Add jobs to the scheduler to be executed every minute at specific seconds
    # Each job passes a different argument to the function
    scheduler.add_job(insert_or_update_spot, 'cron', minute='*/5', second=5, max_instances=1, coalesce=True,
                      args=[connection, table_name, data])

    # Start the scheduler
    scheduler.start()


def insert_or_update_spot(connection, table_name, data):
    # for row in data.values():
    #     logging.info(str(row))
    # # return
    values_list = [
        (
            values['token'],
            values['bidPrice'],
            values['askPrice'],
            values['volume24h'],
            values['time']
        )
        for _, values in data.items()
    ]

    query = f"""
        INSERT INTO {table_name} (token, bidPrice, askPrice, volume_24h, time) 
        VALUES (%s, %s, %s, %s, %s)
        """

    with closing(connection.cursor()) as cursor:
        cursor.executemany(query, values_list)
        connection.commit()


def insert_or_update_futures_thread(connection, table_name, data):
    # Create an instance of BackgroundScheduler
    scheduler = BackgroundScheduler()

    # Add jobs to the scheduler to be executed every minute at specific seconds
    # Each job passes a different argument to the function
    scheduler.add_job(insert_or_update_futures, 'cron', minute='*/5', second=5, max_instances=1, coalesce=True,
                      args=[connection, table_name, data])

    # Start the scheduler
    scheduler.start()


@dataclass
class FutureData:
    token: str
    funding_annual_percent: float
    nextFundingTime: int
    funding_period: int
    bidPrice: float
    askPrice: float
    volume_24h: float
    time_funding_refresh: int
    time_bid_ask_refresh: int
    time_insert: int
    openInterest: float
    time_openInterest_refresh: int


def read_last_table_data(connection: Connection, exchange: Exchange) -> dict[str, FutureData]:
    fields = ', '.join(FutureData.__dataclass_fields__)
    sql = f"""
        select {fields} from `{exchange.table_name}`
        where time_insert=(SELECT max(time_insert) FROM `{exchange.table_name}`)  
    """
    with closing(connection.cursor(DictCursor)) as cursor:
        cursor.execute(sql)
        rows = {
            exchange.token2coin(row['token']): FutureData(**row) for row in cursor.fetchall()
        }
        connection.commit()
    return rows


def read_last_tax_table_data(connection: Connection, exchange: Exchange) -> dict[str, TaxData]:
    fields = ', '.join(TaxData.__dataclass_fields__)
    sql = f"""
        select {fields} from `{exchange.tax_table_name}`
        where timestamp=(SELECT max(timestamp) FROM `{exchange.tax_table_name}`)  
    """
    with closing(connection.cursor(DictCursor)) as cursor:
        cursor.execute(sql)
        rows = {
            row['token']: TaxData(**row) for row in cursor.fetchall()
        }
        connection.commit()
    return rows


def insert_or_update_futures(connection, table_name, data):
    # for row in data.values():
    #     logging.info(str(row))
    # # return
    time_insert = time_ns() // 1_000_000

    values_list = [
        (
            values['token'],
            values['funding_annual_percent'],
            values['nextFundingTime'],
            values['funding_period'],
            values['bidPrice'],
            values['askPrice'],
            values['volume24h'],
            values['time_funding_refresh'],
            values['time_bid_ask_refresh'],
            time_insert,
            values['openInterest'],
            values['time_openInterest_refresh']
        )
        for _, values in data.items()
    ]

    query = f"""
        INSERT INTO {table_name} (token, funding_annual_percent, nextFundingTime, funding_period, bidPrice, askPrice, volume_24h, time_funding_refresh, time_bid_ask_refresh, time_insert, openInterest, time_openInterest_refresh) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    with closing(connection.cursor()) as cursor:
        cursor.executemany(query, values_list)
        connection.commit()
