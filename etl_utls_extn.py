import csv
import datetime
# from dotenv import load_dotenv
import glob
# from google.cloud import bigquery
import logging
import os
import pandas as pd
import requests
# from const import PATHS
import snowflake.connector
from snowflake.connector import connect
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer


# load_dotenv(".env")
ABSOLUTE_PATH = os.environ.get("ABSOLUTE_PATH")
CSV_WAREHOUSE_PATH = os.environ.get("CSV_WAREHOUSE_PATH")
SNOW_USER=os.environ.get("SNOW_USER")
SNOW_PASSWORD=os.environ.get("SNOW_PASSWORD")
SNOW_ACCOUNT=os.environ.get("SNOW_ACCOUNT")
SNOW_ROLE=os.environ.get("SNOW_ROLE")
SNOW_WAREHOUSE=os.environ.get("SNOW_WAREHOUSE")
SNOW_DATABASE=os.environ.get("SNOW_DATABASE")

# **********************************************************
# ****************** Google Bigquery IO ********************
# **********************************************************

# create a Google Big Query connection
# this relies on environmental variable GOOGLE_APPLICATION_CREDENTIALS which is loaded from the .env
def connect_bigquery():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./google_bigquery_credential.json"
    client = bigquery.Client()
    print("📺 Got Google bigquery client")
    return client


# Get data from Google Bigquery
def download_from_google_bigquery(sql, csv_filename_with_path=None):
    g_client = connect_bigquery()
    query_job = g_client.query(sql)
    print("🌍🌍 Executing query on google big query:")
    print(sql)
    df = query_job.to_dataframe()
    if csv_filename_with_path != None:
        df.to_csv(csv_filename_with_path, index=False)
    return df


# upsert data from Google Bigquery
def copy_from_google_bigquery_to_postgres(sql, table, csv_filename_with_path=None, use_upsert=False, key=None):
    """
    copy a source table in google big query using the sql into a target table in postgres.
    """
    print("⛳️⛳️⛳️ GBQ Upsert starting at " + str(datetime.datetime.now()))
    df = download_from_google_bigquery(sql)

    print("⛳️⛳️⛳️ loading into postgres")
    copy_from_df_to_snowflake(
        df=df, table=table, csv_filename_with_path=csv_filename_with_path, use_upsert=use_upsert, key=key
    )

####**********************************************************
#### ****************** Snowflake  ********************
#### **********************************************************

# Creating connection to snowflake
def connect_snowflake():
    try:
        conn = connect(
        user=SNOW_USER,
        password=SNOW_PASSWORD,
        account=SNOW_ACCOUNT,
        role=SNOW_ROLE

        )
        logging.info('Connection created successfully')
        return conn

    except:
        logging.warning('Connection creation unsuccessful')

def set_session():
    conn = connect_snowflake()
    cur = conn.cursor()
        #cursor = self.create_snowflake_connection().cursor(DictCursor) here the session is hardcoded
    try:
            #cursor.execute(f'USE ROLE {SNOW_ROLE};') Mostly for our purose without this we can fullfill our requirement
            #cursor.execute(f'USE WAREHOUSE {SNOW_WAREHOUSE};'
        sql = "use role ACCOUNTADMIN"
        cur.execute(sql)
        sql = "use warehouse COMPUTE_WH"  # But in future you want to move to different cluster use its name
        cur.execute(sql)
        sql = "use database ANALYTICS"
        cur.execute(sql)
        sql = "use schema PUBLIC"
        cur.execute(sql)
        return conn
    except Exception as error_returned:
        raise RuntimeError(f'Setting the Role and Warehouse threw error: {error_returned}' )


def query_snowflake(sql):
    # A cursor object represents a database cursor for execute and fetch operations
    conn = set_session()
    cur = conn.cursor()
    print("🌿🌿 executing query on snowflake:" + str(datetime.datetime.now()))
    try:
        # Prepare and execute a database command, fetch data into a Pandas DataFrame
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        if df.empty:
            print(f'DataFrame is empty!!!! for query {sql}')
        elif columns != None:
            data_rows = cur.fetchall()
            print(datetime.datetime.now())
            return pd.DataFrame(data_rows, columns=columns)
        conn.commit()
        print("🌿🌿 finished: " + str(datetime.datetime.now()))

    except Exception as error_returned:
        raise RuntimeError(
            f'SQL statement: {sql}\n threw error {error_returned}')

    finally:
        # Close the cursor
        cur.close()
        conn.close()

def copy_from_file_to_snowflake_table(csv_filename_with_path, table):
    conn = set_session()
    cur = conn.cursor()
    print(f"🌿 Loading {csv_filename_with_path} into snowflake stage at: " + str(datetime.datetime.now()))
    try:
        sql = 'put file://{0} @INNERCIRCLE auto_compress=false'.format(csv_filename_with_path)
        cur.execute(sql)
        print(f"🌿🌿 copying data from stage into snowflake {table}:" + str(datetime.datetime.now()))
        sql = 'copy into {0} from (select c.* from @innercircle (file_format => D_FILEFORMAT ) c) PURGE =TRUE'.format(table)
        cur.execute(sql)
        print(f"🌿🌿 copy data from stage into snowflake {table}:" + str(datetime.datetime.now()))

    finally:
        cur.close()
    conn.close()

def create_staging_table(table):
    staging_table = f"{table}_staging"
    query_snowflake(f"drop table if exists {staging_table}")
    query_snowflake(f"create table {staging_table} as select * from {table} limit 0")
    return staging_table

def copy_from_df_to_snowflake_table(df, table, csv_filename_with_path=None, use_upsert=False, key=None, update=False):
    print("""🌿 Load a python df into snowflake""")
    temp_file_will_be_removed = False

    # create a temp csv file name
    if csv_filename_with_path == None:
        now_str = str(datetime.datetime.now()).replace(" ", "_").replace(":", "_").replace(".", "_")
        csv_filename_with_path = ABSOLUTE_PATH + f"tmp_dataframe_{now_str}.csv"
        temp_file_will_be_removed = True

    df.to_csv(csv_filename_with_path, index=False)
    if use_upsert:
        if key == None:
            raise ValueError("🤯 executing upsert without providing key")
#         upsert_from_file_to_postgres(csv_filename_with_path=csv_filename_with_path, table=table, key=key, update=update)
    else:
        copy_from_file_to_snowflake_table(csv_filename_with_path=csv_filename_with_path, table=table)

    if temp_file_will_be_removed:
        os.remove(csv_filename_with_path)
def update_snowflake(source, target, key):
    df = query_snowflake(sql=f'''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema ilike 'public'
            AND table_name ilike '{target}'
            and column_name != '{key}'
        ;'''
        , columns = ['col'])
    s = str(df.col.apply(lambda x: f'{x} = s.{x}').to_list())
    sql_set_string = s.replace("'", '').replace('[','').replace(']','')
    query_snowflake(sql=f'''
    update {target}
    set
    {sql_set_string}
    from {source} s
        where {target}.{key} = s.{key};
    ''')

def exports_from_snowflake_to_s3(sql, table):
    conn = set_session()
    cur = conn.cursor()
    print(f"🌿 Loading snowflake query results into s3/{table}: " + str(datetime.datetime.now()))
    try:
        sql = "copy into '@gets3data/{0}/' from ({1})  file_format=(type=csv COMPRESSION =  NONE)HEADER = TRUE OVERWRITE = FALSE SINGLE = TRUE".format(table, sql)
        cur.execute(sql)

    finally:
        cur.close()
    conn.close()