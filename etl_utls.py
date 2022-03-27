import csv
import datetime
from dotenv import load_dotenv
import glob
from google.cloud import bigquery
import os
import pandas as pd
import psycopg2 as pg2
import json
import requests
from const import PATHS
import re
import snowflake.connector
from snowflake.connector import connect
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer

load_dotenv(".env")
ABSOLUTE_PATH = os.environ.get("ABSOLUTE_PATH")
CSV_WAREHOUSE_PATH = os.environ.get("CSV_WAREHOUSE_PATH")
SNOW_USER=os.environ.get("SNOW_USER")
SNOW_PASSWORD=os.environ.get("SNOW_PASSWORD")
SNOW_ACCOUNT=os.environ.get("SNOW_ACCOUNT")
SNOW_ROLE=os.environ.get("SNOW_ROLE")
SNOW_WAREHOUSE=os.environ.get("SNOW_WAREHOUSE")
SNOW_DATABASE=os.environ.get("SNOW_DATABASE")
RUNNING_IN_CLOUD = os.environ.get("RUNNING_IN_CLOUD") == "True"
OPENSEA_API_KEY = os.environ.get("OPENSEA_API_KEY")
OPENSEA_V1_ABI_FILENAME = os.environ.get("OPENSEA_V1_ABI_FILENAME")

# **********************************************************
# ****************** Postgres IO ***************************
# **********************************************************

if RUNNING_IN_CLOUD:
    postgres_conn_params = {
        "host": os.environ.get("REMOTE_PSQL_ADDRESS"),
        "database": os.environ.get("REMOTE_PSQL_DB_NAME"),
        "user": os.environ.get("REMOTE_PSQL_USERNAME"),
        "password": os.environ.get("REMOTE_PSQL_PASSWORD"),
    }
else:
    postgres_conn_params = {
        "host": "localhost",  #
        "database": "blockchain",
        "user": os.environ.get("LOCAL_PSQL_USERNAME"),
        "password": os.environ.get("LOCAL_PSQL_PASSWORD"),
    }

# create a Postgres Connection
def connect_postgres(params_dic):
    """Connect to the PostgreSQL database server"""
    conn = None
    try:
        # connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database...")
        conn = pg2.connect(**params_dic)
    except (Exception, pg2.DatabaseError) as error:
        print("🤯 Error connecting to postgres: ")
        print(error)
        raise
    print("📺 Got Postgress Connection")
    return conn


def query_postgres(sql, columns=None):
    try:
        conn = connect_postgres(postgres_conn_params)
        cur = conn.cursor()
        print("🌿🌿 executing query on postgres:" + str(datetime.datetime.now()))
        print(sql)
        cur.execute(sql)
        if columns != None:
            data_rows = cur.fetchall()
            print(datetime.datetime.now())
            return pd.DataFrame(data_rows, columns=columns)
        conn.commit()
        print("🌿🌿 finished: " + str(datetime.datetime.now()))

    except (Exception, pg2.Error) as error:
        print("🤯 Error querying postgres: ")
        print(error)
        raise

    finally:
        # closing database connection.
        if conn:
            cur.close()
            conn.close()


# load csv into postgres
def copy_from_file_to_postgres(csv_filename_with_path, table):
    """
    Load the csv file into a posgres table.
    """
    print(f"🌿 Loading {csv_filename_with_path} into {table} at: " + str(datetime.datetime.now()))
    sql = f"""
    COPY {table}
    FROM '{csv_filename_with_path}'
    DELIMITER ','
    CSV HEADER;
    """
    query_postgres(sql)


def upsert_from_file_to_postgres(csv_filename_with_path, table, key):
    print(f"🌿 Upsert into {csv_filename_with_path} into {table} at: " + str(datetime.datetime.now()))
    staging_table = create_staging_table(table)
    copy_from_file_to_postgres(csv_filename_with_path, table=staging_table)
    upsert_postgres(source=staging_table, target=table, key=key)
    query_postgres(f"drop table if exists {staging_table}")


# load a python df into postgres
def copy_from_df_to_postgres(df, table, csv_filename_with_path=None, use_upsert=False, key=None):
    print("""🌿 Load a python df into postgres""")
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
        upsert_from_file_to_postgres(csv_filename_with_path=csv_filename_with_path, table=table, key=key)
    else:
        copy_from_file_to_postgres(csv_filename_with_path=csv_filename_with_path, table=table)

    if temp_file_will_be_removed:
        os.remove(csv_filename_with_path)


def create_staging_table(table):
    staging_table = f"{table}_staging"
    query_postgres(f"drop table if exists {staging_table}")
    query_postgres(f"create table {staging_table} as select * from {table} limit 0")
    return staging_table


# upsert data from one postgres table to another
def upsert_postgres(source, target, key):
    """
    upsert a source table in postgres into a target table. The two tables should
    have the same schema. The old records and new should be differentated using the key variable
    """
    sql = f"""
        insert into {target}
        select
            s.*
        from {source} s
        left join {target} t
            on t.{key} = s.{key}
        where t.{key} is null
    """
    query_postgres(sql)


def export_postgres(table, csv_filename_with_path):
    sql = f"""
    COPY {table} TO '{csv_filename_with_path}' DELIMITER ',' CSV HEADER;
    """
    query_postgres(sql)


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
    copy_from_df_to_postgres(
        df=df, table=table, csv_filename_with_path=csv_filename_with_path, use_upsert=use_upsert, key=key
    )
####**********************************************************
#### ****************** Snowflake  ********************
#### **********************************************************

# Creating connection to snowflake
def connect_snowflake():

    try:
        cnn = connect(
        user=SNOW_USER,
        password=SNOW_PASSWORD,
        account=SNOW_ACCOUNT,
        role=SNOW_ROLE

        )
        logging.info('Connection created successfully')
        return cnn

    except:
        logging.warning('Connection creation unsuccessful')

def set_session():
    cnn = connect_snowflake()
    cs = cnn.cursor()
        #cursor = self.create_snowflake_connection().cursor(DictCursor) here the session is hardcoded
    try:
        # cursor.execute(f'USE ROLE {SNOW_ROLE};') Mostly for our purose without this we can fullfill our requirement
        # cursor.execute(f'USE WAREHOUSE {SNOW_WAREHOUSE};'
        sql = "use role ACCOUNTADMIN"
        cs.execute(sql)
        sql = "use warehouse COMPUTE_WH"  # But in future you want to move to different cluster use its name
        cs.execute(sql)
        sql = "use database ANALYTICS"
        cs.execute(sql)
        sql = "use schema PUBLIC"
        cs.execute(sql)
        return cnn
    except Exception as error_returned:
        raise RuntimeError(f'Setting the Role and Warehouse threw error: {error_returned}' )


def copy_from_file_to_snowflake_table(csv_filename_with_path, table):
    cnn = set_session()
    cs = cnn.cursor()
    print(f"🌿 Loading {csv_filename_with_path} into snowflake stage at: " + str(datetime.datetime.now()))
    file = re.findall(r'[A-Za-z0-9_\-\.]+\.[A-Za-z0-9]+$', csv_filename_with_path)
    try:
        sql = 'put file://{0} @INNERCIRCLE auto_compress=false'.format(csv_filename_with_path)
        cs.execute(sql)
        print(f"🌿🌿 copying data from stage into snowflake {table}:" + str(datetime.datetime.now()))
        sql = 'copy into {0} from (select c.* from @innercircle/{1} (file_format => D_FILEFORMAT ) c) PURGE =FALSE on_error=continue'.format(
            table, file[0])
        cs.execute(sql)
        print(f"🌿🌿 copy data from stage into snowflake {table}:" + str(datetime.datetime.now()))
        sql = "create or replace table rejectedrows_{0} as select * from table(validate(ANALYTICS.PUBLIC.{0},job_id=>'_last'))".format(
            table)
        cs.execute(sql)

    finally:
        cs.close()
    cnn.close()


def read_from_snowflake(sql):
    # A cursor object represents a database cursor for execute and fetch operations
    cnn = set_session()
    cs = cnn.cursor()
    print("🌿🌿 executing query on snowflake tables:" + str(datetime.datetime.now()))
    try:
        # Prepare and execute a database command, fetch data into a Pandas DataFrame
        cs.execute(sql)
        df = cs.fetch_pandas_all()
    except Exception as error_returned:
        raise RuntimeError(
            f'SQL statement: {sql}\n threw error {error_returned}')

    finally:
        # Close the cursor
        cs.close()
        cnn.close()

    # print(df)
    return df

def df_to_snowflake(df,table):
    cnn = set_session()
    cs = cnn.cursor()
    print("""🌿 Load a python df into snowflake""")
    try:
        write_pandas(
            conn=cnn,
            df=df,
            table_name=table,
            database='ANALYTICS',
            schema='PUBLIC')
    except Exception as e:
        print("Seems issue in writing to snowflake table", e)
        raise e

#this is using query and processed data in s3 within table name folder in S3
def export_from_snowflake_to_s3(sql, table):
    cnn = set_session()
    cs = cnn.cursor()
    s3_date=str(datetime.datetime.today().strftime ('%d%m%Y'))
    print(f"🌿 Loading snowflake query results into s3/{table}: " + str(datetime.datetime.now()))
    try:
        sql = "copy into '@gets3data/{0}/{2}/' from ({1})  file_format=(type=csv COMPRESSION =  NONE)\
        HEADER = TRUE OVERWRITE = FALSE SINGLE = TRUE".format(table, sql,s3_date)
        cs.execute(sql)

    finally:
        cs.close()
    cnn.close()

def copy_snowflake_internal_stage_local(csv_filename_with_path):
    cnn = set_session()
    cs = cnn.cursor()
    print(f"🌿 Loading all files from snowflake stage to {csv_filename_with_path}: " + str(datetime.datetime.now()))
    try:
        sql = 'get @innercircle/ file://{0}'.format(csv_filename_with_path)
        cs.execute(sql)
    #         print(f"🌿🌿 copy data from stage into snowflake {table}:" + str(datetime.datetime.now()))

    finally:
        cs.close()
    cnn.close()

# **********************************************************
# ****************** CSV FILE IO ***************************
# **********************************************************

# get the filenames under a certain path
def get_all_files_in_path(path):
    files = glob.glob(path + "*")
    files.sort()
    return files


# load all files under a path into postgres
def load_all_local_files(table, path):
    files = get_all_files_in_path(path)
    for file in files:
        print("🦄🦄 copying >> " + file)
        copy_from_file_to_postgres(table, file)


def rename_csv_header_single_file(file, new_columns):
    print("🪖 renaming " + file.split("/")[-1])
    with open(file, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
    if header != new_columns:
        data = pd.read_csv(file)
        data.columns = new_columns
        data.to_csv(file, index=False)


def rename_csv_header_dataset(table, new_columns):
    files = glob.glob(CSV_WAREHOUSE_PATH + PATHS[table] + "*")
    for file in files:
        rename_csv_header_single_file(file, new_columns)


# **********************************************************
# ****************** ETL utils *****************************
# **********************************************************

# get a list of dates from start_date ('2021-12-24' format) to end_date and return the list reverse if required.
# The default end_date is the day before the execution date.
def get_date_list(start_date=None, end_date=None, reverse=False):
    dim_date = pd.read_csv("dim_dates.csv")

    if end_date == None:  # default end date is yesterday
        end_date = get_previous_day()

    dim_date = dim_date[dim_date.full_date <= end_date]

    if start_date != None:
        dim_date = dim_date[dim_date.full_date >= start_date]

    dates = list(dim_date.full_date)
    dates.sort()

    if reverse:
        dates.reverse()
    return dates

def delete_current_day_data(date, table, key="timestamp"):
    gap = check_table_for_date_gaps(
        table=table
        , start_date=date
        , end_date=date
        , key=key)

    if len(gap)==0: # there is existing data.
        query_postgres(
            sql = f"delete from {table} where date({key}) = '{date}'"
        )

def get_previous_day(from_date=None, num_days=1):
    dim_date = pd.read_csv("dim_dates.csv")
    if from_date == None:
        today = datetime.datetime.now().date().strftime("%Y-%m-%d")
        from_date = today
    prev_day = dim_date.full_date[dim_date.full_date < from_date].max()
    if num_days == 1:
        return prev_day
    else:
        return get_previous_day(from_date=prev_day, num_days=num_days-1)


# get max(timestamp of existing table)
def get_terminal_ts(table, end, offset=None, key="timestamp"):
    if end != "max" and end != "min":
        raise ValueError("🤯 The end param in get_terminal_ts should be either max or min")
    offset_string = ""
    if offset != None:
        offset_string = f" + interval'{offset}'"
    sql = f"select cast({end}({key} {offset_string}) as varchar) from {table}"
    data = query_postgres(sql, columns=["ts"])
    ts = data.ts.iloc[0]
    print("⏱ Terminal ts retrieved: " + str(ts))
    return ts

def check_table_for_date_gaps(table, start_date, end_date=None, key="timestamp"):
    dates = get_date_list(start_date=start_date, end_date=end_date)
    end_date_clause = f"and {key} <= date('{end_date}') + interval'1 day'" if end_date != None else ""
    sql = f"""
        select cast(date({key}) as varchar) as date
        from {table}
        where {key} >= '{start_date}'
            {end_date_clause}
        group by 1
    """
    uploaded = query_postgres(sql, columns=["date"])
    uploaded = uploaded.date.to_list()

    gaps = [date for date in dates if date not in uploaded]
    gaps.sort()
    print(f"🦄🦄: {table} gaps:")
    print(gaps)
    return gaps


# **********************************************************
# ****************** OpenSea utils *************************
# **********************************************************



# call opensea API to retrieve collection's meta data
def get_contract_meta_data_from_opensea(contract):
    collection_keys = [
        "name",
        "safelist_request_status",
        "description",
        "image_url",
        "banner_image_url",
        "external_url",
        "twitter_username",
        "discord_url",
        "telegram_url",
        "instagram_username",
        "medium_username",
        "wiki_url",
        "payout_address",
        "slug"
    ]
    meta = {"address": contract}

    url = f"https://api.opensea.io/api/v1/asset_contract/{contract}"
    headers = {"X-API-KEY": OPENSEA_API_KEY}
    response = requests.request("GET", url, headers=headers)
    status_code = response.status_code
    if status_code != 200:  # logging only
        print(f"🚨 contract {contract} returns non-200 status code: {status_code}")
        if status_code in (429, 404):  # too many requests let the calling function handle this
            print("🚨 encounter HTTP error 429 too many requests or 404 page doesn't exist")
            return meta, status_code
        return meta, status_code
    try:
        data = json.loads(response.text)
        collection = data.get("collection", {})
        if (collection == None) | (status_code == 406):
            print(
                """
            🚨 collection null or status_code ==406 HTTP Not Acceptable { "detail": "Unable to determine token standard for contract." }
            """
            )
            meta["name"] = "Unnamed"
            for key in collection_keys:
                if key != "name":
                    meta[key] = None
        else:
            meta["name"] = collection.get(
                "name", "Unnamed"
            )  # give the default value to satisfy the the db requirement of not null. Also to avoid repeatedly getting meta data for this contract in the next run.
            for key in collection_keys:
                if key != "name":
                    meta[key] = collection.get(key, None)

            meta['last_updated_at']=datetime.datetime.now()

    except Exception as e:
        raise ValueError("🤯 Error: Received data is not json. Could be network timeout", e, "data:", data)

    print(f"{contract}: status_code {status_code}, name {meta['name']}")
    return meta, status_code
