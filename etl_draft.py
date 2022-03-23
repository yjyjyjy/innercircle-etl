#ADDING CONFIG FILE FROM my own config module below is my location
import sys
MODULE_FULL_PATH = 'C:/bigdata/PycharmProjects/PysparkPOC/config/'

sys.path.insert(1, MODULE_FULL_PATH)
import config as c
import pandas as pd
import snowflake.connector
import logging
import os
from io import StringIO
import boto3
import csv
from snowflake.connector import *

# Creating connection to snowflake¶
def create_snowflake_connection():
    # WE CAN PASS ARGUMENT ALSO DEPENDING ON REQUIREMENTS
    # The connection object holds the connection and session information to keep the database connection active

    try:
        cnn = connect(
        user=os.environ["SNOW_USER"],
        password=os.environ["SNOW_PASSWORD"],
        account=os.environ["SNOW_ACCOUNT"],
        role=os.environ["SNOW_ROLE"],
        )
        logging.info('Connection created successfully')
        return cnn

    except:
        logging.warning('Connection creation unsuccessful')

#Reading from snowflake tables and returning dataframe¶
def read_from_snowflake(query):
    # A cursor object represents a database cursor for execute and fetch operations
    s_client = create_snowflake_connection()
    cs = s_client.cursor()

    try:
        # Prepare and execute a database command, fetch data into a Pandas DataFrame
        sql = "use role ACCOUNTADMIN"
        cs.execute(sql)
        print('accesing warehouse ')
        sql = "use warehouse COMPUTE_WH"
        cs.execute(sql)
        print("accesing database")
        sql = "use database ANALYTICS"
        cs.execute(sql)
        print("accesing schema")
        sql = "use schema PUBLIC"
        cs.execute(sql)
        cs.execute(query)
        df = cs.fetch_pandas_all()
    finally:
        pass
        # Close the cursor
        cs.close()

    print(df)
    return df
#small test along the way thats works will remove this in later version
query='select * from block'
df=read_from_snowflake(query)

''' this is just a waste As Iam trying to depicts whats you have from bigquery the csv file path and table name
def snowflake_stage(table):
    staging_table = f"test/{table}" #test is the stage name here ours is GETS3DATA 
    return staging_table
def local_filepath():
    filepath= "C:\\bigdata\\PycharmProjects\\PysparkPOC\\lunyu\\{0}\\{0}.csv".format(c.table3) #use your own
    return filepath'''

# Loading into snowflake stage¶
def snowflake_local2stage():
    cnn=create_snowflake_connection()
    cs = cnn.cursor()
    try:
        sql = "use role accountadmin"
        cs.execute(sql)
        print('accesing warehouse ')
        sql = "use warehouse COMPUTE_WH"
        cs.execute(sql)
        print("accesing database")
        sql = "use database ANALYTICS"
        cs.execute(sql)
        print("accesing schema")
        sql = "use schema PUBLIC"
        cs.execute(sql)
        sql = 'put file://{0} @{1} auto_compress=false'.format(c.FILE_PATH,c.stage)
        cs.execute(sql)

    finally:
        cs.close()
    cnn.close()

#again testing purpose snowflake_local2stage() we will work with arguments after discussion

# Copying from snowflake stage into resp table
def snowflake_stage_table():
    cnn=create_snowflake_connection()
    cs = cnn.cursor()
    try:
        sql = "use role accountadmin"
        cs.execute(sql)
        print('accesing warehouse ')
        sql = "use warehouse COMPUTE_WH"
        cs.execute(sql)
        print("accesing database")
        sql = "use database ANALYTICS"
        cs.execute(sql)
        print("accesing schema")
        sql = "use schema PUBLIC"
        cs.execute(sql)
        sql = "copy into {0} from @{1}{0}.csv file_format= (FORMAT_NAME=D_FILEFORMAT RECORD_DELIMITER='\n' SKIP_HEADER = 1 )\
         purge=true on_error=skip_file".format(c.table,c.stage)
    finally:
        cs.close()
    cnn.close()
#everything depend on argument and we need to fien tune those things.

###########################################################
#######################################################################
### func to Execute Many SQL Statements from a File Using the Python Connector for Snowflake
##everything became simpler with this function working on more advanced one also
#########################################
def run_query_on_snowflake(query):
    cnn= create_snowflake_connection()
    with open (query, 'r',encoding='utf-8') as f:
        for i in cnn.execute_stream(f):
            for j in i:
                print(j)
    cnn.close()

########### unloading from SNOWFLAKE TABLE TO AMAZON S3########## done

query='C:\\bigdata\\PycharmProjects\\PysparkPOC\\lunyu\\SQL\\query_snow_2_S3.sql'
run_query_on_snowflake(query)

########### LOCAL TO SNOWFLAKE STAGE

query='C:\\bigdata\\PycharmProjects\\PysparkPOC\\lunyu\\SQL\\query_stage.sql'
run_query_on_snowflake(query)

# The last part will not work bcz of the file that i send through it but i can demonstarate