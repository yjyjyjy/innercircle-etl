
#!pip install snowflake-connector-python==2.7.4

import pandas as pd
import snowflake.connector
import logging
import os
from io import StringIO
import boto3
import csv

# MY ACCOUNT HAVE DEXPLORER ROLE AND WE MAY NEED TO ASSIGN SYSADMIN\(MASTER ROLE :ACCOUNTADMIN)

# Create the connection string 
cnn = snowflake.connector.connect(
   user='DEXPLORER',
   password='Dhruv@1997',
   account='ouhduii-zs68033')

cs = cnn.cursor()
try:
    sql = "use role explorer_role"
    cs.execute(sql)
    print('accesing warehouse ')
    sql = "use warehouse COMPUTE_WH"
    cs.execute(sql)
    print("accesing database")
    sql="use database ANALYTICS"
    cs.execute(sql)
    print("accesing schema")
    sql="use schema PUBLIC"
    cs.execute(sql)

finally:
    cs.close()
#     pass
cnn.close()