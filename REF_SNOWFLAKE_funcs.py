import snowflake.connector
import logging
import os
from io import StringIO
import csv

# Create the connection string
cnn = snowflake.connector.connect(
   user='DEXPLORER',
   password='**********', # IF YOU WANT TO PASS THIS ARGUMENT YOU CAN DO IT.
   account='*******')

cs = cnn.cursor()
def creating_snowflake_fileformat():
    sql = "CREATE FILE FORMAT ANALYTICS.PUBLIC.D_FILEFORMAT_NOHEADER /" \
          "TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' " \
          "RECORD_DELIMITER = '\n' SKIP_HEADER = 0 FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'" \
          " TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' " \
          "DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N')";
    cs.execute(sql)

def creating_snowflake_STAGES():
    sql1 = "CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = S3 ENABLED = TRUE \
    STORAGE_AWS_ROLE_ARN = 'arn:aws:sqs:ap-south-1:841748965781:sf-snowpipe-AIDA4H7BF5GK5V5XTVOWT-MT2il3H-7LI4sH2sDQZ5rQ'\
     STORAGE_ALLOWED_LOCATIONS =('s3://innercircle-etl')";
    cs.execute(sql1)

def validate_snowflake_pipe():
    try:
        # validate any load to the pipe within the previos DAY
        sql="select * from table(validate_pipe_load(pipe_name=>'SNOW_PIPE_BLOCK',start_time=>dateadd(DAy,-1,current_timestamp())));"
        cs.execute(sql)
        # validate any load to the pipe within the previos 12 hour or any custom you tell me to do.
        sql = "select * from table(validate_pipe_load(pipe_name=>'SNOW_PIPE_BLOCK',start_time=>dateadd(hour,-12,current_timestamp())));"
        cs.execute(sql)
    finally:
        cs.close()
    cnn.close()
