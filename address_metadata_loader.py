import pandas as pd
import etl_utls as utl
import os
from address_metadata import address_metadata_worker as mw

mw.FINISHED_FILE

if mw.FINISHED_FILE not exists:
    quit

# read in the file
# split into addresses
# for each address, find load the corresponding json file
# compose the dataframe and load
