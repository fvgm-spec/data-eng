#Importing required libraries
import pandas as pd
import numpy as np
import os
from io import StringIO
from typing import Optional
import requests
import awswrangler as wr
from utilities import init_session, sanitize_dates, format_date, url, RemoteDataError

#Setting variables
token = os.getenv('EOD_API_TOKEN')
aws_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
#This API key is used for demo purposes
EOD_HISTORICAL_DATA_API_KEY_DEFAULT = "OeAFFmMliFG5orCUuwAKQ8l4WWFQ67YX"
EOD_HISTORICAL_DATA_API_URL = "https://eodhistoricaldata.com/api"

def write_data_to_bucket(file_name:str, mode:str):
    """
    Parameters:
    ----------
    mode(str): Available write modes are 'append', 'overwrite' and 'overwrite_partitions'
    """

    path = f"s3://{bucket}/raw-data/{file_name}"
    #Sending dataframe of corresponding ticker to bucket
    wr.s3.to_csv(
        df=df,
        path=path,
        index=True,
        dataset=True,
        mode=mode
    )

def read_csv_from_bucket(folder_name:str) -> pd.DataFrame:

    df = wr.s3.read_csv(path = f"s3://{bucket}/raw-data/{folder_name}/",
                        path_suffix = ".csv"
)
    return df
