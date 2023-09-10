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

def get_eod_data(symbol:str,
                 exchange:str,
                 endpoint:str,
                 start:Optional[str] = None,
                 end:Optional[str] = None,
                 api_key=token,
                 session=None):
    """
    Returns EOD (end of day data) for a given symbol
    """
    symbol_exchange = symbol + "." + exchange
    session = init_session(session)
    start, end = sanitize_dates(start, end)
    endpoint = f"/{endpoint}/{symbol_exchange}"
    url = EOD_HISTORICAL_DATA_API_URL + endpoint
    params = {
        "api_token": api_key,
        "from": format_date(start),
        "to": format_date(end)
    }
    
    r = session.get(url, params=params)
    
    if r.status_code == requests.codes.ok:
        df = pd.read_csv(StringIO(r.text), 
                         skipfooter=1,
                         parse_dates=[0], 
                         index_col=0)
        return df
    
    else:
        params["api_token"] = "YOUR_HIDDEN_API"
        raise RemoteDataError(r.status_code, r.reason, url(url, params))
        
def get_fundamental_data(symbol: str,
                         exchange: str,
                         endpoint: str,
                         api_key=token,
                         session=None):
    """
    Returns EOD (end of day data) for a given symbol
    """
    symbol_exchange = symbol + "." + exchange
    session = init_session(session)
    endpoint = f"/{endpoint}/{symbol_exchange}"
    url = EOD_HISTORICAL_DATA_API_URL + endpoint
    params = {
        "api_token": api_key
    }
    
    r = session.get(url, params=params)
    
    if r.status_code == requests.codes.ok:
        #df = pd.read_csv(StringIO(r.text), skipfooter=1,
        #                 parse_dates=[0], index_col=0)
        print(r.text)
    else:
        pass

    return r.text
