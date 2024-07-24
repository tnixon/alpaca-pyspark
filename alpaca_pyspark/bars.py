from typing import Optional, Generator
import datetime as dt
import requests
import urllib.parse as urlp

import pandas as pd

# response to column mapping
BAR_MAPPING = {
    "t": "timestamp",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
    "n": "trade_count",
    "vw": "vwap",
}

DEFAULT_DATA_ENDPOINT = "https://data.alpaca.markets/v2"

# credentialsuration
# apca_creds = {
#   'key_id': "",
#   'secret_key': ""
# }


def build_url(path_elements: list[str],
              params: dict,
              endpoint: str = DEFAULT_DATA_ENDPOINT,
              page_token: Optional[str] = None) -> str:
  # fold page token into params if present
  if page_token:
    params['page_token'] = page_token

  # build URL
  path = "/".join(path_elements)
  param_str = "&".join([f"{k}={v}" for k, v in params.items()])
  return f"{endpoint}/{path}?{param_str}"


def empty_bars() -> pd.DataFrame:
  return pd.DataFrame(columns=BAR_MAPPING.values())


def bars_to_pd(bars: dict) -> pd.DataFrame:
  bars_pd = empty_bars()
  for sym in bars.keys():
    sym_bars = pd.DataFrame.from_records(bars[sym]).rename(columns=BAR_MAPPING)
    sym_bars["symbol"] = sym
    bars_pd = pd.concat([bars_pd, sym_bars], axis=0, sort=False)
  return bars_pd


def get_bars(symbol: str,
             start_t: dt.datetime,
             end_t: dt.datetime,
             credentials: dict,
             timeframe: str = '1Day',
             limit: int = 1000) -> Generator[pd.DataFrame, None, None]:
  headers = {
    'Content-Type': 'application/json',
    'APCA-API-KEY-ID': credentials['key_id'],
    'APCA-API-SECRET-KEY': credentials['secret_key']
  }
  params = {
    "symbols": symbol,
    "timeframe": timeframe,
    "start": urlp.quote(start_t.isoformat()),
    "end": urlp.quote(end_t.isoformat()),
    "limit": 1000
  }

  def get_bars_page(sess: requests.Session, params: dict) -> dict:
      response = sess.get(build_url(["stocks", "bars"], params), 
                          headers=headers)
      response.raise_for_status()
      return response.json()

  # iterate through pages in a session
  with requests.Session() as sess:
    # call the API
    data = get_bars_page(sess, params)

    # yield data from response
    if "bars" in data and data["bars"]:
      yield bars_to_pd(data["bars"])
    else:
      # return an empty dataframe if no data
      yield empty_bars()

    # iterate through pages
    while "next_page_token" in data and data['next_page_token']:
      params['page_token'] = data['next_page_token']
      data = get_bars_page(sess, params)
      if "bars" in data and data["bars"]:
        yield bars_to_pd(data["bars"])

