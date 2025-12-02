# Databricks notebook source
import alpaca_pyspark

# COMMAND ----------

# credentials
apca_creds = {
  'key_id': dbutils.secrets.get(scope="tnixon_alpaca_api", key="api_key"),
  'secret_key': dbutils.secrets.get(scope="tnixon_alpaca_api", key="secret")
}

# COMMAND ----------

import datetime as dt

start_t = dt.datetime(2021, 1, 1)
end_t = dt.datetime(2022, 1, 1)
symbol = 'AAPL'

aapl_bars = alpaca_pyspark.get_bars(symbol, start_t, end_t, apca_creds)

# COMMAND ----------

import pandas as pd

aapl_bars_pd = alpaca_pyspark.bars.empty_bars()
for bars in aapl_bars:
  aapl_bars_pd = pd.concat([aapl_bars_pd, bars], axis=0)

# COMMAND ----------


