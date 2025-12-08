from typing import Union, Iterator, Tuple, Sequence, Dict, List
from datetime import datetime as dt
import ast

import requests
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

from .common import SymbolPartition, build_page_fetcher

DEFAULT_DATA_ENDPOINT = "https://data.alpaca.markets/v2"

##
## Historical Bars
##

class HistoricalBarsDataSource(DataSource):

    def __init__(self, options: Dict[str, str]) -> None:
        super().__init__(options)

    @classmethod
    def name(cls) -> str:
        return "Alpaca_HistoricalBars"

    def schema(self) -> Union[StructType, str]:
        return """
            symbol STRING,
            time TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INT,
            trade_count INT,
            vwap FLOAT
        """

    def reader(self, schema: StructType) -> "DataSourceReader":
        return HistoricalBarsReader(schema, self.options)


class HistoricalBarsReader(DataSourceReader):

    def __init__(self, schema: StructType, options: Dict[str, str]) -> None:
        super().__init__()
        self.schema = schema
        self.options = options

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            'Content-Type': 'application/json',
            'APCA-API-KEY-ID': self.options['APCA-API-KEY-ID'],
            'APCA-API-SECRET-KEY': self.options['APCA-API-SECRET-KEY']
          }

    def _api_params(self) -> Dict[str, str]:
        return {
            "timeframe": self.options['timeframe'],
            "start": self.options['start'],
            "end": self.options['end'],
            "limit": 1000
          }

    @property
    def endpoint(self) -> str:
        return self.options.get("endpoint", DEFAULT_DATA_ENDPOINT)

    @property
    def symbols(self) -> List[str]:
        symbols = self.options.get("symbols", [])
        if isinstance(symbols, str):
            return ast.literal_eval(symbols)
        return symbols

    def partitions(self) -> Sequence[SymbolPartition]:
        return [SymbolPartition(sym) for sym in self.symbols]

    def __parse_bar(self, sym: str, bar: dict) -> tuple:
        return ( sym,
                 dt.fromisoformat(bar["t"]),
                 float(bar["o"]),
                 float(bar["h"]),
                 float(bar["l"]),
                 float(bar["c"]),
                 int(bar["v"]),
                 int(bar["n"]),
                 float(bar["vw"]) )

    def read(self, partition: SymbolPartition) -> Iterator[Tuple]:
        # set up the page fetcher function
        get_bars_page = build_page_fetcher(self.endpoint, self._headers, ["stocks", "bars"])
        # our base params
        params = self._api_params()
        # set the symbol from the partition
        params['symbols'] = partition.symbol
        # open a HTTP session
        with requests.Session() as sess:
            # tracking pages
            num_pages = 0
            pg = {}
            next_page_token = None
            # cycle through pages
            while next_page_token or num_pages < 1:
                # get the page
                pg = get_bars_page(sess, params, next_page_token)
                # process each bar
                if "bars" in pg and pg["bars"]:
                    bars = pg["bars"]
                    for sym in bars.keys():
                        for bar in bars[sym]:
                            yield self.__parse_bar(sym, bar)
                # go to next page
                num_pages += 1
                next_page_token = pg.get("next_page_token", None)

