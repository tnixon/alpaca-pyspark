import urllib.parse as urlp
from dataclasses import dataclass
from typing import Optional

from pyspark.sql.datasource import InputPartition
from requests import Session


@dataclass
class AlpacaCredentials:
    key: str
    secret: str

# PT = TypeVar("PT")

# __params: List[DataSourceParam] = [
#     DataSourceParam[List[str]]("symbols", True, []),
#     DataSourceParam[str]("endpoint", False, DEFAULT_DATA_ENDPOINT),
#     DataSourceParam[str]("APCA-API-KEY-ID", True, None),
#     DataSourceParam[str]("APCA-API-SECRET-KEY", True, None),
# ]
#
# def __validate_options(self, options: Dict[str, str]):
#     # validate the parameters based on __params definitions
#     for param in self.__params:
#         # get the value
#         option_value = options.get(param.name, param.default)
#         # set the default if value is None
#         if option_value is None:
#             options[param.name] = param.default
#             option_value = options.get(param.name, param.default)
#
#         # Required values must be non-None
#         if option_value is None:
#             if param.required:
#                 raise ValueError(f"Required parameter '{param.name}' is missing")
#             continue
#
#         # value must be of the right type
#         if not isinstance(option_value, param.type):
#             raise TypeError(f"Parameter '{param.name}' must be of type '{param.type}'")
#
#         # Check for empty strings
#         if isinstance(option_value, str) and not option_value.strip():
#             if param.required:
#                 raise ValueError(f"Required parameter '{param.name}' cannot be empty")
#             continue
#
#         # Check for empty lists
#         if isinstance(option_value, list) and len(option_value) == 0:
#             if param.required:
#                 raise ValueError(f"Required parameter '{param.name}' cannot be an empty list")
#             continue

# @dataclass
# class DataSourceParam(Generic[PT]):
#     name: str
#     required: bool
#     default: Optional[PT]
#
#     @property
#     def type(self) -> PT:
#         return PT


@dataclass
class SymbolPartition(InputPartition):
    symbol: str


def build_url(endpoint: str,
              path_elements: list[str],
              params: dict) -> str:
    # build URL
    path = "/".join(path_elements)
    param_str = "&".join([f"{k}={urlp.quote(v)}" for k, v in params.items()])
    return f"{endpoint}/{path}?{param_str}"


def build_page_fetcher(endpoint: str, headers: dict, path_elements: list[str]):
    def get_page(sess: Session, params: dict, page_token: Optional[str]) -> dict:
        # append page token if it exists
        if page_token:
            params['page_token'] = page_token
        response = sess.get(build_url(endpoint, path_elements, params), headers=headers)
        response.raise_for_status()
        return response.json()
    return get_page
