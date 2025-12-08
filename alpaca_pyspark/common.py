import urllib.parse as urlp
from dataclasses import dataclass
from typing import Optional

from pyspark.sql.datasource import InputPartition
from requests import Session, HTTPError


@dataclass
class SymbolPartition(InputPartition):
    symbol: str


def build_url(endpoint: str,
              path_elements: list[str],
              params: dict) -> str:
    # build URL
    path = "/".join(path_elements)
    
    # Convert all param values to strings and handle None values
    param_pairs = []
    for k, v in params.items():
        if v is not None:
            # Convert to string if not already, then quote
            quoted_v = urlp.quote(str(v))
            param_pairs.append(f"{k}={quoted_v}")
    
    param_str = "&".join(param_pairs)

    # assemble the final URL
    return f"{endpoint}/{path}?{param_str}"


def build_page_fetcher(endpoint: str, headers: dict, path_elements: list[str]):
    def get_page(sess: Session, params: dict, page_token: Optional[str]) -> dict:
        # append page token if it exists
        if page_token:
            params['page_token'] = page_token
        response = sess.get(build_url(endpoint, path_elements, params), headers=headers)
        if not response.ok:
            raise HTTPError(f"HTTP error {response.status_code} for {response.url}: {response.text}")
        return response.json()
    return get_page
