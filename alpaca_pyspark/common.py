import urllib.parse as urlp
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
import logging

from pyspark.sql.datasource import InputPartition
from requests import Session, HTTPError, RequestException

logger = logging.getLogger(__name__)


@dataclass
class SymbolPartition(InputPartition):
    """Partition representing a single stock symbol for parallel processing."""
    symbol: str


def build_url(endpoint: str,
              path_elements: List[str],
              params: Dict[str, Any]) -> str:
    """Build a properly encoded URL from components.
    
    Args:
        endpoint: Base API endpoint URL
        path_elements: List of path segments to join
        params: Query parameters dictionary
    
    Returns:
        Complete URL with properly encoded parameters
    """
    # Build URL path
    path = "/".join(path_elements)
    
    # Convert all param values to strings and handle None values
    param_pairs = []
    for k, v in params.items():
        if v is not None:
            # Optimize common type conversions
            if isinstance(v, (int, float, bool)):
                str_v = str(v)
            else:
                str_v = str(v)
            # URL encode the value
            quoted_v = urlp.quote(str_v)
            param_pairs.append(f"{k}={quoted_v}")
    
    param_str = "&".join(param_pairs)

    # Assemble the final URL
    return f"{endpoint}/{path}?{param_str}"


def build_page_fetcher(endpoint: str, headers: Dict[str, str], path_elements: List[str]):
    """Build a page fetcher function with enhanced error handling.
    
    Args:
        endpoint: Base API endpoint
        headers: HTTP headers for requests
        path_elements: URL path segments
    
    Returns:
        Function that fetches a single page of data
    """
    def get_page(sess: Session, params: Dict[str, Any], page_token: Optional[str]) -> Dict[str, Any]:
        """Fetch a single page of data from the API.
        
        Args:
            sess: HTTP session to use
            params: Query parameters
            page_token: Optional pagination token
        
        Returns:
            JSON response as dictionary
            
        Raises:
            HTTPError: For HTTP error responses
            RequestException: For network/connection errors
        """
        # Make a copy to avoid modifying the original params
        request_params = params.copy()
        
        # Append page token if it exists
        if page_token:
            request_params['page_token'] = page_token
            
        try:
            url = build_url(endpoint, path_elements, request_params)
            response = sess.get(url, headers=headers)
            
            if not response.ok:
                # Log error without exposing sensitive data
                logger.error(f"HTTP {response.status_code} for {response.url}")
                
                # Create sanitized error message (avoid exposing sensitive data in response.text)
                if response.status_code == 401:
                    error_msg = "Authentication failed. Please check your API credentials."
                elif response.status_code == 403:
                    error_msg = "Access forbidden. Check your API permissions."
                elif response.status_code == 429:
                    error_msg = "Rate limit exceeded. Please retry after some time."
                elif 400 <= response.status_code < 500:
                    error_msg = f"Client error {response.status_code}. Check your request parameters."
                else:
                    error_msg = f"Server error {response.status_code}. Please try again later."
                    
                raise HTTPError(f"HTTP error {response.status_code}: {error_msg}")
                
            return response.json()
            
        except RequestException as e:
            logger.error(f"Request failed for {endpoint}: {e}")
            raise RequestException(f"Network request failed: {e}") from e
            
    return get_page
