"""
Data.gouv API connector for fetching hospital/clinic registry data.

Handles retrieval of FINESS data (geographic identification numbers)
and establishment information from the official French open data portal.
"""

import logging
from typing import Optional
import pandas as pd
import io

from .base import BaseConnector
from src.config import config, DataGouvConfig


logger = logging.getLogger(__name__)


class DataGouvConnector(BaseConnector):
    """
    Connector for Data.gouv API.
    
    Fetches FINESS (Fichier d'Identification Nationale des Etablissements
    et Structures du domaine sanitaire et social) data containing hospital
    and clinic registry information.
    """
    
    def __init__(self, datagouv_config: Optional[DataGouvConfig] = None):
        """
        Initialize Data.gouv connector.
        
        Args:
            datagouv_config: Configuration object for Data.gouv API
        """
        if datagouv_config is None:
            datagouv_config = config.datagouv
        
        super().__init__(
            name="DataGouv",
            base_url=datagouv_config.base_url,
            timeout=datagouv_config.timeout
        )
        
        self.config = datagouv_config
        self.finess_dataset_id = datagouv_config.finess_dataset_id
        self.finess_keyword = datagouv_config.finess_resource_keyword
    
    def search_datasets(self, query: str) -> Optional[list]:
        """
        Search for datasets by query text.
        
        Args:
            query: Search query
            
        Returns:
            List of dataset dicts or None if failed
        """
        url = f"{self.base_url}/datasets/"
        params = {
            "q": query,
            "page_size": 10,
            "sort": "-created" 
        }
        
        response = self.get(url, params=params)
        
        if response is None:
            logger.error(f"Failed to search datasets for query: {query}")
            return None
            
        try:
            data = response.json()
            return data.get('data', [])
        except ValueError as e:
            logger.error(f"Failed to parse search response: {e}")
            return None

    def get_dataset_info(self, dataset_id: str) -> Optional[dict]:
        """
        Get dataset metadata from Data.gouv API.
        
        Args:
            dataset_id: Dataset ID to fetch metadata for
            
        Returns:
            Dataset metadata dict or None if failed
        """
        url = f"{self.base_url}/datasets/{dataset_id}/"
        response = self.get(url)
        
        if response is None:
            logger.error(f"Failed to fetch dataset info for {dataset_id}")
            return None
        
        try:
            data = response.json()
            logger.info(f"Successfully fetched metadata for dataset {dataset_id}")
            return data
        except ValueError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return None
    
    def find_csv_resource(self, dataset_info: dict, keyword_filter: str) -> Optional[str]:
        """
        Find CSV resource URL matching keyword filter.
        
        Args:
            dataset_info: Dataset metadata dict
            keyword_filter: Keyword to search in resource title
            
        Returns:
            URL of matching CSV resource or None if not found
        """
        resources = dataset_info.get('resources', [])
        
        # Sort by last_modified date (most recent first)
        resources = sorted(
            resources,
            key=lambda x: x.get('last_modified', ''),
            reverse=True
        )
        
        for resource in resources:
            if (resource.get('format', '').upper() == 'CSV' and 
                keyword_filter.lower() in resource.get('title', '').lower()):
                logger.info(f"Found CSV resource: {resource.get('title')}")
                logger.info(f"Last modified: {resource.get('last_modified')}")
                return resource.get('url')
        
        logger.warning(f"No CSV resource found with keyword: {keyword_filter}")
        return None
    
    def download_csv(self, url: str, separator: Optional[str] = None,
                     encoding: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Download and parse CSV from URL.
        
        Args:
            url: CSV file URL
            separator: CSV field separator (default from config)
            encoding: File encoding (default from config)
            
        Returns:
            Pandas DataFrame or None if failed
        """
        if separator is None:
            separator = self.config.csv_separator
        if encoding is None:
            encoding = self.config.csv_encoding
        
        logger.info(f"Downloading CSV from {url}...")
        response = self.get(url)
        
        if response is None:
            logger.error(f"Failed to download CSV from {url}")
            return None
        
        try:
            content = response.content
            df = pd.read_csv(
                io.BytesIO(content),
                sep=separator,
                encoding=encoding,
                low_memory=False
            )
            logger.info(f"Successfully parsed CSV: {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Failed to parse CSV: {e}")
            return None
    
    def fetch_finess_data(self) -> Optional[pd.DataFrame]:
        """
        Fetch FINESS data from Data.gouv.
        
        Returns:
            DataFrame with FINESS establishment data or None if failed
        """
        # Check cache first
        cache_key = "finess_data"
        cached_df = self.get_cached_data(cache_key)
        if cached_df is not None:
            return cached_df
        
        logger.info("Fetching FINESS data from Data.gouv...")
        
        # Get dataset metadata
        dataset_info = self.get_dataset_info(self.finess_dataset_id)
        if dataset_info is None:
            return None
        
        # Find CSV resource
        csv_url = self.find_csv_resource(dataset_info, self.finess_keyword)
        if csv_url is None:
            return None
        
        # Download and parse CSV
        df = self.download_csv(csv_url)
        if df is not None:
            self.cache_data(cache_key, df, ttl_hours=24)  # Cache for 24 hours
        
        return df
    
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        Main fetch method - fetches FINESS data.
        
        Returns:
            DataFrame with FINESS data
        """
        return self.fetch_finess_data()
    
    def validate_response(self, response: Optional[pd.DataFrame]) -> bool:
        """
        Validate FINESS data response.
        
        Args:
            response: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        if response is None or not isinstance(response, pd.DataFrame):
            logger.error("Invalid response: not a DataFrame")
            return False
        
        if len(response) == 0:
            logger.error("Invalid response: empty DataFrame")
            return False
        
        # Check for required columns (case-insensitive)
        required_cols = ['num_finess_et', 'rs', 'siret']
        df_cols_lower = [c.lower() for c in response.columns]
        
        for col in required_cols:
            if col.lower() not in df_cols_lower:
                logger.error(f"Missing required column: {col}")
                return False
        
        logger.info("FINESS data validation successful")
        return True