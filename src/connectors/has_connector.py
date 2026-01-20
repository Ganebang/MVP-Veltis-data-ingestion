"""
HAS (Haute Autorité de Santé) connector for quality certification data.

Fetches hospital/clinic certification and quality assessment information
from the French health authority's official sources.
"""

import logging
from typing import Optional, List, Dict
import pandas as pd
from datetime import datetime

from .base import BaseConnector
from src.config import config, HASConfig


logger = logging.getLogger(__name__)


class HASConnector(BaseConnector):
    """
    Connector for HAS data sources.
    
    Fetches certification levels, visit dates, and patient satisfaction scores
    for healthcare establishments.
    """
    
    def __init__(self, has_config: Optional[HASConfig] = None):
        """
        Initialize HAS connector.
        
        Args:
            has_config: Configuration object for HAS API
        """
        if has_config is None:
            has_config = config.has
        
        super().__init__(
            name="HAS",
            base_url=has_config.base_url,
            timeout=has_config.timeout
        )
        
        self.config = has_config
        self.api_key = has_config.api_key
    
    def fetch_certification_data(self, finess_list: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Fetch certification data for establishments.
        
        Args:
            finess_list: Optional list of FINESS numbers to filter
            
        Returns:
            DataFrame with certification data or None if failed
        """
        # Check cache first
        cache_key = "has_certification_data"
        cached_df = self.get_cached_data(cache_key)
        if cached_df is not None:
            return cached_df
        
        logger.info("Fetching HAS certification data...")
        
        try:
            # HAS provides data through various endpoints and open datasets
            # This is a placeholder for the actual implementation
            # In production, this would connect to HAS SFTI or their API
            
            # Example structure of expected data
            df = pd.DataFrame({
                'finess': [],
                'niveau_certification': [],
                'date_certification': [],
                'lien_rapport': [],
                'score_satisfaction': []
            })
            
            if len(df) == 0:
                logger.warning("No HAS certification data found")
                return None
            
            self.cache_data(cache_key, df, ttl_hours=48)  # Cache for 48 hours
            logger.info(f"Fetched {len(df)} HAS records")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching HAS data: {e}")
            return None
    
    def fetch_satisfaction_scores(self, finess_list: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Fetch patient satisfaction scores (e-Satis) for establishments.
        
        Args:
            finess_list: Optional list of FINESS numbers to filter
            
        Returns:
            DataFrame with satisfaction scores or None if failed
        """
        # Check cache first
        cache_key = "has_satisfaction_scores"
        cached_df = self.get_cached_data(cache_key)
        if cached_df is not None:
            return cached_df
        
        logger.info("Fetching HAS patient satisfaction scores (e-Satis)...")
        
        try:
            # e-Satis data would be fetched from HAS official portal
            # This is a placeholder for the actual implementation
            
            df = pd.DataFrame({
                'finess': [],
                'score_satisfaction': [],
                'date_score': [],
                'nombre_reponses': []
            })
            
            if len(df) == 0:
                logger.warning("No e-Satis data found")
                return None
            
            self.cache_data(cache_key, df, ttl_hours=24)
            logger.info(f"Fetched {len(df)} satisfaction score records")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching e-Satis data: {e}")
            return None
    
    def parse_certification_level(self, raw_level: str) -> str:
        """
        Parse and standardize certification level strings.
        
        Args:
            raw_level: Raw certification level string from HAS
            
        Returns:
            Standardized certification level
        """
        if raw_level is None:
            return "Non évalué"
        
        raw_level_lower = str(raw_level).lower().strip()
        
        # Map various HAS certification levels to standard values
        level_mapping = {
            'haute qualité': 'Haute Qualité',
            'très bon': 'Haute Qualité',
            'certifié': 'Certifié',
            'bon': 'Certifié',
            'non conforme': 'Non Conforme',
            'faible': 'Non Conforme',
            'non évalué': 'Non évalué',
            'en attente': 'Non évalué',
        }
        
        for key, value in level_mapping.items():
            if key in raw_level_lower:
                return value
        
        # Default fallback
        logger.warning(f"Unknown certification level: {raw_level}")
        return "Non évalué"
    
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        Main fetch method - fetches all available HAS data.
        
        Returns:
            Combined DataFrame with certification and satisfaction data
        """
        finess_list = kwargs.get('finess_list', None)
        
        # Fetch both certification and satisfaction data
        cert_df = self.fetch_certification_data(finess_list)
        satis_df = self.fetch_satisfaction_scores(finess_list)
        
        # Merge on FINESS if both available
        if cert_df is not None and satis_df is not None:
            try:
                merged = pd.merge(cert_df, satis_df, on='finess', how='left')
                logger.info(f"Merged HAS data: {len(merged)} records")
                return merged
            except Exception as e:
                logger.error(f"Error merging HAS data: {e}")
                return cert_df
        
        return cert_df if cert_df is not None else satis_df
    
    def validate_response(self, response: Optional[pd.DataFrame]) -> bool:
        """
        Validate HAS data response.
        
        Args:
            response: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        if response is None or not isinstance(response, pd.DataFrame):
            logger.error("Invalid response: not a DataFrame")
            return False
        
        # HAS data can be partial, so we just check it's not completely empty
        if len(response) == 0:
            logger.warning("HAS response is empty")
            return False
        
        logger.info("HAS data validation successful")
        return True
