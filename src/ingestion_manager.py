"""
Ingestion Manager for orchestrating data download from various sources.
"""

import logging
import os
import io
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd
from datetime import datetime

from src.config import config
from src.connectors.datagouv_api import DataGouvConnector
from src.connectors.has_connector import HASConnector

logger = logging.getLogger(__name__)

class IngestionManager:
    """
    Orchestrates the ingestion of data from various sources (Data.gouv, HAS, etc.)
    and manages storage in a raw/{year} directory structure.
    """
    
    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize IngestionManager.
        
        Args:
            base_path: Base path for storing data. Defaults to config.raw_data_path.
        """
        self.base_path = Path(base_path or config.pipeline.raw_data_path)
        self.datagouv_connector = DataGouvConnector()
        # self.has_connector = HASConnector() # Assuming HAS connector might be used for direct API later, 
                                            # but current plan uses DataGouv for HAS files too.
    
    def ensure_year_directory(self, year: int) -> Path:
        """
        Ensure the directory for the given year exists.
        
        Args:
            year: Year to create directory for.
            
        Returns:
            Path object for the year directory.
        """
        year_path = self.base_path / str(year)
        year_path.mkdir(parents=True, exist_ok=True)
        return year_path

    def download_health_metrics(self, year: int) -> bool:
        """
        Download Health Metrics (IQSS) for a specific year.

        Searches for the IQSS dataset on data.gouv.fr matching the year pattern,
        identifies the most relevant resource (CSV or XLSX), and downloads it.

        Args:
            year (int): Target year for the health metrics data (e.g., 2023).

        Returns:
            bool: True if the file was successfully downloaded and saved, False otherwise.
        """
        query = config.datagouv.iqss_search_pattern.format(year)
        logger.info(f"Searching for Health Metrics dataset for year {year} with query: '{query}'")
        
        datasets = self.datagouv_connector.search_datasets(query)
        if not datasets:
            logger.warning(f"No datasets found for Health Metrics {year}")
            return False
            
        # Prioritize exact match or best match
        target_dataset = datasets[0] 
        dataset_id = target_dataset.get('id')
        logger.info(f"Found dataset: {target_dataset.get('title')} (ID: {dataset_id})")
        
        dataset_info = self.datagouv_connector.get_dataset_info(dataset_id)
        if not dataset_info:
            return False
            
        # Try to find a CSV resource first, or XLSX
        # IQSS data is often in XLSX or CSV. We need to be flexible.
        # For this MVP, let's look for a resource that looks like the main data file.
        # Keywords to look for: "resultats", "indicateurs", "csv", "xlsx"
        
        # Helper to find resource
        resources = dataset_info.get('resources', [])
        target_resource = None
        
        # Simple heuristic: Look for largest CSV or XLSX file that has 'resultat' or 'indicateur' in title
        # Or just download the first CSV/XLSX
        
        for res in resources:
            fmt = res.get('format', '').lower()
            title = res.get('title', '').lower()
            if fmt in ['csv', 'xlsx'] and ('resultat' in title or 'indicateur' in title):
                target_resource = res
                break
        
        if not target_resource and resources:
             # Fallback to the first CSV/XLSX if no title match
            for res in resources:
                if res.get('format', '').lower() in ['csv', 'xlsx']:
                    target_resource = res
                    break
        
        if not target_resource:
            logger.warning(f"No suitable resource found for Health Metrics {year}")
            return False
            
        url = target_resource.get('url')
        logger.info(f"Downloading resource: {target_resource.get('title')} from {url}")
        
        # Download file
        response = self.datagouv_connector.get(url)
        if not response:
            logger.error(f"Failed to download Health Metrics for {year}")
            return False
            
        # Save file
        ext = target_resource.get('format', 'csv').lower()
        filename = f"health_metrics.{ext}"
        save_path = self.ensure_year_directory(year) / filename
        
        try:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Saved Health Metrics to {save_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save file: {e}")
            return False

    def download_finess_data(self, year: int) -> bool:
        """
        Download FINESS data (Establishment directory).

        Fetches the current geographic FINESS extraction from data.gouv.fr.
        Note that FINESS data is typically a live snapshot; this method saves the
        current state into the year-specific folder to serve as a vintage for that year.

        Args:
            year (int): Target year directory to save the data in.

        Returns:
            bool: True if the data was successfully downloaded and saved, False otherwise.
        """
        logger.info(f"Downloading FINESS data for {year} (using current snapshot)")
        
        # Use existing logic in DataGouvConnector but save to file instead of returning DF
        dataset_info = self.datagouv_connector.get_dataset_info(self.datagouv_connector.finess_dataset_id)
        if not dataset_info:
            return False
            
        api_keyword = config.datagouv.finess_resource_keyword
        # Keyword "extraction" usually gets the main file
        
        csv_url = self.datagouv_connector.find_csv_resource(dataset_info, api_keyword)
        if not csv_url:
            return False
            
        response = self.datagouv_connector.get(csv_url)
        if not response:
            return False
            
        save_path = self.ensure_year_directory(year) / "finess.csv"
        try:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Saved FINESS data to {save_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save FINESS data: {e}")
            return False

    def download_has_certification(self, year: int) -> bool:
        """
        Download HAS Certification data.

        Downloads certification decisions (`demarche.csv`) and establishment geolocation
        links (`etablissement-geo.csv`) to ensuring mappability to FINESS numbers.
        Currently targets the 2021-2025 certification repository.

        Args:
            year (int): Target year directory to save the data in.

        Returns:
            bool: True if at least one resource was successfully downloaded.
        """
        logger.info(f"Downloading HAS Certification data for {year}")
        
        dataset_id = None
        if 2021 <= year <= 2025:
            dataset_id = config.datagouv.has_cert_2021_2025_id
        # Add logic for older years if needed, e.g. 2014-2020 dataset
        
        if not dataset_id:
            logger.warning(f"No HAS dataset configuration for year {year}")
            return False
            
        dataset_info = self.datagouv_connector.get_dataset_info(dataset_id)
        if not dataset_info:
            return False
            
        # We need both the decisions (demarche.csv) and the establishment link (etablissement-geo.csv)
        resources = dataset_info.get('resources', [])
        
        files_to_download = {
            'demarche.csv': 'has_demarche.csv',
            'etablissement-geo.csv': 'has_etab_geo.csv'
        }
        
        success_count = 0
        
        for resource_name, local_name in files_to_download.items():
            target_resource = None
            for res in resources:
                # specific title check or strict filename check if possible (url usually contains filename)
                # HAS resources seem to be named clearly in title too
                if resource_name in res.get('title', '').lower() or resource_name in res.get('url', '').lower():
                    target_resource = res
                    break
            
            if target_resource:
                url = target_resource.get('url')
                response = self.datagouv_connector.get(url)
                if response:
                    save_path = self.ensure_year_directory(year) / local_name
                    try:
                        with open(save_path, 'wb') as f:
                            f.write(response.content)
                        logger.info(f"Saved {local_name} to {save_path}")
                        success_count += 1
                    except Exception as e:
                        logger.error(f"Failed to save {local_name}: {e}")
            else:
                 logger.warning(f"Could not find resource matching {resource_name}")

        return success_count > 0

    def run_year_ingestion(self, year: int):
        """
        Run full ingestion for a specific year.
        
        Args:
            year: Year to process.
        """
        logger.info(f"Starting ingestion for year {year}")
        
        self.download_health_metrics(year)
        self.download_finess_data(year)
        self.download_has_certification(year)
        
        logger.info(f"Completed ingestion for {year}")

    def run_multi_year_ingestion(self, start_year: int, end_year: int):
        """
        Run ingestion for a range of years.
        
        Args:
            start_year: Start year (inclusive)
            end_year: End year (inclusive)
        """
        logger.info(f"Starting multi-year ingestion from {start_year} to {end_year}")
        for year in range(start_year, end_year + 1):
            self.run_year_ingestion(year)
