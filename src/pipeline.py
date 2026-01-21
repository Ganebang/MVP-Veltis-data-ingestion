"""
Data pipeline orchestrator.

Main entry point for managing end-to-end data ingestion workflow
from multiple sources through transformation to output.
"""

import logging
from typing import Optional, Dict
from datetime import datetime
import pandas as pd

from src.connectors.datagouv_api import DataGouvConnector
from src.connectors.has_connector import HASConnector
# from src.transformers.data_blender import process_and_merge  # Legacy - use data_cleaner instead
from src.config import config


logger = logging.getLogger(__name__)


class DataPipeline:
    """Main orchestration class for data ingestion pipeline."""
    
    def __init__(self):
        """Initialize pipeline with configured data sources."""
        self.config = config
        self.start_time = None
        self.end_time = None
        self.results = {}
        
        # Initialize connectors
        self.datagouv = DataGouvConnector(config.datagouv)
        self.has = HASConnector(config.has)
        self.financial = FinancialConnector(config.banque_de_france)
    
    def fetch_source_data(self, save_raw: bool = True) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Fetch raw data from all configured sources.
        
        Args:
            save_raw: Whether to save raw data to data/raw folder
        
        Returns:
            Dictionary with DataFrames from each source
        """
        logger.info("=" * 60)
        logger.info("FETCHING DATA FROM ALL SOURCES")
        logger.info("=" * 60)
        
        source_data = {}
        raw_data_path = self.config.pipeline.raw_data_path
        
        # Fetch FINESS data
        logger.info("\n[1/4] Fetching FINESS data from Data.gouv...")
        try:
            df_finess = self.datagouv.fetch_data()
            if df_finess is not None:
                logger.info(f"✓ FINESS data fetched: {len(df_finess)} records")
                source_data['finess'] = df_finess
                # Save raw data
                if save_raw:
                    finess_path = f"{raw_data_path}/finess_raw.csv"
                    df_finess.to_csv(finess_path, index=False, encoding='utf-8')
                    logger.info(f"✓ Raw FINESS data saved to {finess_path}")
            else:
                logger.error("✗ Failed to fetch FINESS data")
                source_data['finess'] = None
        except Exception as e:
            logger.error(f"✗ Error fetching FINESS: {e}")
            source_data['finess'] = None
        
        # Fetch HAS data
        logger.info("\n[2/4] Fetching HAS certification data...")
        try:
            df_has = self.has.fetch_data()
            if df_has is not None:
                logger.info(f"✓ HAS data fetched: {len(df_has)} records")
                source_data['has'] = df_has
                # Save raw data
                if save_raw:
                    has_path = f"{raw_data_path}/has_raw.csv"
                    df_has.to_csv(has_path, index=False, encoding='utf-8')
                    logger.info(f"✓ Raw HAS data saved to {has_path}")
            else:
                logger.warning("⚠ No HAS data available")
                source_data['has'] = None
        except Exception as e:
            logger.error(f"✗ Error fetching HAS: {e}")
            source_data['has'] = None
        
        # Fetch Financial data
        logger.info("\n[3/4] Fetching financial data...")
        try:
            df_financial = self.financial.fetch_data()
            if df_financial is not None:
                logger.info(f"✓ Financial data fetched: {len(df_financial)} records")
                source_data['financial'] = df_financial
                # Save raw data
                if save_raw:
                    financial_path = f"{raw_data_path}/financial_raw.csv"
                    df_financial.to_csv(financial_path, index=False, encoding='utf-8')
                    logger.info(f"✓ Raw financial data saved to {financial_path}")
            else:
                logger.warning("⚠ No financial data available")
                source_data['financial'] = None
        except Exception as e:
            logger.error(f"✗ Error fetching financial data: {e}")
            source_data['financial'] = None
        
        logger.info("\n" + "=" * 60)
        logger.info("DATA FETCHING COMPLETE")
        logger.info("=" * 60)
        
        return source_data
    
    def transform_data(self, source_data: Dict[str, Optional[pd.DataFrame]]) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Transform raw data into Veltis dictionary format.
        
        Args:
            source_data: Dictionary of raw DataFrames from sources
            
        Returns:
            Dictionary with transformed DataFrames
        """
        logger.info("\n" + "=" * 60)
        logger.info("TRANSFORMING DATA")
        logger.info("=" * 60)
        
        try:
            transformed = process_and_merge(
                source_data.get('finess'),
                source_data.get('has')
            )
            
            if transformed is None:
                logger.error("✗ Data transformation failed")
                return None
            
            logger.info("✓ Data transformation complete")
            logger.info(f"  - Etablissements: {len(transformed['etablissements'])} records")
            logger.info(f"  - Qualifications: {len(transformed['qualifications'])} records")
            
            return transformed
            
        except Exception as e:
            logger.error(f"✗ Error during transformation: {e}")
            return None
    
    def validate_output(self, transformed_data: Dict[str, pd.DataFrame]) -> bool:
        """
        Validate transformed data quality.
        
        Args:
            transformed_data: Transformed DataFrames
            
        Returns:
            True if validation passes, False otherwise
        """
        logger.info("\n" + "=" * 60)
        logger.info("VALIDATING OUTPUT DATA")
        logger.info("=" * 60)
        
        try:
            # Check etablissements
            df_etab = transformed_data.get('etablissements')
            if df_etab is not None and len(df_etab) > 0:
                logger.info(f"✓ Etablissements validation: {len(df_etab)} records")
                
                # Check for required columns
                required_cols = ['vel_id', 'finess_et', 'siret', 'raison_sociale']
                for col in required_cols:
                    if col not in df_etab.columns:
                        logger.error(f"✗ Missing required column: {col}")
                        return False
                
                # Check for null values in key columns
                for col in required_cols:
                    null_count = df_etab[col].isna().sum()
                    if null_count > 0:
                        logger.warning(f"⚠ {null_count} null values in {col}")
            else:
                logger.error("✗ No etablissements data")
                return False
            
            # Check qualifications (if present)
            df_qual = transformed_data.get('qualifications')
            if df_qual is not None and len(df_qual) > 0:
                logger.info(f"✓ Qualifications validation: {len(df_qual)} records")
            
            logger.info("✓ All validations passed")
            return True
            
        except Exception as e:
            logger.error(f"✗ Validation error: {e}")
            return False
    
    def save_output(self, transformed_data: Dict[str, pd.DataFrame], 
                    output_path: Optional[str] = None) -> bool:
        """
        Save transformed data to files.
        
        Args:
            transformed_data: Transformed DataFrames
            output_path: Optional output directory path
            
        Returns:
            True if save successful, False otherwise
        """
        logger.info("\n" + "=" * 60)
        logger.info("SAVING OUTPUT DATA")
        logger.info("=" * 60)
        
        if output_path is None:
            output_path = self.config.pipeline.processed_data_path
        
        try:
            # Save etablissements
            df_etab = transformed_data.get('etablissements')
            if df_etab is not None and len(df_etab) > 0:
                etab_path = f"{output_path}/etablissements.csv"
                df_etab.to_csv(etab_path, index=False, encoding='utf-8')
                logger.info(f"✓ Etablissements saved to {etab_path}")
            
            # Save qualifications
            df_qual = transformed_data.get('qualifications')
            if df_qual is not None and len(df_qual) > 0:
                qual_path = f"{output_path}/qualifications.csv"
                df_qual.to_csv(qual_path, index=False, encoding='utf-8')
                logger.info(f"✓ Qualifications saved to {qual_path}")
            
            logger.info("✓ All data saved successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error saving data: {e}")
            return False
    
    def run(self, save_output: bool = True, save_raw: bool = True) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Execute complete data pipeline.
        
        Args:
            save_output: Whether to save output to files
            save_raw: Whether to save raw data to data/raw folder
        
        Returns:
            Dictionary with transformed DataFrames or None if failed
        """
        self.start_time = datetime.utcnow()
        
        logger.info("\n")
        logger.info("#" * 60)
        logger.info("# VELTIS DATA INGESTION PIPELINE - STARTED")
        logger.info(f"# {self.start_time}")
        logger.info("#" * 60)
        logger.info("\n")
        
        try:
            # 1. Fetch data
            # This step downloads raw files from external APIs (Data.gouv, HAS, etc.)
            source_data = self.fetch_source_data(save_raw=save_raw)
            if not any(source_data.values()):
                logger.error("✗ No data fetched from any source")
                return None
            
            # 2. Transform data
            # This step cleans, normalizes, and merges the raw data into our internal schema
            transformed_data = self.transform_data(source_data)
            if transformed_data is None:
                logger.error("✗ Transformation failed")
                return None
            
            # 3. Validate output
            if not self.validate_output(transformed_data):
                logger.error("✗ Validation failed")
                return None
            
            # 4. Save output
            if save_output:
                if not self.save_output(transformed_data):
                    logger.error("✗ Save failed")
                    return None
            
            self.end_time = datetime.utcnow()
            duration = (self.end_time - self.start_time).total_seconds()
            
            logger.info("\n")
            logger.info("#" * 60)
            logger.info("# VELTIS DATA INGESTION PIPELINE - COMPLETED")
            logger.info(f"# Duration: {duration:.2f} seconds")
            logger.info("#" * 60)
            logger.info("\n")
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"✗ Pipeline error: {e}")
            return None
    
    def close(self):
        """Close all connector sessions."""
        logger.info("Closing connector sessions...")
        self.datagouv.close()
        self.has.close()
        self.financial.close()
        logger.info("Connectors closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None):
    """
    Configure logging for the pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
    """
    import sys
    
    logger_root = logging.getLogger()
    logger_root.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    logger_root.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(formatter)
        logger_root.addHandler(file_handler)
