"""
Data Cleaner: Raw → Bronze Layer

Handles basic data cleaning operations:
- Load raw data files
- Handle encoding issues
- Standardize column names
- Remove duplicates
- Basic data type conversions
- Save cleaned data to bronze layer
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Clean raw data and produce bronze layer output.
    
    Raw data comes from external sources (APIs, downloads) and may have:
    - Encoding issues
    - Inconsistent column names
    - Duplicates
    - Messy formatting
    
    Bronze layer provides cleaned, standardized data ready for transformation.
    """
    
    def __init__(self, raw_base_path: str, bronze_base_path: str):
        """
        Initialize DataCleaner.
        
        Args:
            raw_base_path: Path to raw data directory
            bronze_base_path: Path to bronze data directory
        """
        self.raw_base_path = Path(raw_base_path)
        self.bronze_base_path = Path(bronze_base_path)
    
    def clean_finess(self, year: int) -> Optional[pd.DataFrame]:
        """
        Clean FINESS establishment data.
        
        Operations:
        - Load with correct encoding and separator
        - Assign proper column names based on official FINESS structure
        - Standardize column names (lowercase, strip)
        - Remove duplicates
        - Handle malformed lines
        
        Args:
            year: Year to process
            
        Returns:
            Cleaned DataFrame or None if file not found
        """
        file_path = self.raw_base_path / str(year) / "finess.csv"
        if not file_path.exists():
            logger.error(f"FINESS file not found: {file_path}")
            return None
        
        try:
            logger.info(f"Cleaning FINESS data for year {year}...")
            
            # Define official FINESS column names based on data.gouv.fr structure
            # Source: https://www.data.gouv.fr/datasets/finess-extraction-des-entites-juridiques
            finess_columns = [
                'structureet',           # 0: Structure type
                'finess_et',             # 1: FINESS Establishment ID
                'finess_ej',             # 2: FINESS Legal Entity ID
                'rs',                    # 3: Short name (Raison Sociale)
                'rslongue',              # 4: Long name (Raison Sociale Longue)
                'complrs',               # 5: Complement RS
                'compldistrib',          # 6: Complement distribution
                'numvoie',               # 7: Street number
                'typvoie',               # 8: Street type (rue, avenue, etc.)
                'voie',                  # 9: Street name
                'compvoie',              # 10: Street complement
                'lieuditbp',             # 11: Place/BP
                'commune',               # 12: Municipality code
                'departement',           # 13: Department code
                'libdepartement',        # 14: Department name
                'ligneacheminement',     # 15: Postal routing line (CP + ville)
                'telephone',             # 16: Phone number
                'telecopie',             # 17: Fax number
                'categetab',             # 18: Establishment category code
                'libcategetab',          # 19: Establishment category name
                'categagretab',          # 20: Aggregated category code
                'libcategagretab',       # 21: Aggregated category name
                'siret',                 # 22: SIRET number
                'codeape',               # 23: APE code
                'codemft',               # 24: MFT code
                'libmft',                # 25: MFT label
                'codesph',               # 26: SPH code
                'libsph',                # 27: SPH label (category detail)
                'dateouv',               # 28: Opening date
                'dateautor',             # 29: Authorization date
                'datemaj',               # 30: Last update date
                'numuai'                 # 31: UAI number
            ]
            
            # Load with proper encoding and handling of bad lines
            df = pd.read_csv(
                file_path, 
                sep=';', 
                encoding='utf-8', 
                header=1, 
                low_memory=False, 
                on_bad_lines='warn',
                names=finess_columns  # Assign proper column names
            )
            
            # Column names are already assigned and standardized
            
            # Remove exact duplicates
            initial_count = len(df)
            df = df.drop_duplicates()
            duplicates_removed = initial_count - len(df)
            
            if duplicates_removed > 0:
                logger.info(f"  Removed {duplicates_removed} duplicate rows")
            
            # Save to bronze
            bronze_path = self.bronze_base_path / str(year)
            bronze_path.mkdir(parents=True, exist_ok=True)
            output_file = bronze_path / "finess_clean.csv"
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"  ✓ Cleaned FINESS saved to {output_file}")
            logger.info(f"  Records: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"  ✗ Error cleaning FINESS data: {e}")
            return None
    
    def clean_has_demarche(self, year: int) -> Optional[pd.DataFrame]:
        """
        Clean HAS demarche (certification process) data.
        
        Operations:
        - Load CSV with proper encoding
        - Clean column names (remove BOM, quotes, normalize)
        - Remove duplicates
        
        Args:
            year: Year to process
            
        Returns:
            Cleaned DataFrame or None if file not found
        """
        file_path = self.raw_base_path / str(year) / "has_demarche.csv"
        if not file_path.exists():
            logger.error(f"HAS demarche file not found: {file_path}")
            return None
        
        try:
            logger.info(f"Cleaning HAS demarche data for year {year}...")
            
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Clean column names (remove BOM, quotes, normalize)
            df.columns = [
                c.lower()
                .replace('ï»¿', '')  # Remove BOM
                .replace('"', '')     # Remove quotes
                .strip()              # Remove whitespace
                for c in df.columns
            ]
            
            # Remove duplicates
            initial_count = len(df)
            df = df.drop_duplicates()
            duplicates_removed = initial_count - len(df)
            
            if duplicates_removed > 0:
                logger.info(f"  Removed {duplicates_removed} duplicate rows")
            
            # Save to bronze
            bronze_path = self.bronze_base_path / str(year)
            bronze_path.mkdir(parents=True, exist_ok=True)
            output_file = bronze_path / "has_demarche_clean.csv"
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"  ✓ Cleaned HAS demarche saved to {output_file}")
            logger.info(f"  Records: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"  ✗ Error cleaning HAS demarche data: {e}")
            return None
    
    def clean_has_etab_geo(self, year: int) -> Optional[pd.DataFrame]:
        """
        Clean HAS establishment geography data.
        
        Operations:
        - Load CSV with proper encoding
        - Clean column names (remove BOM, quotes, normalize)
        - Remove duplicates
        
        Args:
            year: Year to process
            
        Returns:
            Cleaned DataFrame or None if file not found
        """
        file_path = self.raw_base_path / str(year) / "has_etab_geo.csv"
        if not file_path.exists():
            logger.error(f"HAS etab geo file not found: {file_path}")
            return None
        
        try:
            logger.info(f"Cleaning HAS establishment geo data for year {year}...")
            
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Clean column names (remove BOM, quotes, normalize)
            df.columns = [
                c.lower()
                .replace('ï»¿', '')  # Remove BOM
                .replace('"', '')     # Remove quotes
                .strip()              # Remove whitespace
                for c in df.columns
            ]
            
            # Remove duplicates
            initial_count = len(df)
            df = df.drop_duplicates()
            duplicates_removed = initial_count - len(df)
            
            if duplicates_removed > 0:
                logger.info(f"  Removed {duplicates_removed} duplicate rows")
            
            # Save to bronze
            bronze_path = self.bronze_base_path / str(year)
            bronze_path.mkdir(parents=True, exist_ok=True)
            output_file = bronze_path / "has_etab_geo_clean.csv"
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"  ✓ Cleaned HAS etab geo saved to {output_file}")
            logger.info(f"  Records: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"  ✗ Error cleaning HAS etab geo data: {e}")
            return None
    
    def clean_health_metrics(self, year: int) -> Optional[pd.DataFrame]:
        """
        Clean health metrics (IQSS) data.
        
        Operations:
        - Load Excel file
        - Standardize column names (lowercase, remove accents, underscores)
        - Clean categorical values (remove numeric prefixes like "1- ")
        - Remove duplicates
        
        Args:
            year: Year to process
            
        Returns:
            Cleaned DataFrame or None if file not found
        """
        file_path = self.raw_base_path / str(year) / "health_metrics.xlsx"
        if not file_path.exists():
            logger.error(f"Health metrics file not found: {file_path}")
            return None
        
        try:
            logger.info(f"Cleaning health metrics data for year {year}...")
            
            # Load Excel file
            df = pd.read_excel(file_path)
            
            # Standardize column names
            df.columns = (
                df.columns.astype(str)
                .str.lower()
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore').str.decode('utf-8')
                .str.replace(r'[^\w\s]', '', regex=True)
                .str.replace(r'\s+', '_', regex=True)
                .str.strip()
            )
            
            # Clean categorical values - remove numeric prefixes
            # Examples: "1- Oui" -> "Oui", "2- Facultatif" -> "Facultatif"
            object_cols = df.select_dtypes(include=['object']).columns
            for col in object_cols:
                df[col] = df[col].astype(str).str.replace(r'^\d+\s*-\s*', '', regex=True)
                # Convert 'nan' strings back to None
                df.loc[df[col] == 'nan', col] = None
            
            # Convert numeric columns
            for col in df.columns:
                if 'score' in col or 'taux' in col or 'valeur' in col:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert 'nb' columns to integer (nullable Int64 to handle NaN)
            for col in df.columns:
                if col.startswith('nb'):
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            
            # Remove duplicates
            initial_count = len(df)
            df = df.drop_duplicates()
            duplicates_removed = initial_count - len(df)
            
            if duplicates_removed > 0:
                logger.info(f"  Removed {duplicates_removed} duplicate rows")
            
            # Save to bronze
            bronze_path = self.bronze_base_path / str(year)
            bronze_path.mkdir(parents=True, exist_ok=True)
            output_file = bronze_path / "health_metrics_clean.csv"
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            logger.info(f"  ✓ Cleaned health metrics saved to {output_file}")
            logger.info(f"  Records: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"  ✗ Error cleaning health metrics data: {e}")
            return None
    
    def clean_year(self, year: int) -> dict:
        """
        Clean all data sources for a given year.
        
        Args:
            year: Year to process
            
        Returns:
            Dictionary with cleaned DataFrames
        """
        logger.info("=" * 60)
        logger.info(f"CLEANING DATA FOR YEAR {year} (Raw → Bronze)")
        logger.info("=" * 60)
        
        results = {}
        
        # Clean FINESS
        df_finess = self.clean_finess(year)
        results['finess'] = df_finess
        
        # Clean HAS data
        df_has_demarche = self.clean_has_demarche(year)
        results['has_demarche'] = df_has_demarche
        
        df_has_etab = self.clean_has_etab_geo(year)
        results['has_etab_geo'] = df_has_etab
        
        # Clean health metrics
        df_metrics = self.clean_health_metrics(year)
        results['health_metrics'] = df_metrics
        
        logger.info("=" * 60)
        logger.info(f"CLEANING COMPLETE FOR YEAR {year}")
        logger.info("=" * 60)
        
        return results
