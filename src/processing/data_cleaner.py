
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict
import uuid
import re

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Process and clean raw health data (FINESS, HAS, Health Metrics).
    Produces normalized tables Etablissement, Qualification, and HealthMetrics.
    """
    
    def __init__(self, raw_base_path: str):
        self.raw_base_path = Path(raw_base_path)
    
    def _generate_uuid(self, val):
        return uuid.uuid4()

    def _map_category(self, val):
        val = str(val).lower()
        if 'public' in val:
            return 'Public'
        elif 'privé' in val or 'prive' in val:
            return 'Privé'
        elif 'espic' in val:
            return 'ESPIC'
        else:
            return 'Autre'

    def load_clean_finess(self, year: int) -> Optional[pd.DataFrame]:
        """
        Load and clean FINESS data for a given year.

        Reads the raw FINESS CSV, handling inconsistent headers and layout.
        Maps columns to the `Etablissement` schema, constructs standardized addresses,
        and generates UUIDs.

        Args:
            year (int): The year directory to read raw data from.

        Returns:
            Optional[pd.DataFrame]: A DataFrame conforming to the `Etablissement` schema,
            or None if the file is missing or unparseable.
        """
        file_path = self.raw_base_path / str(year) / "finess.csv"
        if not file_path.exists():
            logger.error(f"FINESS file not found: {file_path}")
            return None
            
        try:
            # Load with implicit index assumption based on inspection
            # Index 1: finess_et
            # Index 4: raison_sociale (Long name)
            # Index 7, 8, 9: Address parts
            # Index 15: CP + Ville (e.g. "01300 BELLEY")
            # Index 22: SIRET
            # Index 27: Category description
            
            df = pd.read_csv(file_path, sep=';', encoding='latin-1', header=1, low_memory=False, on_bad_lines='warn')
            
            # Check if columns are sufficient
            if len(df.columns) < 28:
                logger.warning("FINESS data has fewer columns than expected. Parsing might be incorrect.")
            
            clean_df = pd.DataFrame()
            
            # Extract basic columns safely
            clean_df['finess_et'] = df.iloc[:, 1].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(9)
            clean_df['siret'] = df.iloc[:, 22].astype(str).str.replace(r'\.0$', '', regex=True)
            clean_df['raison_sociale'] = df.iloc[:, 4].fillna(df.iloc[:, 3]) # Fallback to short name
            
            # Address construction
            # Concatenate non-null address parts
            addr_parts = df.iloc[:, [7, 8, 9, 11]].apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
            clean_df['adresse_postale'] = addr_parts
            
            # CP and City
            # Split "01300 BELLEY" -> "01300" and "BELLEY"
            cp_ville = df.iloc[:, 15].astype(str)
            clean_df['code_postal'] = cp_ville.str.extract(r'^(\d{5})')[0]
            # If extraction failed, might be missing 0 or short
            clean_df.loc[clean_df['code_postal'].isna(), 'code_postal'] = cp_ville.str.extract(r'^(\d+)')[0]
            
            # Category
            clean_df['categorie_etab'] = df.iloc[:, 27].apply(self._map_category)
            
            # Generate UUIDs
            clean_df['vel_id'] = [uuid.uuid4() for _ in range(len(clean_df))]
            
            # Metadata
            clean_df['date_created'] = pd.Timestamp.now()
            clean_df['date_updated'] = pd.Timestamp.now()
            clean_df['source'] = 'Data.gouv'
            
            logger.info(f"Loaded and cleaned FINESS (Etablissement) data for {year}: {len(clean_df)} records")
            return clean_df
            
        except Exception as e:
            logger.error(f"Error processing FINESS data: {e}")
            return None

    def load_clean_has(self, year: int) -> Optional[pd.DataFrame]:
        """
        Load and merge HAS data to produce Qualification data.

        Reads `has_demarche.csv` and `has_etab_geo.csv`, merges them on `code_demarche`,
        and links them to canonical FINESS numbers.

        Args:
            year (int): The year directory to read raw data from.

        Returns:
            Optional[pd.DataFrame]: A DataFrame conforming to the `Qualification` schema
            (pre-merge state), or None if files are missing.
        """
        year_path = self.raw_base_path / str(year)
        demarche_path = year_path / "has_demarche.csv"
        geo_path = year_path / "has_etab_geo.csv"
        
        if not demarche_path.exists() or not geo_path.exists():
             logger.error(f"HAS files missing in {year_path}")
             return None
             
        try:
            df_dem = pd.read_csv(demarche_path)
            df_dem.columns = [c.lower().replace('ï»¿', '').replace('"', '').strip() for c in df_dem.columns]
            
            df_geo = pd.read_csv(geo_path)
            df_geo.columns = [c.lower().replace('ï»¿', '').replace('"', '').strip() for c in df_geo.columns]
            
            if 'code_demarche' not in df_dem.columns or 'code_demarche' not in df_geo.columns:
                logger.error("Missing code_demarche column in HAS files")
                return None
                
            merged = pd.merge(df_dem, df_geo, on='code_demarche', how='inner')
            
            # Check for FINESS column in merged data (from geo)
            # Usually 'finess_eg' or 'finess_ej'
            # We map to finess_et to link with Etablissement
            finess_col = 'finess_eg' if 'finess_eg' in merged.columns else 'finess_ej'
            if finess_col not in merged.columns:
                logger.error("No FINESS column found in HAS geo data")
                return None
            
            clean_df = pd.DataFrame()
            clean_df['finess_et_link'] = merged[finess_col].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(9)
            
            # Qualification Columns
            clean_df['qua_id'] = [uuid.uuid4() for _ in range(len(clean_df))]
            clean_df['niveau_certification'] = merged['decision_de_la_cces']
            
            # Date parsing
            if 'date_de_decision' in merged.columns:
                 clean_df['date_visite'] = pd.to_datetime(merged['date_de_decision'], dayfirst=True, errors='coerce')
            
            clean_df['source'] = 'HAS'
            clean_df['freshness'] = 'Bisannuelle'
            clean_df['date_created'] = pd.Timestamp.now()
            clean_df['date_updated'] = pd.Timestamp.now()
            
            logger.info(f"Loaded and cleaned HAS (Qualification) data for {year}: {len(clean_df)} records")
            return clean_df
            
        except Exception as e:
            logger.error(f"Error processing HAS data: {e}")
            return None

    def load_clean_health_metrics(self, year: int) -> Optional[pd.DataFrame]:
        """
        Load and clean Health Metrics (IQSS) data.
        
        Args:
            year (int): Year to process.
            
        Returns:
            Optional[pd.DataFrame]: Cleaned metrics data.
        """
        file_path = self.raw_base_path / str(year) / "health_metrics.xlsx"
        if not file_path.exists():
            logger.error(f"Health Metrics file not found: {file_path}")
            return None
            
        try:
            # Load Excel - headers are often on row 1 (0-indexed) or messy
            df = pd.read_excel(file_path)
            
            # Standardize column headers
            # 1. Lowercase
            # 2. Replace spaces/special chars with underscores
            # 3. Remove accents
            df.columns = (df.columns.astype(str)
                          .str.lower()
                          .str.normalize('NFKD')
                          .str.encode('ascii', errors='ignore').str.decode('utf-8')
                          .str.replace(r'[^\w\s]', '', regex=True)
                          .str.replace(r'\s+', '_', regex=True)
                          .str.strip())
            
            # Normalize FINESS column if present
            # Look for common variations
            finess_col = next((c for c in df.columns if 'finess' in c), None)
            
            clean_df = df.copy()
            if finess_col:
                clean_df['finess_et_link'] = clean_df[finess_col].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(9)
                # Drop original if messy, or keep? Keeping for trace is safer but renaming for clarity
                if finess_col != 'finess_et_link':
                    clean_df.rename(columns={finess_col: 'finess_raw'}, inplace=True)
            else:
                logger.warning("No FINESS column found in Health Metrics. Linking will be impossible.")
            
            # Type Enforcement for Numeric Columns
            # Detect potential numeric columns (often contain text like 'NC', '%', etc.)
            for col in clean_df.columns:
                if 'score' in col or 'taux' in col or 'valeur' in col:
                    clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
            
            # Metadata
            clean_df['metric_id'] = [uuid.uuid4() for _ in range(len(clean_df))]
            clean_df['source_file'] = 'health_metrics.xlsx'
            clean_df['processed_at'] = pd.Timestamp.now()
            
            logger.info(f"Loaded and cleaned Health Metrics for {year}: {len(clean_df)} records")
            return clean_df
            
        except Exception as e:
            logger.error(f"Error processing Health Metrics: {e}")
            return None

    def process_year(self, year: int) -> Dict[str, pd.DataFrame]:
        """
        Process all data for a specific year and save normalized tables.

        Orchestrates the loading, cleaning, and merging of FINESS and HAS data.
        Produces and saves `etablissements.csv` and `qualifications.csv`.

        Args:
            year (int): The year to process.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary containing the processed DataFrames:
                - 'etablissements': Normalized establishment data.
                - 'qualifications': Normalized qualification data linked to establishments.
                - 'health_metrics': Processed IQSS metrics.
        """
        df_etab = self.load_clean_finess(year)
        df_qual_raw = self.load_clean_has(year)
        df_metrics_raw = self.load_clean_health_metrics(year)
        
        if df_etab is None:
            logger.error("Cannot proceed without Etablissement data.")
            return
        
        # Link Qualifications to Etablissements via finess_et -> vel_id
        df_qual = pd.DataFrame()
        if df_qual_raw is not None:
            # Merge to get vel_id
            merged_qual = pd.merge(
                df_qual_raw, 
                df_etab[['finess_et', 'vel_id']], 
                left_on='finess_et_link', 
                right_on='finess_et', 
                how='inner'
            )
            
            # Select final Qualification columns
            qual_cols = ['qua_id', 'vel_id', 'niveau_certification', 'date_visite', 
                         'source', 'freshness', 'date_created', 'date_updated']
            # Add placeholders for missing schema fields
            merged_qual['url_rapport'] = None
            merged_qual['score_satisfaction'] = None
            
            df_qual = merged_qual[qual_cols + ['url_rapport', 'score_satisfaction']]
            logger.info(f"Linked {len(df_qual)} qualifications to establishments")

        # Process Health Metrics linking
        df_metrics = pd.DataFrame()
        if df_metrics_raw is not None:
            # Link to Etablissements via finess_et
            if 'finess_et_link' in df_metrics_raw.columns:
                merged_metrics = pd.merge(
                    df_metrics_raw,
                    df_etab[['finess_et', 'vel_id']],
                    left_on='finess_et_link',
                    right_on='finess_et',
                    how='inner'
                )
                
                # Keep all columns from raw metrics, plus vel_id, minus finess join keys to avoid clutter
                # But ensure we keep our cleaning metadata
                cols_to_keep = [c for c in merged_metrics.columns if c not in ['finess_et', 'finess_et_link']]
                # Ensure vel_id is first for readability
                cols_final = ['vel_id'] + [c for c in cols_to_keep if c != 'vel_id']
                
                df_metrics = merged_metrics[cols_final]
                logger.info(f"Linked {len(df_metrics)} health metrics records to establishments")
            else:
                logger.warning("Skipping Health Metrics linking: No link key found.")

        # Save outputs
        self.save_processed(df_etab, df_qual, df_metrics, year)
        return {'etablissements': df_etab, 'qualifications': df_qual, 'health_metrics': df_metrics}

    def save_processed(self, df_etab: pd.DataFrame, df_qual: pd.DataFrame, df_metrics: pd.DataFrame, year: int):
         save_path = self.raw_base_path.parent / "processed" / str(year)
         save_path.mkdir(parents=True, exist_ok=True)
         
         etab_path = save_path / "etablissements.csv"
         df_etab.to_csv(etab_path, index=False)
         logger.info(f"Saved Etablissements to {etab_path}")
         
         if not df_qual.empty:
             qual_path = save_path / "qualifications.csv"
             df_qual.to_csv(qual_path, index=False)
             logger.info(f"Saved Qualifications to {qual_path}")

         if not df_metrics.empty:
             metrics_path = save_path / "health_metrics.csv"
             df_metrics.to_csv(metrics_path, index=False)
             logger.info(f"Saved Health Metrics to {metrics_path}")
