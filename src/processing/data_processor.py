
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict
import uuid
import re

logger = logging.getLogger(__name__)

from src.models.schemas import Etablissement, Qualification, HealthMetrics
import dataclasses

class DataProcessor:
    """
    Process bronze layer data into final processed output.
    
    Reads cleaned data from bronze layer and applies:
    - Schema enforcement
    - UUID generation
    - Data merging and linking
    - Derived field creation
    
    Produces normalized tables: Etablissement, Qualification, and HealthMetrics.
    """
    
    def __init__(self, bronze_base_path: str):
        """
        Initialize DataProcessor.
        
        Args:
            bronze_base_path: Path to bronze data directory
        """
        self.bronze_base_path = Path(bronze_base_path)
    
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
            
    def _enforce_schema(self, df: pd.DataFrame, schema_class) -> pd.DataFrame:
        """
        Enforce strict schema on DataFrame.
        - Keeps only columns defined in schema.
        - Adds missing columns with None.
        - Orders columns according to schema.
        """
        if df.empty:
            return pd.DataFrame()
            
        schema_fields = [f.name for f in dataclasses.fields(schema_class)]
        
        # Add missing fields
        for field in schema_fields:
            if field not in df.columns:
                df[field] = None
                
        # Select and reorder strictly
        return df[schema_fields]

    def load_clean_finess(self, year: int) -> Optional[pd.DataFrame]:
        """
        Load and transform FINESS data from bronze layer.

        Reads cleaned FINESS CSV from bronze, transforms to Etablissement schema.
        Applies business logic:
        - Constructs standardized addresses
        - Generates UUIDs
        - Extracts department codes
        - Maps categories

        Args:
            year (int): The year to process.

        Returns:
            Optional[pd.DataFrame]: A DataFrame conforming to the `Etablissement` schema,
            or None if the file is missing or unparseable.
        """
        file_path = self.bronze_base_path / str(year) / "finess_clean.csv"
        if not file_path.exists():
            logger.error(f"FINESS file not found: {file_path}")
            return None
            
        try:
            # Load cleaned data from bronze layer
            # Column names are already cleaned and standardized
            df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
            
            clean_df = pd.DataFrame()
            
            # Extract basic columns using named columns (more maintainable)
            # Strip trailing .0 that sometimes appears when IDs are read as floats
            clean_df['finess_et'] = df['finess_et'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(9)
            clean_df['siret'] = df['siret'].astype(str).str.replace(r'\.0$', '', regex=True)
            
            # Use Long Name, fall back to Short Name if missing
            clean_df['raison_sociale'] = df['rslongue'].fillna(df['rs'])
            
            # Address construction
            # Concatenate non-null address parts to form a full string
            addr_parts = df[['numvoie', 'typvoie', 'voie', 'lieuditbp']].apply(
                lambda x: ' '.join(x.dropna().astype(str)), axis=1
            )
            clean_df['adresse_postale'] = addr_parts
            
            # CP and City Extraction from ligneacheminement
            # Expected format: "01300 BELLEY"
            cp_ville = df['ligneacheminement'].astype(str)
            # Regex to grab the first 5 digits
            clean_df['code_postal'] = cp_ville.str.extract(r'^(\d{5})')[0]
            # Fallback: grab any leading digits if exact 5 not found
            clean_df.loc[clean_df['code_postal'].isna(), 'code_postal'] = cp_ville.str.extract(r'^(\d+)')[0]
            
            # Convert to Int (Nullable)
            clean_df['code_postal'] = pd.to_numeric(clean_df['code_postal'], errors='coerce').astype('Int64')
            
            # Category Mapping
            # Detailed: SPH label (e.g., "Etablissement public de santé")
            clean_df['categorie_detail'] = df['libsph'].astype(str).str.strip()
            # Simplified: Public/Private mapping using helper map function
            clean_df['categorie_etab'] = df['libsph'].apply(self._map_category)
            
            # Geography: Departement extraction
            def extract_dept(cp):
                if pd.isna(cp):
                    return None
                # Cast to int to avoid "1300.0" string from float conversion
                s_cp = str(int(cp)).zfill(5)
                # DOM-TOM (97x) handling
                if s_cp.startswith('97'):
                    return s_cp[:3]
                # Standard mainland departments (first 2 digits)
                return s_cp[:2]

            clean_df['departement'] = clean_df['code_postal'].apply(extract_dept)
            
            # Generate UUIDs for internal linking
            clean_df['vel_id'] = [uuid.uuid4() for _ in range(len(clean_df))]
            
            # Metadata fields
            clean_df['date_created'] = pd.Timestamp.now()
            clean_df['date_updated'] = pd.Timestamp.now()
            clean_df['source'] = 'Data.gouv'
            
            logger.info(f"Transformed FINESS (Etablissement) data for {year}: {len(clean_df)} records")
            return clean_df
            
        except Exception as e:
            logger.error(f"Error processing FINESS data: {e}")
            return None

    def load_clean_has(self, year: int) -> Optional[pd.DataFrame]:
        """
        Load and transform HAS data from bronze layer to produce Qualification data.

        Reads cleaned `has_demarche_clean.csv` and `has_etab_geo_clean.csv`,
        merges them on `code_demarche`, and prepares for linking to FINESS.

        Args:
            year (int): The year to process.

        Returns:
            Optional[pd.DataFrame]: A DataFrame conforming to the `Qualification` schema
            (pre-merge state), or None if files are missing.
        """
        year_path = self.bronze_base_path / str(year)
        demarche_path = year_path / "has_demarche_clean.csv"
        geo_path = year_path / "has_etab_geo_clean.csv"
        
        if not demarche_path.exists() or not geo_path.exists():
             logger.error(f"HAS files missing in {year_path}")
             return None
             
        try:
            # Load cleaned data from bronze - column names already normalized
            df_dem = pd.read_csv(demarche_path, encoding='utf-8')
            df_geo = pd.read_csv(geo_path, encoding='utf-8')
            
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
            
            # Construct URL (Heuristic)
            # Pattern: https://www.has-sante.fr/jcms/c_{code_demarche}
            clean_df['url_rapport'] = merged['code_demarche'].apply(
                lambda x: f"https://www.has-sante.fr/jcms/c_{x}" if pd.notna(x) else None
            )
            
            # Date parsing
            if 'date_de_decision' in merged.columns:
                 clean_df['date_visite'] = pd.to_datetime(merged['date_de_decision'], dayfirst=True, errors='coerce')
            
            clean_df['source'] = 'HAS'
            clean_df['freshness'] = 'Bisannuelle'
            clean_df['date_created'] = pd.Timestamp.now()
            clean_df['date_updated'] = pd.Timestamp.now()
            
            logger.info(f"Transformed HAS (Qualification) data for {year}: {len(clean_df)} records")
            return clean_df
            
        except Exception as e:
            logger.error(f"Error processing HAS data: {e}")
            return None

    def load_clean_health_metrics(self, year: int) -> Optional[pd.DataFrame]:
        """
        Load and transform Health Metrics (IQSS) data from bronze layer.
        
        Args:
            year (int): Year to process.
            
        Returns:
            Optional[pd.DataFrame]: Transformed metrics data.
        """
        file_path = self.bronze_base_path / str(year) / "health_metrics_clean.csv"
        if not file_path.exists():
            logger.error(f"Health Metrics file not found: {file_path}")
            return None
            
        try:
            # Load cleaned CSV from bronze - already normalized
            df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
            
            # Find FINESS column (already normalized in bronze)
            finess_col = next((c for c in df.columns if 'finess' in c), None)
            
            clean_df = df.copy()
            if finess_col:
                # Ensure FINESS format is consistent
                clean_df['finess_et_link'] = clean_df[finess_col].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(9)
            else:
                logger.warning("No FINESS column found in Health Metrics. Linking will be impossible.")

            # Metadata
            clean_df['metric_id'] = [uuid.uuid4() for _ in range(len(clean_df))]
            clean_df['source_file'] = 'health_metrics.xlsx'
            clean_df['processed_at'] = pd.Timestamp.now()
            
            logger.info(f"Transformed Health Metrics for {year}: {len(clean_df)} records")
            return clean_df
            
        except Exception as e:
            logger.error(f"Error processing Health Metrics: {e}")
            return None


    def process_year(self, year: int) -> Dict[str, pd.DataFrame]:
        df_etab = self.load_clean_finess(year)
        df_qual_raw = self.load_clean_has(year)
        df_metrics_raw = self.load_clean_health_metrics(year)
        
        if df_etab is None:
            logger.error("Cannot proceed without Etablissement data.")
            return
        
        # --- ETABLISSEMENT ---
        # Already mostly aligned, but run enforcement to strip extra temp cols
        df_etab_final = self._enforce_schema(df_etab, Etablissement)

        # --- QUALIFICATIONS ---
        df_qual_final = pd.DataFrame()
        if df_qual_raw is not None:
             # Merge to get vel_id
            merged_qual = pd.merge(
                df_qual_raw, 
                df_etab[['finess_et', 'vel_id']], 
                left_on='finess_et_link', 
                right_on='finess_et', 
                how='inner'
            )
            
            # Construct URL - Moved to load_clean_has
            # merged_qual already has url_rapport if available
            
            # --- MERGE HEALTH METRICS INTO QUALIFICATIONS ---
            # User Change (2026-01-21): Removed score columns from qualifications as they are duplicative/redundant.
            # We revert to just HAS data in this table.
            
            df_qual_final = self._enforce_schema(merged_qual, Qualification)

        # --- HEALTH METRICS ---
        df_metrics_final = pd.DataFrame()
        if df_metrics_raw is not None:
             if 'finess_et_link' in df_metrics_raw.columns:
                merged_metrics = pd.merge(
                    df_metrics_raw,
                    df_etab[['finess_et', 'vel_id']],
                    left_on='finess_et_link',
                    right_on='finess_et',
                    how='inner'
                )
                
                # Metadata
                merged_metrics['annee'] = year
                merged_metrics['source'] = 'IQSS'
                
                # Attempt to map dynamic raw columns to schema if they exist
                # e.g. "score_all_ssr_ajust" might be "score_all_ssr_ajust_2024" or similiar
                # For now, we assume partial names match or we rely on the _enforce checks
                # which will fill None if explicit names don't match.
                
                # If the raw columns are exact matches (which they often are after normalization), it works.
                # If not, we might need more complex mapping logic here. 
                # For this step, we trust the `load_clean_health_metrics` normalization.
                
                df_metrics_final = self._enforce_schema(merged_metrics, HealthMetrics)
                logger.info(f"Linked {len(df_metrics_final)} health metrics records")

        # Save outputs
        self.save_processed(df_etab_final, df_qual_final, df_metrics_final, year)
        return {'etablissements': df_etab_final, 'qualifications': df_qual_final, 'health_metrics': df_metrics_final}

    def save_processed(self, df_etab: pd.DataFrame, df_qual: pd.DataFrame, df_metrics: pd.DataFrame, year: int):
         save_path = self.bronze_base_path.parent / "silver" / str(year)
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
