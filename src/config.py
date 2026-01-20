"""
Configuration management for data ingestion pipeline.

Handles environment variables, API configurations, and data source settings
for different deployment environments (development, staging, production).
"""

import os
import logging
from typing import Optional
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass
class DataGouvConfig:
    """Data.gouv API configuration"""
    base_url: str = "https://www.data.gouv.fr/api/1"
    timeout: int = 30
    # FINESS dataset ID for hospital/clinic registry
    finess_dataset_id: str = "53699569a3a729239d2046eb"
    finess_resource_keyword: str = "extraction"
    csv_separator: str = ";"
    csv_encoding: str = "latin-1"
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables."""
        return cls(
            base_url=os.getenv("DATAGOUV_BASE_URL", "https://www.data.gouv.fr/api/1"),
            timeout=int(os.getenv("DATAGOUV_TIMEOUT", "30")),
            finess_dataset_id=os.getenv("DATAGOUV_FINESS_DATASET_ID", "53699569a3a729239d2046eb"),
            finess_resource_keyword=os.getenv("DATAGOUV_FINESS_KEYWORD", "extraction"),
            csv_separator=os.getenv("DATAGOUV_CSV_SEPARATOR", ";"),
            csv_encoding=os.getenv("DATAGOUV_CSV_ENCODING", "latin-1"),
        )

    # Health Metrics (IQSS) configuration
    iqss_search_pattern: str = "Indicateurs de qualité et de sécurité des soins - recueil {}"
    
    # HAS Certification configuration
    # 2021-2025 dataset ID
    has_cert_2021_2025_id: str = "624aeba41407eb7cadece64e"



@dataclass
class HASConfig:
    """HAS (Haute Autorité de Santé) API configuration"""
    base_url: str = "https://www.has-sante.fr/api"
    timeout: int = 30
    api_key: Optional[str] = None  # If required in future
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables."""
        return cls(
            base_url=os.getenv("HAS_BASE_URL", "https://www.has-sante.fr/api"),
            timeout=int(os.getenv("HAS_TIMEOUT", "30")),
            api_key=os.getenv("HAS_API_KEY", None),
        )


@dataclass
class BanqueDeCommandeConfig:
    """Banque de France & Bpifrance API configuration"""
    base_url: str = "https://api.banque-france.fr"
    timeout: int = 30
    api_key: Optional[str] = None
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables."""
        return cls(
            base_url=os.getenv("BANQUE_DE_FRANCE_BASE_URL", "https://api.banque-france.fr"),
            timeout=int(os.getenv("BANQUE_DE_FRANCE_TIMEOUT", "30")),
            api_key=os.getenv("BANQUE_DE_FRANCE_API_KEY", None),
        )


@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "veltis_data"
    user: str = "veltis_user"
    password: Optional[str] = None
    ssl_mode: str = "prefer"
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables."""
        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "veltis_data"),
            user=os.getenv("DB_USER", "veltis_user"),
            password=os.getenv("DB_PASSWORD", None),
            ssl_mode=os.getenv("DB_SSL_MODE", "prefer"),
        )
    
    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        pwd_part = f":{self.password}" if self.password else ""
        return f"postgresql://{self.user}{pwd_part}@{self.host}:{self.port}/{self.database}?sslmode={self.ssl_mode}"


@dataclass
class PipelineConfig:
    """Data pipeline configuration"""
    # Data freshness settings (in hours)
    finess_refresh_hours: int = 730  # ~30 days
    has_refresh_hours: int = 1440    # ~60 days
    financial_refresh_hours: int = 8760  # 1 year
    
    # Batch processing
    batch_size: int = 100
    max_workers: int = 4  # For concurrent requests
    
    # Retry strategy
    max_retries: int = 3
    retry_backoff_seconds: int = 5
    
    # Data storage
    raw_data_path: str = "/workspaces/MVP-web-scrapping-project/data/raw"
    processed_data_path: str = "/workspaces/MVP-web-scrapping-project/data/processed"
    
    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = None
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables."""
        return cls(
            finess_refresh_hours=int(os.getenv("FINESS_REFRESH_HOURS", "730")),
            has_refresh_hours=int(os.getenv("HAS_REFRESH_HOURS", "1440")),
            financial_refresh_hours=int(os.getenv("FINANCIAL_REFRESH_HOURS", "8760")),
            batch_size=int(os.getenv("BATCH_SIZE", "100")),
            max_workers=int(os.getenv("MAX_WORKERS", "4")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_backoff_seconds=int(os.getenv("RETRY_BACKOFF_SECONDS", "5")),
            raw_data_path=os.getenv("RAW_DATA_PATH", "/workspaces/MVP-web-scrapping-project/data/raw"),
            processed_data_path=os.getenv("PROCESSED_DATA_PATH", "/workspaces/MVP-web-scrapping-project/data/processed"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv("LOG_FILE", None),
        )


class Config:
    """Main configuration class combining all sources."""
    
    def __init__(self):
        self.datagouv = DataGouvConfig.from_env()
        self.has = HASConfig.from_env()
        self.banque_de_france = BanqueDeCommandeConfig.from_env()
        self.database = DatabaseConfig.from_env()
        self.pipeline = PipelineConfig.from_env()
    
    @classmethod
    def load(cls) -> "Config":
        """Load all configurations from environment."""
        return cls()


# Global config instance
config = Config.load()
