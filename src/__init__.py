"""
Initialization script for the Veltis data ingestion project.
"""

# Version info
__version__ = "1.0.0"
__author__ = "Veltis Team"

# Import main components for easy access
from src.pipeline import setup_logging
from src.config import config
from src.models.schemas import (
    Etablissement,
    Qualification,
    FinancialData,
    HealthMetrics,
    CategorieEtablissement,
    NiveauCertification,
)

__all__ = [
    'setup_logging',
    'config',
    'Etablissement',
    'Qualification',
    'FinancialData',
    'HealthMetrics',
    'CategorieEtablissement',
    'NiveauCertification',
]
