"""
Data dictionary schemas for Veltis data ingestion project.

Defines the structure and validation rules for clinical establishment data
from official French sources (Data.gouv, HAS, Banque de France, etc.)
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List
from uuid import UUID
import uuid


class CategorieEtablissement(Enum):
    """Établissement categories from Data.gouv"""
    PUBLIC = "Public"
    PRIVE = "Privé"
    ESPIC = "ESPIC"
    AUTRE = "Autre"


class NiveauCertification(Enum):
    """Certification levels from HAS"""
    HAUTE_QUALITE = "Haute Qualité"
    CERTIFIE = "Certifié"
    NON_CONFORME = "Non Conforme"
    NON_EVALUE = "Non évalué"


@dataclass
class Etablissement:
    """
    ETABLISSEMENT table schema.
    
    Main establishment (hospital/clinic) record from Data.gouv.
    Attributes:
        vel_id: Internal unique identifier (GUID/UUID - Primary Key)
        finess_et: Geographic FINESS number (9 characters) - Pivot Key 1
        siret: Legal establishment identifier (14 characters) - Pivot Key 2
        raison_sociale: Official name of hospital/clinic
        categorie_etab: Category (Public, Privé, ESPIC, etc.)
        adresse_postale: Standardized complete address
        code_postal: Postal code for geographic filtering
        date_created: Data creation timestamp
        date_updated: Last data update timestamp
    """
    vel_id: UUID = field(default_factory=uuid.uuid4)
    finess_et: str = None  # 9 character code
    siret: str = None      # 14 character code
    raison_sociale: str = None
    categorie_etab: CategorieEtablissement = None  # Simplified category (Public, Privé...)
    categorie_detail: str = None  # Raw detailed category from FINESS
    adresse_postale: str = None
    code_postal: Optional[int] = None
    departement: str = None  # Derived from code_postal (e.g. "75")
    date_created: datetime = field(default_factory=datetime.utcnow)
    date_updated: datetime = field(default_factory=datetime.utcnow)
    source: str = "Data.gouv"
    freshness: str = "Mensuelle"

    def validate(self) -> List[str]:
        """
        Validate establishment data.
        
        Returns:
            List of validation error messages. Empty if valid.
        """
        errors = []
        
        if not self.finess_et or len(str(self.finess_et).strip()) != 9:
            errors.append("finess_et must be 9 characters")
            
        if not self.siret or len(str(self.siret).strip()) != 14:
            errors.append("siret must be 14 characters")
            
        if not self.raison_sociale or len(str(self.raison_sociale).strip()) == 0:
            errors.append("raison_sociale cannot be empty")
            
        if not self.code_postal or len(str(self.code_postal).strip()) == 0:
            errors.append("code_postal cannot be empty")
            
        return errors

    def is_valid(self) -> bool:
        """Check if establishment is valid."""
        return len(self.validate()) == 0


@dataclass
class Qualification:
    """
    QUALIFICATION table schema.
    
    Quality certification record from HAS (Haute Autorité de Santé).
    Attributes:
        qua_id: Unique record identifier (GUID/UUID - Primary Key)
        vel_id: Link to ETABLISSEMENT table (Foreign Key)
        niveau_certification: Certification result (Haute Qualité, Certifié, etc.)
        date_visite: Last certification visit date
        url_rapport: Link to complete PDF report on HAS website
        score_satisfaction: Patient satisfaction score (e-Satis)
        date_created: Data creation timestamp
        date_updated: Last data update timestamp
    """
    qua_id: UUID = field(default_factory=uuid.uuid4)
    vel_id: UUID = None  # Foreign key
    niveau_certification: NiveauCertification = NiveauCertification.NON_EVALUE
    date_visite: Optional[datetime] = None
    url_rapport: Optional[str] = None

    date_created: datetime = field(default_factory=datetime.utcnow)
    date_updated: datetime = field(default_factory=datetime.utcnow)
    source: str = "HAS"
    freshness: str = "Bisannuelle"

    def validate(self) -> List[str]:
        """
        Validate qualification data.
        
        Returns:
            List of validation error messages. Empty if valid.
        """
        errors = []
        
        if not self.vel_id:
            errors.append("vel_id (Foreign Key) is required")
            
        if self.url_rapport and not self.url_rapport.startswith(('http://', 'https://')):
            errors.append("url_rapport must be a valid HTTP(S) URL")
            
        return errors

    def is_valid(self) -> bool:
        """Check if qualification is valid."""
        return len(self.validate()) == 0


@dataclass
class FinancialData:
    """
    FINANCIAL_DATA table schema.
    
    Financial information from Banque de France and Bpifrance.
    Attributes:
        fin_id: Unique record identifier (GUID/UUID - Primary Key)
        vel_id: Link to ETABLISSEMENT table (Foreign Key)
        siren: SIREN number (9 characters)
        chiffre_affaires: Annual revenue
        effectifs: Number of employees
        date_bilan: Financial statement date
    """
    fin_id: UUID = field(default_factory=uuid.uuid4)
    vel_id: UUID = None  # Foreign key
    siren: Optional[str] = None
    chiffre_affaires: Optional[float] = None
    effectifs: Optional[int] = None
    date_bilan: Optional[datetime] = None
    date_created: datetime = field(default_factory=datetime.utcnow)
    date_updated: datetime = field(default_factory=datetime.utcnow)
    source: str = "Banque de France / Bpifrance"
    freshness: str = "Annuelle"

    def validate(self) -> List[str]:
        """Validate financial data."""
        errors = []
        
        if not self.vel_id:
            errors.append("vel_id (Foreign Key) is required")
            
        if self.siren and len(str(self.siren).strip()) != 9:
            errors.append("siren must be 9 characters if provided")
            
        if self.chiffre_affaires and self.chiffre_affaires < 0:
            errors.append("chiffre_affaires cannot be negative")
            
        if self.effectifs and self.effectifs < 0:
            errors.append("effectifs cannot be negative")
            
        return errors

    def is_valid(self) -> bool:
        """Check if financial data is valid."""
        return len(self.validate()) == 0


@dataclass
class HealthMetrics:
    """
    HEALTH_METRICS table schema.
    
    Quality indicators (IQSS) from HAS/Ministry of Health.
    """
    metric_id: UUID = field(default_factory=uuid.uuid4)
    vel_id: UUID = None  # Foreign Key
    
    # Global Scores
    score_all_ssr_ajust: Optional[float] = None
    score_ajust_esatis_region: Optional[float] = None
    
    # Process Scores
    score_accueil_ssr_ajust: Optional[float] = None
    score_pec_ssr_ajust: Optional[float] = None
    score_lieu_ssr_ajust: Optional[float] = None
    score_repas_ssr_ajust: Optional[float] = None
    score_sortie_ssr_ajust: Optional[float] = None
    
    # Metadata
    classement: Optional[str] = None
    evolution: Optional[str] = None
    participation: Optional[str] = None
    depot: Optional[str] = None
    annee: int = None
    
    date_created: datetime = field(default_factory=datetime.utcnow)
    source: str = "IQSS"
