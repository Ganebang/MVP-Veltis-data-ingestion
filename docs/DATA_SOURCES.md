# Data Sources Documentation

This document describes all external data sources used by the Veltis data ingestion pipeline.

## Overview

The pipeline ingests data from official French healthcare data sources to build a unified database of healthcare establishments and their quality metrics.

| Data Source | Provider | Update Frequency | Format | Use Case |
|------------|----------|------------------|--------|----------|
| FINESS | Data.gouv.fr | Daily | CSV | Establishment registry |
| HAS Certification | Data.gouv.fr (HAS) | Biannual | CSV | Quality certification |
| Health Metrics (IQSS) | Data.gouv.fr | Annual | Excel | Quality indicators |

## 1. FINESS - Establishment Registry

### Description
FINESS (Fichier National des Établissements Sanitaires et Sociaux) is the national registry of all healthcare and social establishments in France.

### Provider
- **Source**: Ministère de la Santé et de la Prévention
- **Platform**: data.gouv.fr
- **Dataset URL**: https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/

### Dataset Details
- **Dataset ID**: `53699569a3a729239d2046eb`
- **Update Frequency**: Daily
- **License**: License Ouverte / Open License
- **Format**: CSV (semicolon-delimited)
- **Encoding**: latin-1
- **Size**: ~160 MB (varies)

### Key Fields
| Field | Description | Example |
|-------|-------------|---------|
| `FINESS ET` | Unique establishment identifier (9 digits) | `010008852` |
| `SIRET` | Business registration number (14 digits) | `12345678901234` |
| `Raison sociale` | Organization name | `CHU de Lyon` |
| `Adresse` | Full address | `5 Place d'Arsonval` |
| `Code postal` | Postal code | `69003` |
| `Catégorie` | Establishment type | `Centre Hospitalier` |

### Data Quality Notes
- **Header row**: Contains metadata (must skip row 1)
- **Missing values**: Some fields may be empty (especially SIRET for public institutions)
- **Column count**: ~30 columns (varies by extract date)
- **Row count**: ~500,000 establishments

### Refresh Strategy
We fetch the latest snapshot for each year. Since it's updated daily, we always get current data.

## 2. HAS Certification Data

### Description
Certification data from the Haute Autorité de Santé (HAS) contains quality assessments and certification decisions for healthcare establishments.

### Provider
- **Source**: Haute Autorité de Santé (HAS)
- **Platform**: data.gouv.fr
- **Dataset URL**: https://www.data.gouv.fr/fr/datasets/decisions-de-certification-des-etablissements-de-sante/

### Dataset Details
- **Dataset ID**: `5336cdbea3a7292eb43e6cba`
- **Update Frequency**: Bianually (certification cycles)
- **License**: License Ouverte / Open License
- **Format**: CSV (semicolon-delimited)
- **Encoding**: utf-8

### Files in Dataset
The HAS dataset contains **two CSV files** that must be merged

:

#### has_demarche.csv
Contains certification decisions.

| Field | Description | Example |
|-------|-------------|---------|
| `code_demarche` | Process identifier (link key) | `DEM-12345` |
| `decision_de_la_cces` | Certification decision | `Certifié` |
| `date_de_decision` | Decision date | `12/06/2023` |

#### has_etab_geo.csv
Contains establishment identifiers and geographic data.

| Field | Description | Example |
|-------|-------------|---------|
| `code_demarche` | Process identifier (link key) | `DEM-12345` |
| `finess_eg` | FINESS EG (geographic entity) | `010000017` |
| `finess_ej` | FINESS EJ (legal entity) | `010000025` |

### Certification Levels
- **Certifié**: Certified
- **Certifié avec recommandations**: Certified with recommendations
- **Haute Qualité**: High quality certification
- **Non certifié**: Not certified

### Linking to FINESS
Use `finess_eg` (preferred) or `finess_ej` from `has_etab_geo.csv` to link with FINESS ET identifiers.

## 3. Health Quality Metrics (IQSS)

### Description
IQSS (Indicateurs de Qualité et de Sécurité des Soins) are standardized quality and safety indicators collected annually from healthcare facilities.

### Provider
- **Source**: Ministère de la Santé
- **Platform**: data.gouv.fr
- **Dataset URL**: https://www.data.gouv.fr/fr/datasets/indicateurs-de-qualite-et-de-securite-des-soins-iqss/

### Dataset Details
- **Update Frequency**: Annual (published early in the year following measurement)
- **License**: License Ouverte / Open License
- **Format**: Excel (.xlsx)
- **Encoding**: UTF-8
- **Size**: ~5-10 MB per year

### Key Indicators

The dataset contains numerous quality metrics across different care areas:

| Indicator Category | Examples |
|-------------------|----------|
| **Patient Satisfaction** | E-satis scores, recommendation rates |
| **Care Quality** | Adherence to guidelines, complication rates |
| **Safety** | Infection rates, medication errors |
| **Continuity** | Follow-up care, discharge quality |

### Key Fields
| Field | Description | Example |
|-------|-------------|---------|
| `FINESS` | Establishment identifier | `010008852` |
| `RS_FINESS` | Organization name | `CHU Lyon` |
| `score_all_ssr_ajust` | Adjusted overall SSR score | `73.2` |
| `taux_reco_brut` | Raw recommendation rate | `58.6` |

### Data Characteristics
- **Row count**: ~3,000-5,000 establishments per year
- **Column count**: ~30-50 metrics (varies by year)
- **Missing values**: Common for establishments not participating in specific measures
- **Data types**: Mix of text (identifiers, names) and numeric (scores, rates)

### Year-Specific Datasets
Each year has its own dedicated dataset with year-specific search patterns:

- **2021**: "Indicateurs de qualité et de sécurité des soins - recueil 2021"
- **2022**: "Indicateurs de qualité et de sécurité des soins - recueil 2022"
- **2023**: "Indicateurs de qualité et de sécurité des soins - recueil 2023"
- **2024**: "Indicateurs de qualité et de sécurité des soins - recueil 2024"

## Data Integration Strategy

### 1. FINESS as Master Reference
- FINESS serves as the master establishment registry
- Each establishment gets a UUID (`vel_id`) upon ingestion

### 2. Linking HAS Data
- Merge `has_demarche.csv` and `has_etab_geo.csv` on `code_demarche`
- Link to FINESS using `finess_eg` → `FINESS ET`
- Multiple certifications per establishment are possible (different dates)

### 3. Linking Health Metrics
- Direct match on `FINESS` field from Excel
- Normalize FINESS identifiers (remove decimals, zero-pad to 9 digits)
- One row per establishment per year

## Data Quality Considerations

### Common Issues

1. **Identifier Mismatches**:
   - FINESS codes may be stored as numbers (lose leading zeros)
   - **Solution**: Always zero-pad to 9 digits

2. **Encoding Differences**:
   - FINESS uses latin-1, others use utf-8
   - **Solution**: Specify encoding explicitly when loading

3. **Missing Data**:
   - Not all establishments participate in all programs
   - **Solution**: Use outer joins and handle nulls gracefully

4. **Historical Changes**:
   - Establishments may close, merge, or change IDs
   - **Solution**: Process year-by-year to maintain historical accuracy

## License Information

All data sources are published under the **License Ouverte / Open License 2.0**, which allows:
- ✅ Free use, reuse, and distribution
- ✅ Commercial and non-commercial use
- ✅ Modification and derivative works

**Requirements**:
- ℹ️ Mention the source
- ℹ️ Include link to license

License URL: https://www.etalab.gouv.fr/licence-ouverte-open-licence/

## Update Schedule

| Source | Expected Update | How We Fetch |
|--------|----------------|--------------|
| FINESS | Daily | Latest snapshot via API |
| HAS Certification | January & July | Latest snapshot via API |
| IQSS | March (for previous year) | Year-specific dataset search |

## Contact & Support

For questions about data sources:

- **FINESS**: Contact via data.gouv.fr dataset comments
- **HAS**: https://www.has-sante.fr/jcms/fc_2814988/fr/nous-contacter
- **IQSS**: Via Ministère de la Santé or data.gouv.fr

## Related Resources

- [data.gouv.fr Search](https://www.data.gouv.fr/fr/datasets/)
- [HAS Certification Information](https://www.has-sante.fr/jcms/c_2824177/fr/certification-des-etablissements-de-sante)
- [FINESS User Guide (PDF)](https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/)
- [Open License Text](https://www.etalab.gouv.fr/wp-content/uploads/2017/04/ETALAB-Licence-Ouverte-v2.0.pdf)
