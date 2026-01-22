# Veltis Data Ingestion Pipeline

> Automated pipeline for ingesting and processing French healthcare data (FINESS, HAS, IQSS)

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: Open License](https://img.shields.io/badge/License-Open%20License-green.svg)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/)

## Overview

This project provides a complete **multi-tier data pipeline** for fetching, cleaning, and processing French healthcare data from official open data sources. It implements a **medallion architecture** (Bronze-Silver) to ensure data quality and traceability, creating a unified database of healthcare establishments with their quality metrics and certifications.

### Key Features

- **Multi-Tier Architecture**: Raw → Bronze → Silver data layers with clear quality gates
- **Automated Ingestion**: Fetch data from data.gouv.fr and HAS
- **Data Cleaning (Bronze)**: Standardize encoding, headers, and formats without transformations
- **Data Processing (Silver)**: Apply business logic, generate UUIDs, and link related data
- **Year-by-Year**: Process historical data maintaining temporal accuracy
- **Extensible**: Modular connector architecture for adding new sources

### Data Sources

| Source | Description | Provider |
|--------|-------------|----------|
| **FINESS** | National registry of healthcare establishments (~500k records) | Ministère de la Santé |
| **HAS Certification** | Quality certifications and decisions | Haute Autorité de Santé |
| **IQSS Metrics** | Annual quality and safety indicators | Ministère de la Santé |

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Step 1: Run ingestion (fetch raw data)
python scripts/run_ingestion.py --year 2024

# Step 2: Run cleaning (Raw → Bronze)
python scripts/run_cleaning.py --year 2024

# Step 3: Run processing (Bronze → Silver)
python scripts/run_processing.py --year 2024

# Explore results with notebooks
jupyter notebook notebooks/
```

**Output**:
- Raw data: `data/raw/2024/`
- Bronze (cleaned): `data/bronze/2024/`
- Silver (processed): `data/silver/2024/`
- Final CSV files: `etablissements.csv`, `qualifications.csv`, `health_metrics.csv`

## Project Structure

```
MVP-web-scrapping-project/
├── scripts/                   # CLI entry points
│   ├── run_ingestion.py      # Step 1: Fetch raw data
│   ├── run_cleaning.py       # Step 2: Raw → Bronze (standardization)
│   └── run_processing.py     # Step 3: Bronze → Silver (transformations)
│
├── src/                       # Core library code
│   ├── pipeline.py           # Main pipeline orchestrator
│   ├── ingestion_manager.py  # Coordinates data ingestion
│   ├── processing/
│   │   ├── data_cleaner.py   # Bronze: Cleaning & standardization
│   │   └── data_processor.py # Silver: Business logic & linking
│   ├── connectors/           # API connectors (data.gouv, HAS)
│   ├── models/               # Data schemas (Etablissement, Qualification, etc.)
│   └── config.py             # Configuration management
│
├── docs/                      # Documentation
│   ├── API_CONNECTORS.md     # Connector usage guide
│   ├── DATA_SOURCES.md       # Data source details
│   ├── DATA_DICTIONARY.md    # Schema and field definitions
│   └── USAGE.md              # Complete user guide
│
├── notebooks/                 # Jupyter exploration notebooks
│   ├── 01_raw_data_exploration.ipynb     # Explore raw data
│   ├── 02_bronze_data_exploration.ipynb  # Explore cleaned data
│   └── 03_silver_data_exploration.ipynb  # Explore processed data
│
├── data/                      # Data storage (not in git)
│   ├── raw/{year}/           # Downloaded files from APIs
│   ├── bronze/{year}/        # Cleaned, standardized data
│   └── silver/{year}/        # Processed, linked, ready-to-use data
│
└── requirements.txt           # Python dependencies
```

## Documentation

📚 **[Complete Usage Guide](docs/USAGE.md)** - Installation, running, troubleshooting

🔌 **[API Connectors](docs/API_CONNECTORS.md)** - How connectors work, extending

📊 **[Data Sources](docs/DATA_SOURCES.md)** - Source details, schemas, integration

📖 **[Data Dictionary](docs/DATA_DICTIONARY.md)** - Business values, field definitions, scores

## Installation

### Requirements
- Python 3.8+
- pip

### Setup

```bash
# 1. Clone repository
git clone https://github.com/your-org/MVP-web-scrapping-project.git
cd MVP-web-scrapping-project

# 2. (Optional) Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

## Usage

### 1. Data Ingestion (Raw Layer)

Fetch raw data from official sources:

```bash
python scripts/run_ingestion.py --year 2024
```

Downloads:
- FINESS establishment registry (CSV, ~160MB)
- HAS certification data (2 CSV files)
- IQSS health quality metrics (Excel)

Output: `data/raw/2024/`

### 2. Data Cleaning (Bronze Layer)

Standardize and clean the raw data:

```bash
python scripts/run_cleaning.py --year 2024
```

Cleans:
- Encoding standardization (UTF-8)
- Header normalization
- Column name cleaning
- Basic validation

Output: `data/bronze/2024/`

### 3. Data Processing (Silver Layer)

Apply business logic and link data:

```bash
python scripts/run_processing.py --year 2024
```

Produces:
- `etablissements.csv` - Normalized establishment data with UUIDs
- `qualifications.csv` - HAS certifications linked to establishments
- `health_metrics.csv` - IQSS metrics linked to establishments

Output: `data/silver/2024/`

### 4. Data Exploration

Explore data at each tier with Jupyter notebooks:

```bash
jupyter notebook notebooks/
```

Available notebooks:
- `01_raw_data_exploration.ipynb` - Explore raw ingested data
- `02_bronze_data_exploration.ipynb` - Explore cleaned data
- `03_silver_data_exploration.ipynb` - Explore final processed data

## Architecture

### Data Flow Overview

```
┌─────────────────┐
│  Data Sources   │
│  (APIs/Files)   │
└────────┬────────┘
         │
         ↓
┌─────────────────────────┐
│   RAW Layer             │  ← run_ingestion.py
│   (data/raw/{year})     │
│   • Original files      │
│   • No transformations  │
└────────┬────────────────┘
         │
         ↓
┌─────────────────────────┐
│   BRONZE Layer          │  ← run_cleaning.py
│   (data/bronze/{year})  │   (data_cleaner.py)
│   • UTF-8 encoding      │
│   • Clean headers       │
│   • Standardized files  │
└────────┬────────────────┘
         │
         ↓
┌─────────────────────────┐
│   SILVER Layer          │  ← run_processing.py
│   (data/silver/{year})  │   (data_processor.py)
│   • UUIDs generated     │
│   • Business logic      │
│   • Linked tables       │
│   • Ready for analysis  │
└─────────────────────────┘
```

### Processing Flow

**Stage 1: Raw → Bronze (Cleaning)**
```
data/raw/{year}/
       ↓
DataCleaner
  ├─ clean_finess()        # Standardize FINESS data
  ├─ clean_has()           # Standardize HAS data
  └─ clean_health_metrics() # Standardize IQSS data
       ↓
data/bronze/{year}/
```

**Stage 2: Bronze → Silver (Processing)**
```
data/bronze/{year}/
       ↓
DataProcessor
  ├─ load_clean_finess()        # Apply Etablissement schema
  ├─ load_clean_has()           # Apply Qualification schema
  └─ load_clean_health_metrics() # Generate UUIDs & link
       ↓
data/silver/{year}/
  ├─ etablissements.csv
  ├─ qualifications.csv
  └─ health_metrics.csv
```

### Connector Architecture

All connectors inherit from `BaseConnector`:

```python
from src.connectors.base import BaseConnector

class MyConnector(BaseConnector):
    def fetch_data(self):
        # Implementation with automatic retry,
        # error handling, and logging
        pass
```

See [API_CONNECTORS.md](docs/API_CONNECTORS.md) for details.

### Data Schema (Silver Layer)

Final processed data follows these schemas:

### Etablissement (Establishment)
- `vel_id` (UUID) - Internal unique ID
- `finess_et` (string) - Official FINESS code
- `siret` (string) - Business number
- `raison_sociale` (string) - Organization name
- `adresse_postale` (string) - Full address
- `code_postal` (string) - Postal code
- `departement` (string) - Department code
- `categorie_etab` (string) - Category (Public/Privé/ESPIC)

### Qualification (Certification)
- `qua_id` (UUID) - Qualification ID
- `vel_id` (UUID) - Link to establishment
- `niveau_certification` (string) - Certification level
- `date_visite` (date) - Certification visit date
- `url_rapport` (string) - Link to official certification report

### Health Metrics
- `metric_id` (UUID) - Metric record ID
- `vel_id` (UUID) - Link to establishment
- Multiple score columns (satisfaction, quality indicators, etc.)
- `annee` (int) - Year of metrics
- `processed_at` (timestamp) - Processing time

## Configuration

Most users can use defaults. To customize:

1. Copy `.env.example` to `.env`
2. Edit values as needed

```bash
cp .env.example .env
# Edit .env with your preferred editor
```

See [USAGE.md](docs/USAGE.md) for configuration details.

## Extending

### Adding a New Data Source

1. Create connector in `src/connectors/my_connector.py`
2. Inherit from `BaseConnector`
3. Implement `fetch_data()` method
4. Add to `IngestionManager`
5. Add cleaning logic in `DataProcessor`

Example:

```python
# src/connectors/my_connector.py
from src.connectors.base import BaseConnector

class MyConnector(BaseConnector):
    def __init__(self, config):
        super().__init__()
        self.api_url = config.api_url
    
    def fetch_data(self):
        response = self._make_request(self.api_url)
        return response.json()
```

See [API_CONNECTORS.md](docs/API_CONNECTORS.md) for full guide.

## Troubleshooting

### Common Issues

**"Dataset not found"**
- The year's data might not be published yet
- Try an earlier year (e.g., 2022 instead of 2024)

**"Connection timeout"**
- Check internet connection
- data.gouv.fr might be temporarily unavailable
- Try again later

**"Parsing error"**
- Check file encoding (FINESS uses latin-1, HAS uses utf-8)
- Verify delimiter (all CSVs use `;` semicolon)
- Check header rows (FINESS has metadata in row 1)

See [USAGE.md - Troubleshooting](docs/USAGE.md#troubleshooting-ingestion) for more.

## Data Quality

The pipeline implements a **multi-tier quality approach**:

### Bronze Layer (Cleaning)
- **Encoding**: Automatic detection and UTF-8 standardization
- **Headers**: Skips metadata rows, normalizes column names
- **Consistency**: Uniform file formats and delimiters
- **No transformations**: Preserves original data semantics

### Silver Layer (Processing)
- **Schema enforcement**: Strict dataclass validation
- **UUID generation**: Reproducible unique identifiers
- **Data linking**: FINESS code normalization and cross-referencing
- **Type safety**: Enforced numeric types for scores/rates
- **Missing values**: Graceful handling with logging

## Performance

Typical runtimes on standard hardware:

- **Ingestion**: 1-2 minutes (downloads ~200MB)
- **Cleaning**: 5-10 seconds (Bronze layer creation)
- **Processing**: 10-20 seconds (Silver layer with UUIDs and linking)
- **Memory**: ~2GB peak for full dataset

## License

This project uses data published under the **License Ouverte / Open License 2.0**:
- ✅ Free use, reuse, and distribution
- ✅ Commercial and non-commercial use
- ℹ️ Source attribution required

License: https://www.etalab.gouv.fr/licence-ouverte-open-licence/

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests (when available)
4. Update documentation
5. Submit a pull request

## Support

- **Documentation**: See `docs/` directory
- **Issues**: Open a GitHub issue
- **Data Questions**: Contact data providers via data.gouv.fr

## Acknowledgments

Data sources:
- **Ministère de la Santé et de la Prévention** - FINESS and IQSS data
- **Haute Autorité de Santé (HAS)** - Certification data
- **data.gouv.fr** - Open data platform

## Roadmap

- [ ] Unit tests for connectors and processing
- [ ] Additional years (2021-2024)
- [ ] Database export (PostgreSQL/SQLite)
- [ ] API for querying processed data
- [ ] Automated scheduling (cron/airflow)
- [ ] Data quality reporting

---

**Questions?** See [docs/USAGE.md](docs/USAGE.md) or open an issue.
