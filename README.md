# Veltis Data Ingestion Pipeline

> Automated pipeline for ingesting and processing French healthcare data (FINESS, HAS, IQSS)

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: Open License](https://img.shields.io/badge/License-Open%20License-green.svg)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/)

## Overview

This project provides a complete data pipeline for fetching, cleaning, and normalizing French healthcare data from official open data sources. It creates a unified database of healthcare establishments with their quality metrics and certifications.

### Key Features

- **Automated Ingestion**: Fetch data from data.gouv.fr and HAS
- **Data Cleaning**: Handle messy headers, encoding issues, and missing values
- **Normalization**: Generate consistent schemas with UUIDs and linking
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

# Run ingestion (defaults to 2023, or specify year)
python scripts/run_ingestion.py --year 2024

# Process data
python scripts/run_processing.py --year 2024

# Explore results
jupyter notebook notebooks/exploration_veltis_data.ipynb
```

**Output**:
- Raw data: `data/raw/2023/`
- Processed data: `data/processed/2023/`
- 3 normalized CSV files: `etablissements.csv`, `qualifications.csv`, `health_metrics.csv`

## Project Structure

```
MVP-web-scrapping-project/
├── scripts/              # CLI entry points
│   ├── run_ingestion.py   # Fetch raw data
│   └── run_processing.py  # Clean and normalize
│
├── src/                  # Core library code
│   ├── ingestion_manager.py    # Orchestrates ingestion
│   ├── processing/
│   │   └── data_cleaner.py     # Cleans and normalizes data
│   ├── connectors/             # API connectors (data.gouv, HAS)
│   ├── models/                 # Data schemas
│   └── config.py               # Configuration
│
├── docs/                 # Documentation
│   ├── API_CONNECTORS.md  # Connector usage guide
│   ├── DATA_SOURCES.md    # Data source details
│   └── USAGE.md           # Complete user guide
│
├── notebooks/            # Jupyter notebooks
├── data/                 # Data storage (not in git)
│   ├── raw/              # Downloaded files
│   └── processed/        # Cleaned and normalized files
│
└── requirements.txt      # Python dependencies
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

### 1. Data Ingestion

Fetch raw data from official sources:

```bash
python scripts/run_ingestion.py
```

Downloads:
- FINESS establishment registry (CSV, ~160MB)
- HAS certification data (2 CSV files)
- IQSS health quality metrics (Excel)

Output: `data/raw/2023/`

### 2. Data Processing

Clean and normalize the raw data:

```bash
python scripts/run_processing.py
```

Produces:
- `etablissements.csv` - Normalized establishment data with UUIDs
- `qualifications.csv` - HAS certifications linked to establishments
- `health_metrics.csv` - IQSS metrics linked to establishments

Output: `data/processed/2023/`

### 3. Data Exploration

Open the Jupyter notebook to explore the data:

```bash
jupyter notebook notebooks/exploration_veltis_data.ipynb
```

The notebook provides:
- Automatic dependency installation
- Data loading and preview
- Basic statistics and quality checks

## Architecture

### Ingestion Flow

```
data.gouv.fr APIs
       ↓
DataGouvConnector / HASConnector
       ↓
IngestionManager
       ↓
data/raw/{year}/
```

### Processing Flow

```
data/raw/{year}/
       ↓
DataProcessor
  ├─ load_clean_finess()
  ├─ load_clean_has()
  └─ load_clean_health_metrics()
       ↓
data/processed/{year}/
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

## Data Schema

### Etablissement (Establishment)
- `vel_id` (UUID) - Internal unique ID
- `finess_et` (string) - Official FINESS code
- `siret` (string) - Business number
- `raison_sociale` (string) - Organization name
- `adresse_postale` (string) - Full address
- `code_postal` (string) - Postal code
- `categorie_etab` (string) - Category (Public/Privé/ESPIC)

### Qualification (Certification)
- `qua_id` (UUID) - Qualification ID
- `vel_id` (UUID) - Link to establishment
- `niveau_certification` (string) - Certification level
- `date_visite` (date) - Certification visit date
- `score_satisfaction` (float) - Satisfaction score (if available)

### Health Metrics
- `vel_id` (UUID) - Link to establishment
- Multiple score and rate columns (varies by year)
- `metric_id` (UUID) - Metric record ID
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

The pipeline handles common data quality issues:

- **Encoding**: Automatic detection and conversion
- **Headers**: Skips metadata rows, normalizes column names
- **Missing values**: Graceful handling with logging
- **Type conversion**: Enforces numeric types for scores/rates
- **Linking**: Normalizes FINESS codes (zero-padding, decimal removal)

## Performance

Typical runtimes on standard hardware:

- **Ingestion**: 1-2 minutes
- **Processing**: 10-30 seconds
- **Memory**: ~2GB for full dataset

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
