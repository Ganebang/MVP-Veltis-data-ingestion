# Usage Guide

This guide provides step-by-step instructions for using the Veltis Data Ingestion Pipeline.

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/your-org/MVP-web-scrapping-project.git
cd MVP-web-scrapping-project

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run ingestion for 2023
python scripts/run_ingestion.py

# 4. Process the data
python scripts/run_processing.py

# 5. Explore the results
jupyter notebook notebooks/exploration_veltis_data.ipynb
```

## Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)
- (Optional) Jupyter Notebook for data exploration

### Step 1: Clone the Repository

```bash
git clone https://github.com/your-org/MVP-web-scrapping-project.git
cd MVP-web-scrapping-project
```

### Step 2: Create Virtual Environment (Recommended)

```bash
# Create virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

Required packages:
- `pandas` - Data manipulation
- `requests` - HTTP requests for API calls
- `openpyxl` - Excel file reading
- `python-dotenv` - Environment variable management

### Step 4: Configure (Optional)

Copy `.env.example` to `.env` and customize if needed:

```bash
cp .env.example .env
```

Most users can use the defaults. Advanced configuration options are documented in [`DATA_SOURCES.md`](DATA_SOURCES.md).

## Running Data Ingestion

### Single Year Ingestion

Fetch raw data for a specific year (e.g., 2023):

```bash
python scripts/run_ingestion.py
```

This will:
1. Search for the IQSS (Health Metrics) dataset for 2023
2. Download the Excel file to `data/raw/2023/health_metrics.xlsx`
3. Fetch the latest FINESS snapshot to `data/raw/2023/finess.csv`
4. Download HAS certification data to `data/raw/2023/has_*.csv`

**Output Location**: `data/raw/2023/`

### Expected Runtime
- Small year (2021): ~30 seconds
- Large year (2023): ~1-2 minutes

### Troubleshooting Ingestion

**Problem**: "Dataset not found for year XXXX"  
**Solution**: The IQSS dataset might not be published yet. Try an earlier year.

**Problem**: "Connection timeout"  
**Solution**: Check your internet connection. The API might be temporarily unavailable.

**Problem**: "Download failed"  
**Solution**: Check the logs for the specific error. The file might have been moved or renamed on data.gouv.fr.

## Running Data Processing

After ingestion, clean and normalize the raw data:

```bash
python scripts/run_processing.py
```

This will:
1. Load raw data from `data/raw/2023/`
2. Clean FINESS data:
   - Skip metadata header row
   - Extract key columns (FINESS ET, SIRET, name, address, postal code, category)
   - Generate UUIDs for each establishment
3. Clean and merge HAS data:
   - Merge `has_demarche.csv` and `has_etab_geo.csv`
   - Link to FINESS establishments
   - Extract certification level and date
4. Clean Health Metrics:
   - Normalize column headers (remove accents, spaces)
   - Enforce numeric types for scores/rates
   - Link to FINESS establishments
5. Save processed data to `data/processed/2023/`:
   - `etablissements.csv` - Normalized establishment data
   - `qualifications.csv` - Quality certifications linked to establishments
   - `health_metrics.csv` - Quality metrics linked to establishments

**Output Location**: `data/processed/2023/`

### Expected Runtime
- ~10-30 seconds depending on data volume

### Output Files

#### etablissements.csv
Normalized establishment data with schema:

| Column | Type | Description |
|--------|------|-------------|
| `vel_id` | UUID | Internal unique identifier |
| `finess_et` | String | official FINESS ET code (9 digits) |
| `siret` | String | SIRET business number |
| `raison_sociale` | String | Organization name |
| `adresse_postale` | String | Full address |
| `code_postal` | String | Postal code |
| `categorie_etab` | String | Establishment category |
| `date_created` | Timestamp | Record creation timestamp |
| `date_updated` | Timestamp | Last update timestamp |
| `source` | String | Data source (always "Data.gouv") |

#### qualifications.csv
Quality certifications linked to establishments:

| Column | Type | Description |
|--------|------|-------------|
| `qua_id` | UUID | Qualification record ID |
| `vel_id` | UUID | Link to establishment |
| `niveau_certification` | String | Certification level |
| `date_visite` | Date | Certification visit date |
| `url_rapport` | String | URL to certification report |
| `score_satisfaction` | Float | Satisfaction score (if available) |
| `source` | String | Data source |
| `freshness` | String | Update frequency |
| `date_created` | Timestamp | Record creation |
| `date_updated` | Timestamp | Last update |

#### health_metrics.csv
Many columns (varies by year). Key columns:

| Column | Type | Description |
|--------|------|-------------|
| `vel_id` | UUID | Link to establishment |
| `finess_raw` | String | Original FINESS from source |
| `rs_finess` | String | Organization name |
| `score_*` | Float | Various quality scores |
| `taux_*` | Float | Various rates |
| `metric_id` | UUID | Metric record ID |
| `processed_at` | Timestamp | Processing timestamp |

## Exploring the Data

### Using Jupyter Notebook

Start Jupyter and open the exploration notebook:

```bash
jupyter notebook notebooks/exploration_veltis_data.ipynb
```

The notebook will:
1. Automatically install dependencies (pandas, openpyxl)
2. Load raw data from `data/raw/2023/`
3. Display preview and statistics for each dataset

### Using Python

```python
import pandas as pd

# Load processed data
etab = pd.read_csv('data/processed/2023/etablissements.csv')
qual = pd.read_csv('data/processed/2023/qualifications.csv')
metrics = pd.read_csv('data/processed/2023/health_metrics.csv')

# Basic exploration
print(f"Total establishments: {len(etab)}")
print(f"Total certifications: {len(qual)}")
print(f"Establishments with metrics: {len(metrics)}")

# Example: Find establishments with high satisfaction
high_quality = qual[qual['niveau_certification'] == 'Haute Qualité']
print(f"High quality certifications: {len(high_quality)}")
```

## Data Processing vs. Data Ingestion

| Aspect | Ingestion | Processing |
|--------|-----------|------------|
| **Purpose** | Download raw data | Clean and normalize |
| **Input** | External APIs | `data/raw/`| **Output** | `data/raw/` | `data/processed/` |
| **When to run** | When you need fresh data | After ingestion |
| **Idempotent** | No (re-downloads) | Yes (same input = same output) |

## Advanced Usage

### Multi-Year Ingestion

To ingest data for multiple years, edit `scripts/run_ingestion.py`:

```python
# In run_ingestion.py, replace:
manager.run_year_ingestion(2023)

# With:
manager.run_multi_year_ingestion(2021, 2024)
```

This will fetch data for 2021, 2022, 2023, and 2024.

### Custom Year Processing

To process a different year, edit `scripts/run_processing.py`:

```python
# Change the year variable
year = 2022  # Instead of 2023

# Then run
processor = DataProcessor(raw_path)
results = processor.process_year(year)
```

### Programmatic Usage

Import and use the core classes directly:

```python
from src.ingestion_manager import IngestionManager
from src.processing.data_cleaner import DataProcessor

# Ingestion
manager = IngestionManager()
manager.run_year_ingestion(2023)

# Processing
processor = DataProcessor("/path/to/data/raw")
results = processor.process_year(2023)

# Access results
establishments = results['etablissements']
qualifications = results['qualifications']
metrics = results['health_metrics']
```

## Directory Structure

After running ingestion and processing:

```
MVP-web-scrapping-project/
├── data/
│   ├── raw/
│   │   └── 2023/
│   │       ├── finess.csv
│   │       ├── has_demarche.csv
│   │       ├── has_etab_geo.csv
│   │       └── health_metrics.xlsx
│   └── processed/
│       └── 2023/
│           ├── etablissements.csv
│           ├── qualifications.csv
│           └── health_metrics.csv
├── scripts/
│   ├── run_ingestion.py
│   └── run_processing.py
└── notebooks/
    └── exploration_veltis_data.ipynb
```

## Logging

All operations are logged to the console. To save logs to a file:

```python
# In your script, modify the logging setup
from src.pipeline import setup_logging

setup_logging("INFO", log_file="pipeline.log")
```

Log levels (from most to least verbose):
- `DEBUG`: Detailed diagnostic information
- `INFO`: Confirmation messages (default)
- `WARNING`: Something unexpected but not critical
- `ERROR`: A serious problem occurred

## Performance Tips

1. **Ingestion**: Run during off-peak hours to avoid API slowdowns
2. **Processing**: Use SSD storage for faster CSV parsing
3. **Memory**: Processing 500k+ establishments requires ~2GB RAM
4. **Parallel**: For multi-year, consider running years in parallel (advanced)

## Getting Help

- **Documentation**: See [`API_CONNECTORS.md`](API_CONNECTORS.md) and [`DATA_SOURCES.md`](DATA_SOURCES.md)
- **Logs**: Check console output for detailed error messages
- **Data Quality**: Inspect raw files manually if processing fails

## Next Steps

- Explore the processed data in Jupyter
- Integrate with your application/database
- Set up scheduled ingestion (e.g., weekly cron job)
- Extend with additional data sources

## Common Workflows

### Weekly Data Refresh
```bash
# Fetch latest data (Friday morning)
python scripts/run_ingestion.py

# Process immediately
python scripts/run_processing.py

# Update your database/application
# (custom integration code)
```

### Historical Analysis
```bash
# Modify scripts to loop through years 2021-2024
# Process each year
# Compare year-over-year trends in notebook
```

### Quality Assurance
```bash
# Run ingestion
python scripts/run_ingestion.py

# Check raw file row counts
wc -l data/raw/2023/*.csv

# Run processing
python scripts/run_processing.py

# Check processed file row counts
wc -l data/processed/2023/*.csv

# Investigate discrepancies if any
```
