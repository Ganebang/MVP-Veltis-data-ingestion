# API Connectors Documentation

This document describes the API connectors used to fetch data from external sources.

## Overview

The project uses a modular connector architecture where each data source has its own connector class inheriting from `BaseConnector`. This provides:

- **Standardized interface**: All connectors implement common methods
- **Error handling**: Built-in retry logic and error management
- **Caching**: Optional in-memory caching to reduce API calls
- **Logging**: Comprehensive logging for debugging

## BaseConnector

Located in: `src/connectors/base.py`

The base class provides common functionality for all connectors:

```python
from src.connectors.base import BaseConnector

class MyConnector(BaseConnector):
    def fetch_data(self):
        # Implementation
        pass
```

### Key Features

- **HTTP utilities**: `_make_request()` with automatic retries
- **Error handling**: Standardized exception handling
- **Logging**: Pre-configured logger for each connector

## DataGouvConnector

Located in: `src/connectors/datagouv_api.py`

Connects to the French open data portal (data.gouv.fr) to fetch:
- FINESS establishment registry
- Health quality metrics (IQSS)

### Usage Example

```python
from src.connectors.datagouv_api import DataGouvConnector
from src.config import config

# Initialize connector
connector = DataGouvConnector(config.data_gouv)

# Search for datasets
results = connector.search_datasets("FINESS", page_size=5)

# Get specific dataset metadata
metadata = connector.get_dataset_metadata("dataset-id-here")

# Fetch FINESS data
finess_data = connector.fetch_finess_data()
```

### Methods

#### `search_datasets(query, page_size=20)`
Search for datasets by keyword.

**Parameters**:
- `query` (str): Search query
- `page_size` (int): Number of results to return

**Returns**: List of dataset dictionaries with `id`, `title`, `description`

#### `get_dataset_metadata(dataset_id)`
Fetch metadata for a specific dataset.

**Parameters**:
- `dataset_id` (str): Unique dataset identifier

**Returns**: Dictionary containing dataset metadata and resources

#### `fetch_finess_data()`
Download and parse the FINESS CSV file.

**Returns**: pandas DataFrame with establishment data

### Rate Limits

- No authentication required
- No strict rate limits documented
- Uses a 3-second delay between requests by default (can be configured)

### Data Format

FINESS CSV structure:
- **Delimiter**: `;` (semicolon)
- **Encoding**: `latin-1`
- **Header**: Row 1 contains metadata, Row 2 contains column names
- **Key columns**: 
  - Column 1: FINESS ET (establishment ID)
  - Column 4: Raison sociale (organization name)
  - Column 22: SIRET
  - Column 27: Category

## HASConnector

Located in: `src/connectors/has_connector.py`

Connects to the Haute Autorité de Santé (HAS) for:
- Certification status (`has_demarche.csv`)
- Geographic data (`has_etab_geo.csv`)

### Usage Example

```python
from src.connectors.has_connector import HASConnector
from src.config import config

# Initialize connector
connector = HASConnector(config.data_gouv)  # Uses data.gouv to fetch HAS data

# Fetch certification data
cert_data = connector.fetch_certification_data()
```

### Methods

#### `fetch_certification_data()`
Download HAS certification data.

**Returns**: Dictionary containing:
- `demarche`: Certification process data
- `geo`: Geographic/establishment data

### Data Format

HAS data comes as two CSV files:
- **has_demarche.csv**: Certification decisions
  - Key columns: `code_demarche`, `decision_de_la_cces`, `date_de_decision`
- **has_etab_geo.csv**: Establishment geographic data
  - Key columns: `code_demarche`, `finess_eg`, `finess_ej`

Both files use:
- **Delimiter**: `;` (semicolon)
- **Encoding**: `utf-8`
- **Link key**: `code_demarche` (used to merge the two files)

## Configuration

Connectors are configured via `src/config.py`:

```python
from src.config import config

# Access connector settings
data_gouv_config = config.data_gouv
print(data_gouv_config.base_url)
print(data_gouv_config.finess_dataset_id)
```

### Environment Variables

You can override configuration using environment variables (see `.env.example`):

```bash
DATA_GOUV_BASE_URL=https://www.data.gouv.fr/api/1
DATA_GOUV_FINESS_DATASET_ID=your-id-here
```

## Error Handling

All connectors handle common errors:

- **Network errors**: Automatic retry with exponential backoff (3 attempts)
- **HTTP errors**: Logged with status code and response details
- **Data parsing errors**: Caught and logged with details
- **Missing data**: Returns `None` with warning logged

Example error handling:

```python
try:
    data = connector.fetch_finess_data()
    if data is None:
        logger.error("Failed to fetch FINESS data")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
```

## Extending Connectors

To add a new data source:

1. Create a new file in `src/connectors/`
2. Inherit from `BaseConnector`
3. Implement required methods
4. Add configuration to `src/config.py`

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

## Best Practices

1. **Always log operations**: Use `self.logger` in connector methods
2. **Handle None returns**: Check if data fetch returned None
3. **Use configuration**: Don't hardcode URLs or IDs
4. **Respect rate limits**: Add delays between bulk requests
5. **Cache when appropriate**: Use caching for metadata that doesn't change

## Troubleshooting

### "Dataset not found" error
- Verify the dataset ID in configuration
- Check if the dataset is still published on data.gouv.fr

### "Connection timeout" error
- Check internet connection
- Verify API endpoints are accessible
- Increase timeout in connector configuration

### "Parsing error" when reading CSV
- Check the delimiter (`;` vs `,`)
- Verify encoding (`latin-1` vs `utf-8`)
- Inspect raw file for header rows

## Additional Resources

- [data.gouv.fr API Documentation](https://doc.data.gouv.fr/api/)
- [HAS Open Data](https://www.has-sante.fr/jcms/c_2824177/fr/certification-des-etablissements-de-sante-questions-reponses)
- [FINESS Database Information](https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/)
