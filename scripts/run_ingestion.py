import logging
import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.ingestion_manager import IngestionManager
from src.pipeline import setup_logging

def main():
    parser = argparse.ArgumentParser(description='Run data ingestion for a specific year.')
    parser.add_argument('--year', type=int, default=2023, help='Year to ingest data for (default: 2023)')
    args = parser.parse_args()

    setup_logging("INFO")
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting Ingestion for year {args.year}...")
    manager = IngestionManager()
    
    manager.run_year_ingestion(args.year)

if __name__ == "__main__":
    main()
