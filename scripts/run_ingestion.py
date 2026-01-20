
import logging
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.ingestion_manager import IngestionManager
from src.pipeline import setup_logging

def main():
    setup_logging("INFO")
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Yearly Ingestion...")
    manager = IngestionManager()
    
    # Run for 2023 as a test (most recent complete year usually)
    manager.run_year_ingestion(2023)
    
    # Or run for full range
    # manager.run_multi_year_ingestion(2021, 2024)

if __name__ == "__main__":
    main()
