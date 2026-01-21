"""
Script to run data cleaning pipeline (Raw → Bronze).

Usage:
    python scripts/run_cleaning.py --year 2024
"""
import logging
import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.processing.data_cleaner import DataCleaner
from src.pipeline import setup_logging


def main():
    """
    Main entry point for data cleaning.
    Parses command line arguments and triggers the data cleaner.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run data cleaning for a specific year.')
    parser.add_argument('--year', type=int, default=2024, help='Year to clean data for (default: 2024)')
    args = parser.parse_args()

    # Configure logging
    setup_logging("INFO")
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting Data Cleaning for year {args.year}...")
    
    # Setup paths
    raw_path = project_root / "data" / "raw"
    bronze_path = project_root / "data" / "bronze"
    
    # Create cleaner and run
    cleaner = DataCleaner(raw_path, bronze_path)
    
    year = args.year
    logger.info(f"Cleaning data for year {year}...")
    
    # Run the cleaning pipeline
    results = cleaner.clean_year(year)
    
    # Display summary
    if results:
        logger.info("\n" + "=" * 60)
        logger.info("CLEANING SUMMARY")
        logger.info("=" * 60)
        
        for source_name, df in results.items():
            if df is not None:
                logger.info(f"✓ {source_name}: {len(df)} records")
                logger.info(f"  Columns: {len(df.columns)}")
            else:
                logger.info(f"✗ {source_name}: FAILED")
        
        logger.info("=" * 60)
        logger.info(f"Bronze data saved to: {bronze_path / str(year)}")
        logger.info("=" * 60)
    else:
        logger.error("Cleaning failed.")


if __name__ == "__main__":
    main()
