"""
Script to trigger the data processing pipeline.
Usage:
    python scripts/run_processing.py --year 2024
"""
import logging
import sys
import argparse
from pathlib import Path

# Add project root to path
# This ensures that we can import modules from the 'src' directory
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.processing.data_cleaner import DataProcessor
from src.pipeline import setup_logging

def main():
    """
    Main entry point for data processing.
    Parses command line arguments and triggers the data processor.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run data processing for a specific year.')
    parser.add_argument('--year', type=int, default=2023, help='Year to process data for (default: 2023)')
    args = parser.parse_args()

    # Configure logging
    setup_logging("INFO")
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting Processing for year {args.year}...")
    
    # Robust raw path determination
    raw_path = project_root / "data" / "raw"
    processor = DataProcessor(raw_path)
    
    year = args.year
    logger.info(f"Processing data for year {year}...")
    
    # Run the processing pipeline
    results = processor.process_year(year)
    
    # Display preview of results
    if results:
        df_etab = results.get('etablissements')
        df_qual = results.get('qualifications')
        
        logger.info("Successfully processed data.")
        if df_etab is not None:
             logger.info(f"Etablissements: {df_etab.shape}")
             print("\n--- Etablissements Preview ---")
             print(df_etab.head(3).to_string())
             print("\nColumns:")
             for col in df_etab.columns:
                print(f"  - {col}")

        if df_qual is not None:
             logger.info(f"Qualifications: {df_qual.shape}")
             print("\n--- Qualifications Preview ---")
             print(df_qual.head(3).to_string())
             print("\nColumns:")
             for col in df_qual.columns:
                print(f"  - {col}")

        df_metrics = results.get('health_metrics')
        if df_metrics is not None:
             logger.info(f"Health Metrics: {df_metrics.shape}")
             print("\n--- Health Metrics Preview ---")
             print(df_metrics.head(3).to_string())
             print("\nColumns:")
             for col in df_metrics.columns:
                print(f"  - {col}")
            
    else:
        logger.error("Processing failed.")

if __name__ == "__main__":
    main()
