"""
Basic tests for the Veltis data pipeline.

Run with: python -m pytest tests/
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_imports():
    """Test that core modules can be imported."""
    from src.ingestion_manager import IngestionManager
    from src.processing.data_cleaner import DataProcessor
    from src.connectors.datagouv_api import DataGouvConnector
    from src.connectors.has_connector import HASConnector
    from src.config import config
    
    assert IngestionManager is not None
    assert DataProcessor is not None
    assert DataGouvConnector is not None
    assert HASConnector is not None
    assert config is not None


def test_config():
    """Test configuration is loaded correctly."""
    from src.config import config
    
    assert config.data_gouv is not None
    assert config.data_gouv.base_url is not None
    assert config.data_gouv.finess_dataset_id is not None


def test_ingestion_manager_init():
    """Test IngestionManager can be initialized."""
    from src.ingestion_manager import IngestionManager
    
    manager = IngestionManager()
    assert manager is not None
    assert hasattr(manager, 'run_year_ingestion')


def test_data_processor_init():
    """Test DataProcessor can be initialized."""
    from src.processing.data_cleaner import DataProcessor
    
    processor = DataProcessor("/tmp/test")
    assert processor is not None
    assert hasattr(processor, 'process_year')


if __name__ == "__main__":
    # Run tests manually without pytest
    print("Running basic tests...")
    
    try:
        test_imports()
        print("✓ test_imports passed")
    except Exception as e:
        print(f"✗ test_imports failed: {e}")
    
    try:
        test_config()
        print("✓ test_config passed")
    except Exception as e:
        print(f"✗ test_config failed: {e}")
    
    try:
        test_ingestion_manager_init()
        print("✓ test_ingestion_manager_init passed")
    except Exception as e:
        print(f"✗ test_ingestion_manager_init failed: {e}")
    
    try:
        test_data_processor_init()
        print("✓ test_data_processor_init passed")
    except Exception as e:
        print(f"✗ test_data_processor_init failed: {e}")
    
    print("\nAll tests completed!")
