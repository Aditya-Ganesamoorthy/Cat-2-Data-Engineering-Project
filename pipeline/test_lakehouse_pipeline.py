"""
Forwarder to tests/test_lakehouse_pipeline.py for backward compatibility.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tests.test_lakehouse_pipeline import run_full_week2_verification

if __name__ == "__main__":
    success = run_full_week2_verification()
    sys.exit(0 if success else 1)
