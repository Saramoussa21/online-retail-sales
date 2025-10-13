import sys
from pathlib import Path

# Ensure the 'app' package directory is on sys.path so imports like
# 'retail_data_platform...' resolve during tests when pytest is run
# from the repository root.
ROOT_APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_APP))
