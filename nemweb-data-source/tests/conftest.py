"""
Pytest configuration for NEMWEB data source tests.

Sets up the Python path to allow importing from the src directory.
"""

import sys
from pathlib import Path

# Add the src directory to the path so we can import modules
src_dir = Path(__file__).parent.parent
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))
