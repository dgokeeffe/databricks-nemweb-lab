"""
Pytest configuration for NEMWEB data source tests.

The package is installed by uv (via pyproject.toml py-modules), so modules
like nemweb_utils and nemweb_datasource_stream are importable directly.
No sys.path manipulation is needed.
"""

import sys
from pathlib import Path

# Add the source directory so modules can be imported as flat modules
# (matching the py-modules layout in pyproject.toml)
src_dir = Path(__file__).parent.parent
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# Tell pytest to ignore __init__.py in the source dir - it uses relative
# imports that only work when installed as a package, not when loaded directly
collect_ignore_glob = ["../__init__.py"]
