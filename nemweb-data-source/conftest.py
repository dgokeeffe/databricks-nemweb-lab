"""
Root conftest for pytest - tells pytest to ignore __init__.py which uses
relative imports that only work when installed as a package.
"""

collect_ignore = ["__init__.py"]
