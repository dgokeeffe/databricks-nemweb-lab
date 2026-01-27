#!/usr/bin/env python3
"""
Convert Databricks Python notebook files (.py) to Jupyter Notebook format (.ipynb).

This script parses Databricks notebook format:
- # Databricks notebook source header
- # MAGIC %md for markdown cells
- # MAGIC %sql for SQL cells
- # COMMAND ---------- as cell separators

And converts to Jupyter Notebook format with proper cell types and metadata.
"""

import sys
import json
from pathlib import Path
from typing import List, Dict, Any


def parse_databricks_notebook(py_content: str) -> List[Dict[str, Any]]:
    """Parse Databricks .py notebook format into Jupyter notebook cells."""
    lines = py_content.split('\n')
    cells = []
    current_cell = []
    current_cell_type = 'code'  # 'code' or 'markdown'
    
    for line in lines:
        # Check for magic commands (markdown, SQL, etc.)
        if line.strip().startswith('# MAGIC'):
            # Extract the magic command
            magic_content = line.replace('# MAGIC', '').strip()
            
            # Determine cell type from magic command
            if magic_content.startswith('%md'):
                # Markdown cell starts
                if current_cell and current_cell_type == 'code':
                    # Save previous code cell
                    cells.append({
                        'cell_type': 'code',
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [],
                        'execution_count': None
                    })
                    current_cell = []
                
                current_cell_type = 'markdown'
                # Remove %md prefix and add content (if there's content on same line)
                md_content = magic_content.replace('%md', '').strip()
                if md_content:
                    current_cell.append(md_content)
                # Continue - next lines with # MAGIC prefix will be added to markdown cell
                continue
            elif magic_content.startswith('%sql'):
                # SQL cell - save current cell first
                if current_cell:
                    if current_cell_type == 'markdown':
                        cells.append({
                            'cell_type': 'markdown',
                            'source': '\n'.join(current_cell),
                            'metadata': {}
                        })
                    else:
                        cells.append({
                            'cell_type': 'code',
                            'source': '\n'.join(current_cell),
                            'metadata': {},
                            'outputs': [],
                            'execution_count': None
                        })
                    current_cell = []
                
                current_cell_type = 'code'
                current_cell.append(magic_content)
            elif magic_content.startswith('%python') or magic_content.startswith('%scala') or magic_content.startswith('%r'):
                # Language-specific cells - save current cell first
                if current_cell:
                    if current_cell_type == 'markdown':
                        cells.append({
                            'cell_type': 'markdown',
                            'source': '\n'.join(current_cell),
                            'metadata': {}
                        })
                    else:
                        cells.append({
                            'cell_type': 'code',
                            'source': '\n'.join(current_cell),
                            'metadata': {},
                            'outputs': [],
                            'execution_count': None
                        })
                    current_cell = []
                
                current_cell_type = 'code'
                current_cell.append(magic_content)
            else:
                # This is either:
                # 1. A markdown content line (if we're in markdown mode)
                # 2. Another magic command (if we're in code mode)
                if current_cell_type == 'markdown':
                    # Markdown content line - strip # MAGIC prefix and add to markdown cell
                    md_line = magic_content.rstrip()
                    if md_line:  # Only add non-empty lines
                        current_cell.append(md_line)
                else:
                    # Other magic commands - treat as code
                    if current_cell:
                        cells.append({
                            'cell_type': 'code',
                            'source': '\n'.join(current_cell),
                            'metadata': {},
                            'outputs': [],
                            'execution_count': None
                        })
                        current_cell = []
                    current_cell_type = 'code'
                    current_cell.append(magic_content)
        elif line.strip().startswith('# Databricks notebook source'):
            # New notebook marker - save current cell if any
            if current_cell:
                if current_cell_type == 'markdown':
                    cells.append({
                        'cell_type': 'markdown',
                        'source': '\n'.join(current_cell),
                        'metadata': {}
                    })
                else:
                    cells.append({
                        'cell_type': 'code',
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [],
                        'execution_count': None
                    })
                current_cell = []
            current_cell_type = 'code'
            # Skip the header line
            continue
        elif line.strip().startswith('# COMMAND ----------'):
            # Cell separator in Databricks format
            if current_cell:
                if current_cell_type == 'markdown':
                    cells.append({
                        'cell_type': 'markdown',
                        'source': '\n'.join(current_cell),
                        'metadata': {}
                    })
                else:
                    cells.append({
                        'cell_type': 'code',
                        'source': '\n'.join(current_cell),
                        'metadata': {},
                        'outputs': [],
                        'execution_count': None
                    })
                current_cell = []
            current_cell_type = 'code'
        else:
            # Regular content line (not # MAGIC, not # COMMAND, not header)
            if current_cell_type == 'markdown':
                # Continue markdown cell
                # Note: Regular lines in markdown cells shouldn't have # MAGIC prefix
                # But if they do (edge case), we'd handle it above
                current_cell.append(line.rstrip())
            else:
                # Code cell
                current_cell.append(line)
    
    # Add final cell
    if current_cell:
        if current_cell_type == 'markdown':
            cells.append({
                'cell_type': 'markdown',
                'source': '\n'.join(current_cell),
                'metadata': {}
            })
        else:
            cells.append({
                'cell_type': 'code',
                'source': '\n'.join(current_cell),
                'metadata': {},
                'outputs': [],
                'execution_count': None
            })
    
    return cells


def convert_py_to_ipynb(py_file: Path, output_dir: Path) -> Path:
    """Convert a single .py file to .ipynb format."""
    print(f"Converting {py_file.name}...")
    
    # Read the Python file
    with open(py_file, 'r', encoding='utf-8') as f:
        py_content = f.read()
    
    # Check if it's a Databricks notebook
    if not py_content.startswith('# Databricks notebook source'):
        print(f"  ⚠ Skipping {py_file.name} - not a Databricks notebook")
        return None
    
    # Parse into cells
    cells = parse_databricks_notebook(py_content)
    
    if not cells:
        print(f"  ⚠ No cells found in {py_file.name}")
        return None
    
    # Get wheel path for dependencies
    def get_wheel_path():
        """Get the wheel path that will be deployed by the bundle."""
        src_dir = Path(__file__).parent.parent / "src"
        version = "2.10.4"  # Default fallback
        
        try:
            import re
            with open(src_dir / "pyproject.toml", 'r') as f:
                content = f.read()
                match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
                if match:
                    version = match.group(1)
        except Exception:
            pass
        
        wheel_name = f"nemweb_datasource-{version}-py3-none-any.whl"
        return f"/Workspace/Users/david.okeeffe@databricks.com/.bundle/nemweb-lab/dev/files/databricks-nemweb-lab/artifacts/{wheel_name}"
    
    wheel_path = get_wheel_path()
    
    # Create notebook structure with Databricks metadata (matching example structure)
    notebook = {
        'cells': cells,
        'metadata': {
            'application/vnd.databricks.v1+notebook': {
                'computePreferences': {
                    'hardware': {
                        'accelerator': None,
                        'gpuPoolId': None,
                        'memory': None
                    }
                },
                'dashboards': [],
                'environmentMetadata': {
                    'base_environment': '',
                    'dependencies': [wheel_path],
                    'environment_version': '4'
                },
                'inputWidgetPreferences': None,
                'language': 'python',
                'notebookMetadata': {
                    'pythonIndentUnit': 4
                },
                'notebookName': py_file.stem,
                'widgets': {}
            },
            'language_info': {
                'name': 'python'
            },
            'orig_nbformat': 4
        },
        'nbformat': 4,
        'nbformat_minor': 4
    }
    
    # Write .ipynb file
    ipynb_file = output_dir / f"{py_file.stem}.ipynb"
    with open(ipynb_file, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=1, ensure_ascii=False)
    
    print(f"  ✓ Created {ipynb_file.name}")
    return ipynb_file


def main():
    """Convert all .py notebook files to .ipynb format."""
    if len(sys.argv) > 1:
        notebooks_dir = Path(sys.argv[1])
    else:
        notebooks_dir = Path(__file__).parent.parent / "notebooks"
    
    if len(sys.argv) > 2:
        output_dir = Path(sys.argv[2])
    else:
        output_dir = notebooks_dir
    
    if not notebooks_dir.exists():
        print(f"Error: Directory {notebooks_dir} does not exist")
        sys.exit(1)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all .py files
    py_files = list(notebooks_dir.glob("*.py"))
    
    if not py_files:
        print(f"No .py files found in {notebooks_dir}")
        return
    
    print(f"Found {len(py_files)} Python files in {notebooks_dir}")
    print(f"Output directory: {output_dir}\n")
    
    converted = []
    skipped = []
    
    for py_file in sorted(py_files):
        result = convert_py_to_ipynb(py_file, output_dir)
        if result:
            converted.append(result)
        else:
            skipped.append(py_file)
    
    print(f"\n{'='*60}")
    print(f"Conversion complete!")
    print(f"  Converted: {len(converted)} files")
    if skipped:
        print(f"  Skipped: {len(skipped)} files")
    print(f"{'='*60}")
    
    if converted:
        print("\nConverted files:")
        for f in converted:
            print(f"  - {f.name}")


if __name__ == "__main__":
    main()
