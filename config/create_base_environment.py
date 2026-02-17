#!/usr/bin/env python3
"""
Create a workspace base environment for serverless compute (SVLS 4).

Uses the Databricks SDK to:
1. Upload the base environment YAML spec to the workspace.
2. Print instructions for the one UI step (register the base environment with a name).

Base environment creation is documented at:
https://learn.microsoft.com/en-gb/azure/databricks/admin/workspace-settings/base-environment#add-a-base-environment-to-your-workspace

There is no public REST/SDK API to "create" (register) a base environment with a name;
that step is done in the workspace UI. This script automates uploading the spec so the
file is available for the file picker.

Requires: databricks-sdk, workspace admin permissions for the UI step.
Usage:
  uv run python config/create_base_environment.py
  uv run python config/create_base_environment.py --workspace-path /Workspace/Shared/nemweb-ml-lab/base-environment-svls4.yaml
"""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

# Repo root: config/ is under repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SPEC_PATH = REPO_ROOT / "config" / "base-environment-svls4.yaml"
DEFAULT_WORKSPACE_PATH = "/Workspace/Shared/base-environment-svls4.yaml"
DEFAULT_BASE_ENV_NAME = "NEMWEB ML SVLS4"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload base environment YAML to workspace and print UI instructions."
    )
    parser.add_argument(
        "--spec-path",
        type=Path,
        default=DEFAULT_SPEC_PATH,
        help=f"Local path to the base environment YAML (default: {DEFAULT_SPEC_PATH})",
    )
    parser.add_argument(
        "--workspace-path",
        type=str,
        default=DEFAULT_WORKSPACE_PATH,
        help=f"Workspace path where the YAML will be uploaded (default: {DEFAULT_WORKSPACE_PATH})",
    )
    parser.add_argument(
        "--name",
        type=str,
        default=DEFAULT_BASE_ENV_NAME,
        help=f"Name to use when creating the base environment in the UI (default: {DEFAULT_BASE_ENV_NAME})",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the file if it already exists in the workspace",
    )
    args = parser.parse_args()

    if not args.spec_path.exists():
        print(f"Error: Spec file not found: {args.spec_path}", file=sys.stderr)
        return 1

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.workspace import ImportFormat
    except ImportError:
        print(
            "Error: databricks-sdk is required. Install with: uv pip install databricks-sdk",
            file=sys.stderr,
        )
        return 1

    content = args.spec_path.read_bytes()
    path = args.workspace_path.strip()
    if not path.startswith("/"):
        path = "/" + path

    try:
        w = WorkspaceClient()
        parent = str(Path(path).parent)
        # Prefer Files API so the file appears as a real .yaml file (Workspace Import can strip extension)
        try:
            if parent and parent not in ("/", ""):
                print("Creating parent directory...")
                w.files.create_directory(parent)
            print("Uploading base environment spec (Files API)...")
            w.files.upload(path, io.BytesIO(content), overwrite=args.overwrite)
            print(f"Uploaded to: {path}")
        except Exception as files_err:
            err_msg = str(files_err).lower()
            if "internal workspace storage" in err_msg or "not supported" in err_msg:
                # Fallback: Workspace Import API (file may appear without .yaml extension)
                print("Files API not available for workspace path, using Workspace Import...")
                if parent and parent not in ("/", ""):
                    w.workspace.mkdirs(parent)
                w.workspace.upload(
                    path,
                    io.BytesIO(content),
                    format=ImportFormat.AUTO,
                    overwrite=args.overwrite,
                )
                print(f"Uploaded to: {path}")
                print("Note: If the file does not appear as .yaml in the UI, upload the YAML manually")
                print("  (e.g. create a file in Workspace > Shared and paste config/base-environment-svls4.yaml).")
            else:
                raise files_err
    except Exception as e:
        print(f"Upload failed: {e}", file=sys.stderr)
        return 1

    print()
    print("Next step (UI): register the base environment in your workspace.")
    print("  See: https://learn.microsoft.com/en-gb/azure/databricks/admin/workspace-settings/base-environment#add-a-base-environment-to-your-workspace")
    print()
    print("  1. In the workspace, go to Settings.")
    print("  2. Under Workspace admin, select Compute.")
    print("  3. Next to 'Base environments for serverless compute', click Manage.")
    print("  4. Click Create new environment.")
    print(f"  5. Name: {args.name}")
    print(f"  6. Select the environment specification file: {path}")
    print("  7. Click Create.")
    print()
    print("When status is 'Ready to use', users can select this base environment from the")
    print("Base environment dropdown in the Environment side panel for notebooks.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
