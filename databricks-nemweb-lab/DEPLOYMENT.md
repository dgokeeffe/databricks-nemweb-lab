# Deployment Guide

This project supports two deployment methods:

## Method 1: Databricks Asset Bundles (Recommended) ✅

**Modern, automated deployment** - handles versioning, builds, and deployments automatically.

### How it works:
- `databricks.yml` defines the bundle configuration
- The `artifacts` section automatically builds the wheel during deployment
- Version is read from `src/pyproject.toml`
- Everything is deployed via `databricks bundle deploy`

### Usage:
```bash
# Build and deploy everything
databricks bundle deploy --var="environment=dev"

# Deploy to production
databricks bundle deploy --target prod --var="environment=prod"

# Run workflows
databricks bundle run nemweb_lab_workflow --target dev --var="environment=dev"
```

### Benefits:
- ✅ Automatic versioning from `pyproject.toml`
- ✅ No manual version bumps needed
- ✅ Integrated with CI/CD
- ✅ Manages all resources (pipelines, jobs, notebooks)
- ✅ Idempotent deployments

### Version Management:
- Version is defined in `src/pyproject.toml` → `version = "2.10.4"`
- When you change code, bump the version in `pyproject.toml`
- DABs automatically builds and deploys the new version
- No need to update scripts or hardcode versions

---

## Method 2: Legacy Manual Scripts (Not Recommended)

**Legacy scripts** for manual deployment - kept for backwards compatibility.

### Scripts:

#### `build_latest.sh`
- Builds wheel locally
- Copies to `artifacts/` directory
- Creates `environment.yml` (requires manual path editing)
- **Does NOT deploy** - just builds

#### `deploy_wheel.sh`
- Builds wheel
- Deploys to UC Volume via `databricks fs cp`
- Creates `environment.yml` on volume
- **Now reads version from `pyproject.toml`** (updated)
- Requires manual catalog/schema parameters

### Usage:
```bash
# Build only
./build_latest.sh

# Deploy to UC Volume
./deploy_wheel.sh workspace nemweb_lab
```

### Limitations:
- ❌ Requires manual version management (now fixed - reads from pyproject.toml)
- ❌ Doesn't deploy other resources (pipelines, jobs)
- ❌ Not integrated with CI/CD
- ❌ Manual steps required

---

## Version Management

### Where versions are defined:

1. **`src/pyproject.toml`** (Source of truth)
   ```toml
   version = "2.10.4"
   ```

2. **`src/nemweb_utils.py`** (For runtime version checking)
   ```python
   __version__ = "2.10.4"
   ```

### When to bump version:

- ✅ When you make code changes that affect functionality
- ✅ When fixing bugs (patch version: 2.10.4 → 2.10.5)
- ✅ When adding features (minor version: 2.10.4 → 2.11.0)
- ✅ When making breaking changes (major version: 2.10.4 → 3.0.0)

### How to bump version:

1. Update `src/pyproject.toml`:
   ```toml
   version = "2.10.5"  # or 2.11.0, 3.0.0, etc.
   ```

2. Update `src/nemweb_utils.py`:
   ```python
   __version__ = "2.10.5"
   ```

3. Deploy:
   ```bash
   # With DABs (recommended)
   databricks bundle deploy --var="environment=dev"
   
   # Or with legacy script
   ./deploy_wheel.sh workspace nemweb_lab
   ```

---

## Why Use Databricks Asset Bundles?

### Comparison:

| Feature | DABs (`databricks.yml`) | Legacy Scripts |
|---------|------------------------|-----------------|
| Version management | ✅ Automatic from `pyproject.toml` | ⚠️ Manual (now reads from pyproject.toml) |
| Build process | ✅ Integrated | ⚠️ Separate step |
| Deployment | ✅ All resources | ⚠️ Wheel only |
| CI/CD integration | ✅ Native | ❌ Manual |
| Idempotency | ✅ Built-in | ⚠️ Manual |
| Rollback | ✅ Easy | ❌ Difficult |
| State management | ✅ Automatic | ❌ None |

### Recommendation:

**Use Databricks Asset Bundles for all deployments.** The legacy scripts are kept for:
- Quick testing without full bundle deployment
- Backwards compatibility
- Edge cases where DABs might not fit

---

## Quick Reference

### Full deployment (recommended):
```bash
databricks bundle deploy --var="environment=dev"
```

### Build only (for testing):
```bash
./build_latest.sh
```

### Manual wheel deployment (legacy):
```bash
./deploy_wheel.sh workspace nemweb_lab
```

### Check current version:
```bash
grep '^version' databricks-nemweb-lab/src/pyproject.toml
```
