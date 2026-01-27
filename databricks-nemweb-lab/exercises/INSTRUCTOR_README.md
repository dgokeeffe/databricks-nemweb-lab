# Instructor Guide: Exercise 1 Options

## Primary Exercise (01_custom_source_exercise.py)
Uses real AEMO NEMWEB data via HTTP with PyArrow RecordBatch.

**Requirements:**
- Internet access to nemweb.com.au
- PyArrow (included in DBR 15.4+)
- Serverless or DBR 15.4+ cluster

**Known Issues:**
- If Arrow serialization fails, switch to backup
- If NEMWEB is down, switch to backup

## Backup Exercise (01_custom_source_exercise_backup.py)
Generates synthetic data locally - no external dependencies.

**When to use:**
1. NEMWEB HTTP endpoints unavailable
2. Arrow/Serverless serialization issues
3. Network restrictions prevent external access
4. Need a guaranteed-working fallback

**Trade-offs:**
- Uses StringType only (cast to proper types after loading)
- No real data (but teaches same concepts)
- Always works regardless of network/Arrow issues

## Quick Switch
If issues arise during the workshop:

1. Tell participants to open `01_custom_source_exercise_backup.py` instead
2. Use `01_custom_source_solution_backup.py` for the solution
3. The format name is `nemweb_fake` (not `nemweb`)

Both exercises teach the same core concepts:
- DataSource class structure
- Schema definition
- Partition planning
- Reader implementation
