# Databricks notebook source
# MAGIC %md
# MAGIC # Arrow Serialization Error Reproduction
# MAGIC
# MAGIC This notebook reproduces the Arrow assertion error on Serverless when
# MAGIC yielding tuples with datetime values from a custom datasource.
# MAGIC
# MAGIC **The Bug:** `yield (datetime(...), ...)` fails on Serverless
# MAGIC **The Fix:** `yield Row(timestamp=datetime(...))` works
# MAGIC
# MAGIC **Run on Serverless to reproduce the error.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the wheel

# COMMAND ----------

%pip install /Volumes/daveok/default/datasource/arrow_repro-0.1.6-py3-none-any.whl -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Minimal Repro (hardcoded datetime)

# COMMAND ----------

from arrow_repro import BrokenArrowDataSource

spark.dataSource.register(BrokenArrowDataSource)
print("Registered: broken_arrow")

# COMMAND ----------

# This will fail on Serverless with AssertionError
df = spark.read.format("broken_arrow").load()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: NEMWEB Repro (fetches real CSV data)

# COMMAND ----------

from arrow_repro import NemwebArrowDataSource

spark.dataSource.register(NemwebArrowDataSource)
print("Registered: nemweb_broken")

# COMMAND ----------

# This will also fail - fetches real NEMWEB data and yields tuples with datetime
df = spark.read.format("nemweb_broken").option("max_files", "1").load()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Fix
# MAGIC
# MAGIC Use `Row` objects instead of tuples:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import Row
# MAGIC
# MAGIC # Instead of:
# MAGIC yield (datetime(2026, 1, 28), "NSW1", 7500.5)
# MAGIC
# MAGIC # Use:
# MAGIC yield Row(timestamp=datetime(2026, 1, 28), region="NSW1", demand=7500.5)
# MAGIC ```
