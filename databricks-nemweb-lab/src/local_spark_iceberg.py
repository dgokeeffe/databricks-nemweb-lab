"""
Local Spark + Iceberg setup for testing NEMWEB data source.

This creates a local SparkSession with Iceberg catalog, allowing you to
test the full data source API locally before deploying to Databricks.

Usage:
    from local_spark_iceberg import get_local_spark, cleanup_warehouse

    # Get Spark session with Iceberg
    spark = get_local_spark()

    # Register and use the NEMWEB data source
    from nemweb_datasource import NemwebDataSource
    spark.dataSource.register(NemwebDataSource)

    df = (spark.read
          .format("nemweb")
          .option("regions", "NSW1,VIC1")
          .option("start_date", "2024-01-01")
          .option("end_date", "2024-01-03")
          .load())

    # Write to Iceberg table
    df.writeTo("local.nemweb.bronze").createOrReplace()

    # Query with SQL
    spark.sql("SELECT * FROM local.nemweb.bronze WHERE REGIONID = 'NSW1'").show()

Requirements:
    pip install pyspark==3.5.0
    pip install pyiceberg[pyarrow,duckdb]

Note: PySpark 3.5 is used for compatibility. Spark 4.0 features (like Python
DataSource API) require DBR 15.4+ on Databricks.
"""

import os
import shutil
from pathlib import Path


def get_local_spark(
    warehouse_path: str = None,
    app_name: str = "NemwebLocal"
):
    """
    Create a local SparkSession with Iceberg catalog.

    Args:
        warehouse_path: Path for Iceberg warehouse (default: ./iceberg-warehouse)
        app_name: Spark application name

    Returns:
        SparkSession configured with Iceberg
    """
    from pyspark.sql import SparkSession

    if warehouse_path is None:
        warehouse_path = str(Path.cwd() / "iceberg-warehouse")

    # Ensure warehouse directory exists
    Path(warehouse_path).mkdir(parents=True, exist_ok=True)

    # Iceberg Spark runtime JAR - needs to match your Spark version
    # For Spark 3.5: iceberg-spark-runtime-3.5_2.12
    iceberg_version = "1.4.2"
    spark_version = "3.5"
    scala_version = "2.12"

    spark = (SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # Add Iceberg packages
        .config("spark.jars.packages",
                f"org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version}")

        # Configure Iceberg catalog
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)

        # Default to Iceberg for CREATE TABLE
        .config("spark.sql.defaultCatalog", "local")

        # Iceberg SQL extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        .getOrCreate())

    # Create default namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.nemweb")

    print(f"SparkSession created with Iceberg catalog")
    print(f"Warehouse: {warehouse_path}")
    print(f"Default catalog: local")
    print(f"Default namespace: local.nemweb")

    return spark


def cleanup_warehouse(warehouse_path: str = None):
    """Remove the local Iceberg warehouse directory."""
    if warehouse_path is None:
        warehouse_path = str(Path.cwd() / "iceberg-warehouse")

    if Path(warehouse_path).exists():
        shutil.rmtree(warehouse_path)
        print(f"Cleaned up: {warehouse_path}")


def demo_with_sample_data():
    """
    Demo using sample data (no NEMWEB fetch).

    This shows the Iceberg setup works without network calls.
    """
    spark = get_local_spark()

    # Create sample data
    data = [
        ("2024-01-01 00:05:00", "NSW1", 7500.5, 8000.0, -200.5),
        ("2024-01-01 00:05:00", "VIC1", 5200.3, 5500.0, 150.2),
        ("2024-01-01 00:10:00", "NSW1", 7520.0, 8000.0, -180.0),
        ("2024-01-01 00:10:00", "VIC1", 5180.0, 5500.0, 160.0),
    ]

    df = spark.createDataFrame(data, [
        "SETTLEMENTDATE", "REGIONID", "TOTALDEMAND",
        "AVAILABLEGENERATION", "NETINTERCHANGE"
    ])

    # Write to Iceberg
    df.writeTo("local.nemweb.sample_bronze").createOrReplace()
    print("\nWrote sample data to local.nemweb.sample_bronze")

    # Query it back
    print("\nQuerying Iceberg table:")
    spark.sql("""
        SELECT REGIONID, AVG(TOTALDEMAND) as avg_demand
        FROM local.nemweb.sample_bronze
        GROUP BY REGIONID
    """).show()

    # Show table history (Iceberg feature)
    print("\nTable snapshots:")
    spark.sql("SELECT * FROM local.nemweb.sample_bronze.snapshots").show(truncate=False)

    return spark


def demo_with_nemweb():
    """
    Demo with actual NEMWEB data source.

    Note: Python DataSource API requires Spark 4.0 (DBR 15.4+).
    This will fail on local Spark 3.5 but shows the intended usage.
    """
    spark = get_local_spark()

    try:
        # This import works but registration requires Spark 4.0
        from nemweb_datasource import NemwebDataSource

        # This will fail on Spark < 4.0
        spark.dataSource.register(NemwebDataSource)

        df = (spark.read
              .format("nemweb")
              .option("regions", "NSW1")
              .option("start_date", "2024-01-01")
              .option("end_date", "2024-01-01")
              .load())

        df.writeTo("local.nemweb.bronze").createOrReplace()
        print("Success! Data written to local.nemweb.bronze")

    except AttributeError as e:
        if "dataSource" in str(e):
            print("\nNote: spark.dataSource.register() requires Spark 4.0 (DBR 15.4+)")
            print("For local testing with Spark 3.5, use the sample data demo or nemweb_local.py")
        else:
            raise

    return spark


if __name__ == "__main__":
    print("=" * 60)
    print("Local Spark + Iceberg Demo")
    print("=" * 60)

    # Run sample data demo (works with any Spark version)
    spark = demo_with_sample_data()

    print("\n" + "=" * 60)
    print("Attempting NEMWEB DataSource (requires Spark 4.0)...")
    print("=" * 60)

    # Try NEMWEB data source (will show note about Spark version)
    demo_with_nemweb()

    # Cleanup
    # cleanup_warehouse()
