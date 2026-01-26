from setuptools import setup

setup(
    name="nemweb-datasource",
    version="2.0.0",
    description="Custom PySpark data source for AEMO NEMWEB electricity market data",
    py_modules=[
        "nemweb_datasource",
        "nemweb_utils",
        "nemweb_ingest",
        "nemweb_sink",
        "nemweb_local",
        "local_spark_iceberg",
    ],
    python_requires=">=3.12",
)
