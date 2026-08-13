from pathlib import Path

from pyspark.sql import SparkSession

from schema import POLICE_SCHEMA
from cleaning import clean_data
from quality import run_quality_checks

from aggregations import (
    incidents_by_type,
    incidents_by_month,
    incidents_by_location,
    incidents_by_hour,
    incidents_by_subtype
)

from storage import (
    write_curated_data,
    write_analytics_data
)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("CambridgePoliceData")
    .getOrCreate()
)


# ============================================================
# PATH CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "ingestion" / "raw"

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

CURATED_DIR = OUTPUT_DIR / "curated"

ANALYTICS_DIR = OUTPUT_DIR / "analytics"


# ============================================================
# FIND JSON FILES
# ============================================================

print("=" * 60)
print("CAMBRIDGE POLICE PYSPARK PIPELINE")
print("=" * 60)

print("\nRaw data directory:")
print(RAW_DIR)

print("\nFinding JSON files...")

json_files = [
    str(file)
    for file in RAW_DIR.rglob("*.json")
]

print(
    f"JSON files found: {len(json_files)}"
)


# ============================================================
# CHECK FILES
# ============================================================

if not json_files:
    raise FileNotFoundError(
        f"No JSON files found in: {RAW_DIR}"
    )


# ============================================================
# READ RAW DATA
# ============================================================

print("\nReading raw JSON data...")

df = (
    spark.read
    .schema(POLICE_SCHEMA)
    .option("multiLine", True)
    .json(json_files)
)


# ============================================================
# RECORD COUNT BEFORE CLEANING
# ============================================================

records_before = df.count()

print(
    f"\nRecords before cleaning: "
    f"{records_before}"
)


# ============================================================
# SCHEMA BEFORE CLEANING
# ============================================================

print("\nSchema before cleaning:")

df.printSchema()


# ============================================================
# CLEAN DATA
# ============================================================

print("\nRunning data cleaning...")

df = clean_data(df)


# ============================================================
# RECORD COUNT AFTER CLEANING
# ============================================================

records_after = df.count()

print(
    f"\nRecords after cleaning: "
    f"{records_after}"
)

print(
    f"Records removed: "
    f"{records_before - records_after}"
)


# ============================================================
# SCHEMA AFTER CLEANING
# ============================================================

print("\nSchema after cleaning:")

df.printSchema()


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

quality_passed = run_quality_checks(df)


# ============================================================
# STOP IF QUALITY CHECKS FAIL
# ============================================================

if not quality_passed:

    raise ValueError(
        "Data quality checks failed. "
        "Curated data will not be written."
    )


# ============================================================
# SHOW CLEANED DATA
# ============================================================

print("\nFirst 10 cleaned records:")

df.show(
    10,
    truncate=False
)


# ============================================================
# ANALYTICS / AGGREGATIONS
# ============================================================

print("\n")
print("=" * 60)
print("ANALYTICS AGGREGATIONS")
print("=" * 60)


# ------------------------------------------------------------
# 1. INCIDENTS BY TYPE
# ------------------------------------------------------------

print("\nIncidents by Type:")

type_df = incidents_by_type(df)

type_df.show(
    truncate=False
)


# ------------------------------------------------------------
# 2. INCIDENTS BY MONTH
# ------------------------------------------------------------

print("\nIncidents by Month:")

month_df = incidents_by_month(df)

month_df.show(
    truncate=False
)


# ------------------------------------------------------------
# 3. INCIDENTS BY LOCATION
# ------------------------------------------------------------

print("\nTop Locations:")

location_df = incidents_by_location(df)

location_df.show(
    20,
    truncate=False
)


# ------------------------------------------------------------
# 4. INCIDENTS BY HOUR
# ------------------------------------------------------------

print("\nIncidents by Hour:")

hour_df = incidents_by_hour(df)

hour_df.show(
    truncate=False
)


# ------------------------------------------------------------
# 5. INCIDENTS BY SUBTYPE
# ------------------------------------------------------------

print("\nTop Subtypes:")

subtype_df = incidents_by_subtype(df)

subtype_df.show(
    20,
    truncate=False
)


# ============================================================
# WRITE CURATED DATA
# ============================================================

print("\n")
print("=" * 60)
print("WRITING CURATED DATA")
print("=" * 60)

write_curated_data(
    df,
    CURATED_DIR
)


# ============================================================
# WRITE ANALYTICS DATA
# ============================================================

print("\n")
print("=" * 60)
print("WRITING ANALYTICS DATA")
print("=" * 60)


write_analytics_data(
    type_df,
    ANALYTICS_DIR / "by_type"
)


write_analytics_data(
    month_df,
    ANALYTICS_DIR / "by_month"
)


write_analytics_data(
    location_df,
    ANALYTICS_DIR / "by_location"
)


write_analytics_data(
    hour_df,
    ANALYTICS_DIR / "by_hour"
)


write_analytics_data(
    subtype_df,
    ANALYTICS_DIR / "by_subtype"
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n")
print("=" * 60)
print("PYSPARK PIPELINE COMPLETED SUCCESSFULLY")
print("=" * 60)

print(
    f"\nInput records: {records_before}"
)

print(
    f"Cleaned records: {records_after}"
)

print(
    f"Records removed: "
    f"{records_before - records_after}"
)

print(
    f"\nCurated output:"
)

print(
    CURATED_DIR
)

print(
    "\nAnalytics output:"
)

print(
    ANALYTICS_DIR
)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()