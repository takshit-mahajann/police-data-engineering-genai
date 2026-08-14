from pyspark.sql import SparkSession

from schema import POLICE_SCHEMA
from cleaning import clean_data
from quality import run_quality_checks

from logger import get_logger

from exceptions import (
    PipelineError,
    DataQualityError,
    InputDataError,
    PipelineWriteError
)

from config import (
    RAW_DIR,
    CURATED_DIR,
    ANALYTICS_DIR,
    APP_NAME,
    INITIAL_REPARTITION_COUNT,
    FINAL_COALESCE_COUNT,
    ROLLING_WINDOW_DAYS,
    WINDOW_RESULT_ROWS,
    TOP_RESULT_ROWS
)

from optimization import (
    explain_plan,
    get_partition_count as get_optimization_partition_count,
)

from partitioning import (
    get_partition_count,
    repartition_data,
    repartition_by_column,
    coalesce_data
)

from window import (
    add_previous_incident,
    add_next_incident,
    add_row_number,
    add_rank,
    add_dense_rank,
    add_minutes_since_previous,
    add_rolling_incident_count
)

from joins import (
    inner_join,
    left_join,
    right_join,
    full_outer_join
)

from cache import (
    persist_data,
    unpersist_data
)

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
# LOGGER
# ============================================================

logger = get_logger()


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():

    spark = None

    try:

        # ====================================================
        # SPARK SESSION
        # ====================================================

        spark = (
            SparkSession.builder
            .appName(APP_NAME)
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        logger.info(
            "Spark session created successfully."
        )


        # ====================================================
        # PIPELINE HEADER
        # ====================================================

        print("=" * 60)
        print("CAMBRIDGE POLICE PYSPARK PIPELINE")
        print("=" * 60)

        print("\nRaw data directory:")
        print(RAW_DIR)


        # ====================================================
        # FIND JSON FILES
        # ====================================================

        print("\nFinding JSON files...")

        json_files = [
            str(file)
            for file in RAW_DIR.rglob("*.json")
        ]

        print(
            f"JSON files found: "
            f"{len(json_files)}"
        )

        logger.info(
            f"JSON files found: {len(json_files)}"
        )


        # ====================================================
        # CHECK INPUT FILES
        # ====================================================

        if not json_files:

            logger.error(
                f"No JSON files found in: {RAW_DIR}"
            )

            raise InputDataError(
                f"No JSON files found in: {RAW_DIR}"
            )


        # ====================================================
        # READ RAW DATA
        # ====================================================

        print("\nReading raw JSON data...")

        df = (
            spark.read
            .schema(POLICE_SCHEMA)
            .option("multiLine", True)
            .json(json_files)
        )

        logger.info(
            "Raw JSON data loaded successfully."
        )


        # ====================================================
        # RECORD COUNT BEFORE CLEANING
        # ====================================================

        records_before = df.count()

        print(
            f"\nRecords before cleaning: "
            f"{records_before}"
        )

        logger.info(
            f"Records before cleaning: "
            f"{records_before}"
        )


        # ====================================================
        # SCHEMA BEFORE CLEANING
        # ====================================================

        print("\nSchema before cleaning:")

        df.printSchema()


        # ====================================================
        # CLEAN DATA
        # ====================================================

        print("\nRunning data cleaning...")

        df = clean_data(df)

        logger.info(
            "Data cleaning completed."
        )


        # ====================================================
        # RECORD COUNT AFTER CLEANING
        # ====================================================

        records_after = df.count()

        print(
            f"\nRecords after cleaning: "
            f"{records_after}"
        )

        print(
            f"Records removed: "
            f"{records_before - records_after}"
        )

        logger.info(
            f"Records after cleaning: "
            f"{records_after}"
        )

        logger.info(
            f"Records removed: "
            f"{records_before - records_after}"
        )


        # ====================================================
        # SCHEMA AFTER CLEANING
        # ====================================================

        print("\nSchema after cleaning:")

        df.printSchema()


        # ====================================================
        # DATA QUALITY CHECKS
        # ====================================================

        print(
            "\nRunning data quality checks..."
        )

        quality_passed = (
            run_quality_checks(df)
        )


        # ====================================================
        # STOP IF QUALITY CHECKS FAIL
        # ====================================================

        if not quality_passed:

            logger.error(
                "Data quality checks failed."
            )

            raise DataQualityError(
                "Data quality checks failed. "
                "Curated data will not be written."
            )

        logger.info(
            "Data quality checks passed."
        )


        # ====================================================
        # WINDOW FUNCTIONS
        # ====================================================

        print("\n")
        print("=" * 60)
        print("WINDOW FUNCTIONS")
        print("=" * 60)


        print(
            "\n1. Adding previous incident timestamp..."
        )

        df = add_previous_incident(df)


        print(
            "2. Adding next incident timestamp..."
        )

        df = add_next_incident(df)


        print(
            "3. Adding incident row number..."
        )

        df = add_row_number(df)


        print(
            "4. Adding incident rank..."
        )

        df = add_rank(df)


        print(
            "5. Adding dense rank..."
        )

        df = add_dense_rank(df)


        print(
            "6. Calculating minutes since previous incident..."
        )

        df = add_minutes_since_previous(df)


        print(
            "7. Calculating rolling incident count..."
        )

        df = add_rolling_incident_count(
            df,
            days=ROLLING_WINDOW_DAYS
        )


        logger.info(
            "Window functions completed."
        )


        # ====================================================
        # SHOW WINDOW RESULTS
        # ====================================================

        print(
            "\nWindow function results:"
        )

        df.select(
            "location",
            "date_time",
            "previous_date_time",
            "next_date_time",
            "incident_row_number",
            "incident_rank",
            "incident_dense_rank",
            "minutes_since_previous",
            "rolling_7_day_incident_count"
        ).show(
            WINDOW_RESULT_ROWS,
            truncate=False
        )


        # ====================================================
        # JOINS
        # ====================================================

        print("\n")
        print("=" * 60)
        print("JOIN PROCESSING")
        print("=" * 60)

        print(
            "\nJoin functions loaded successfully."
        )

        print(
            "Actual join will be performed when a "
            "real reference dataset is added."
        )

        logger.info(
            "Join module loaded. "
            "No reference dataset available for actual join."
        )


        # ====================================================
        # PARTITIONING
        # ====================================================

        print("\n")
        print("=" * 60)
        print("PARTITIONING")
        print("=" * 60)


        # ----------------------------------------------------
        # CURRENT PARTITIONS
        # ----------------------------------------------------

        initial_partitions = (
            get_partition_count(df)
        )

        print(
            f"\nInitial number of partitions: "
            f"{initial_partitions}"
        )


        # ----------------------------------------------------
        # REPARTITION
        # ----------------------------------------------------

        print(
            "\nRepartitioning data..."
        )

        df = repartition_data(
            df,
            INITIAL_REPARTITION_COUNT
        )

        repartitioned_partitions = (
            get_partition_count(df)
        )

        print(
            f"Partitions after repartition: "
            f"{repartitioned_partitions}"
        )


        # ----------------------------------------------------
        # REPARTITION BY LOCATION
        # ----------------------------------------------------

        print(
            "\nRepartitioning by location..."
        )

        df = repartition_by_column(
            df,
            INITIAL_REPARTITION_COUNT,
            "location"
        )

        location_partitions = (
            get_partition_count(df)
        )

        print(
            f"Partitions after location repartition: "
            f"{location_partitions}"
        )


        # ----------------------------------------------------
        # COALESCE
        # ----------------------------------------------------

        print(
            "\nReducing partitions using coalesce..."
        )

        df = coalesce_data(
            df,
            FINAL_COALESCE_COUNT
        )

        final_partitions = (
            get_partition_count(df)
        )

        print(
            f"Partitions after coalesce: "
            f"{final_partitions}"
        )

        logger.info(
            f"Final partition count: "
            f"{final_partitions}"
        )


        # ====================================================
        # CACHING AND PERSISTENCE
        # ====================================================

        print("\n")
        print("=" * 60)
        print("CACHING AND PERSISTENCE")
        print("=" * 60)


        print(
            "\nPersisting transformed DataFrame..."
        )

        df = persist_data(df)


        print(
            "Materializing persisted DataFrame..."
        )

        persisted_count = df.count()

        print(
            f"Persisted records: "
            f"{persisted_count}"
        )

        logger.info(
            "DataFrame persisted successfully."
        )


        # ====================================================
        # PERFORMANCE OPTIMIZATION
        # ====================================================

        print("\n")
        print("=" * 60)
        print("PERFORMANCE OPTIMIZATION")
        print("=" * 60)


        print(
            "\nCurrent partition count:"
        )

        current_partitions = (
            get_optimization_partition_count(df)
        )

        print(
            f"Partitions: "
            f"{current_partitions}"
        )


        print(
            "\nSpark execution plan:"
        )

        explain_plan(
            df,
            extended=False
        )


        logger.info(
            "Performance analysis completed."
        )


        # ====================================================
        # ANALYTICS / AGGREGATIONS
        # ====================================================

        print("\n")
        print("=" * 60)
        print("ANALYTICS AGGREGATIONS")
        print("=" * 60)


        # ----------------------------------------------------
        # 1. INCIDENTS BY TYPE
        # ----------------------------------------------------

        print(
            "\nIncidents by Type:"
        )

        type_df = incidents_by_type(df)

        type_df.show(
            truncate=False
        )


        # ----------------------------------------------------
        # 2. INCIDENTS BY MONTH
        # ----------------------------------------------------

        print(
            "\nIncidents by Month:"
        )

        month_df = incidents_by_month(df)

        month_df.show(
            truncate=False
        )


        # ----------------------------------------------------
        # 3. INCIDENTS BY LOCATION
        # ----------------------------------------------------

        print(
            "\nTop Locations:"
        )

        location_df = (
            incidents_by_location(df)
        )

        location_df.show(
            TOP_RESULT_ROWS,
            truncate=False
        )


        # ----------------------------------------------------
        # 4. INCIDENTS BY HOUR
        # ----------------------------------------------------

        print(
            "\nIncidents by Hour:"
        )

        hour_df = incidents_by_hour(df)

        hour_df.show(
            truncate=False
        )


        # ----------------------------------------------------
        # 5. INCIDENTS BY SUBTYPE
        # ----------------------------------------------------

        print(
            "\nTop Subtypes:"
        )

        subtype_df = (
            incidents_by_subtype(df)
        )

        subtype_df.show(
            TOP_RESULT_ROWS,
            truncate=False
        )


        logger.info(
            "All aggregations completed successfully."
        )


        # ====================================================
        # WRITE CURATED DATA
        # ====================================================

        print("\n")
        print("=" * 60)
        print("WRITING CURATED DATA")
        print("=" * 60)

        try:

            write_curated_data(
                df,
                CURATED_DIR
            )

        except Exception as e:

            logger.exception(
                "Failed to write curated data."
            )

            raise PipelineWriteError(
                f"Failed to write curated data: {e}"
            ) from e


        # ====================================================
        # WRITE ANALYTICS DATA
        # ====================================================

        print("\n")
        print("=" * 60)
        print("WRITING ANALYTICS DATA")
        print("=" * 60)

        try:

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

        except Exception as e:

            logger.exception(
                "Failed to write analytics data."
            )

            raise PipelineWriteError(
                f"Failed to write analytics data: {e}"
            ) from e


        logger.info(
            "All output data written successfully."
        )


        # ====================================================
        # RELEASE PERSISTED DATA
        # ====================================================

        print(
            "\nReleasing persisted DataFrame..."
        )

        unpersist_data(df)

        logger.info(
            "Persisted DataFrame released."
        )


        # ====================================================
        # FINAL SUMMARY
        # ====================================================

        print("\n")
        print("=" * 60)
        print(
            "PYSPARK PIPELINE COMPLETED SUCCESSFULLY"
        )
        print("=" * 60)

        print(
            f"\nInput records: "
            f"{records_before}"
        )

        print(
            f"Cleaned records: "
            f"{records_after}"
        )

        print(
            f"Records removed: "
            f"{records_before - records_after}"
        )

        print(
            "\nCurated output:"
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

        logger.info(
            "PySpark pipeline completed successfully."
        )


    # ========================================================
    # PIPELINE ERRORS
    # ========================================================

    except PipelineError as e:

        logger.error(
            f"Pipeline failed: {e}"
        )

        raise


    # ========================================================
    # UNEXPECTED ERRORS
    # ========================================================

    except Exception:

        logger.exception(
            "Unexpected pipeline failure."
        )

        raise


    # ========================================================
    # ALWAYS STOP SPARK
    # ========================================================

    finally:

        if spark is not None:

            spark.stop()

            logger.info(
                "Spark session stopped."
            )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()