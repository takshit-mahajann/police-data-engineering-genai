from pyspark.sql import SparkSession


# ============================================================
# SCHEMA / CLEANING / QUALITY
# ============================================================

from schema import POLICE_SCHEMA
from schema_validation import validate_schema
from cleaning import clean_data
from quality import run_quality_checks


# ============================================================
# LOGGING / EXCEPTIONS
# ============================================================

from logger import get_logger

from exceptions import (
    PipelineError,
    DataQualityError,
    InputDataError,
    PipelineWriteError
)


# ============================================================
# CONFIGURATION
# ============================================================

from config import (
    RAW_DIR,
    CURATED_DIR,
    ANALYTICS_DIR,
    CHECKPOINT_FILE,
    APP_NAME,
    INITIAL_REPARTITION_COUNT,
    FINAL_COALESCE_COUNT,
    ROLLING_WINDOW_DAYS,
    WINDOW_RESULT_ROWS,
    TOP_RESULT_ROWS
)


# ============================================================
# INCREMENTAL PROCESSING
# ============================================================

from incremental import (
    load_checkpoint,
    save_checkpoint,
    get_new_files,
    add_processed_files
)


# ============================================================
# OPTIMIZATION
# ============================================================

from optimization import (
    explain_plan,
    get_partition_count as get_optimization_partition_count
)


# ============================================================
# PARTITIONING
# ============================================================

from partitioning import (
    get_partition_count,
    repartition_data,
    repartition_by_column,
    coalesce_data
)


# ============================================================
# WINDOW FUNCTIONS
# ============================================================

from window import (
    add_previous_incident,
    add_next_incident,
    add_row_number,
    add_rank,
    add_dense_rank,
    add_minutes_since_previous,
    add_rolling_incident_count
)


# ============================================================
# JOINS
# ============================================================

from joins import (
    inner_join,
    left_join,
    right_join,
    full_outer_join
)


# ============================================================
# CACHE / PERSISTENCE
# ============================================================

from cache import (
    persist_data,
    unpersist_data
)


# ============================================================
# DEDUPLICATION
# ============================================================

from deduplication import (
    remove_batch_duplicates,
    remove_existing_records
)


# ============================================================
# AGGREGATIONS
# ============================================================

from aggregations import (
    incidents_by_type,
    incidents_by_month,
    incidents_by_location,
    incidents_by_hour,
    incidents_by_subtype
)


# ============================================================
# STORAGE
# ============================================================

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
        # HEADER
        # ====================================================

        print("=" * 60)
        print("CAMBRIDGE POLICE PYSPARK PIPELINE")
        print("=" * 60)

        print("\nRaw data directory:")
        print(RAW_DIR)

        print("\nCheckpoint file:")
        print(CHECKPOINT_FILE)

        # ====================================================
        # LOAD CHECKPOINT
        # ====================================================

        print("\nLoading incremental checkpoint...")

        processed_files = load_checkpoint(
            CHECKPOINT_FILE
        )

        print(
            f"Previously processed files: "
            f"{len(processed_files)}"
        )

        logger.info(
            f"Previously processed files: "
            f"{len(processed_files)}"
        )

        # ====================================================
        # FIND NEW FILES
        # ====================================================

        print("\nFinding new JSON files...")

        new_files = get_new_files(
            RAW_DIR,
            processed_files
        )

        print(
            f"New JSON files found: "
            f"{len(new_files)}"
        )

        logger.info(
            f"New JSON files found: "
            f"{len(new_files)}"
        )

        # ====================================================
        # NO NEW DATA
        # ====================================================

        if not new_files:

            print(
                "\nNo new data to process."
            )

            print(
                "\nPYSPARK PIPELINE COMPLETED"
            )

            print(
                "No new files were detected."
            )

            logger.info(
                "No new files detected. "
                "Pipeline completed without processing."
            )

            return

        # ====================================================
        # DISPLAY NEW FILES
        # ====================================================

        print("\nFiles to be processed:")

        for file in new_files:

            print(
                f"  - {file}"
            )

        logger.info(
            f"Processing {len(new_files)} new files."
        )

        # ====================================================
        # READ RAW JSON
        # ====================================================

        print(
            "\nReading new raw JSON data..."
        )

        json_files = [
            str(file)
            for file in new_files
        ]

        try:

            df = (
                spark.read
                .schema(POLICE_SCHEMA)
                .option("multiLine", True)
                .json(json_files)
            )

        except Exception as e:

            logger.exception(
                "Failed to read raw JSON data."
            )

            raise InputDataError(
                f"Unable to read input JSON files: {e}"
            ) from e

        logger.info(
            "New raw JSON data loaded successfully."
        )

        # ====================================================
        # SCHEMA VALIDATION
        # ====================================================

        print("\n")
        print("=" * 60)
        print("SCHEMA VALIDATION")
        print("=" * 60)

        schema_valid = validate_schema(
            df
        )

        if not schema_valid:

            logger.error(
                "Incoming data failed schema validation."
            )

            raise InputDataError(
                "Incoming data does not match "
                "the expected schema."
            )

        logger.info(
            "Schema validation passed."
        )

        # ====================================================
        # RECORD COUNT BEFORE CLEANING
        # ====================================================

        records_before = df.count()

        print(
            f"\nNew records before cleaning: "
            f"{records_before}"
        )

        logger.info(
            f"New records before cleaning: "
            f"{records_before}"
        )

        # ====================================================
        # SCHEMA BEFORE CLEANING
        # ====================================================

        print(
            "\nSchema before cleaning:"
        )

        df.printSchema()

        # ====================================================
        # CLEAN DATA
        # ====================================================

        print(
            "\nRunning data cleaning..."
        )

        df = clean_data(
            df
        )

        logger.info(
            "Data cleaning completed."
        )

        # ====================================================
        # RECORD COUNT AFTER CLEANING
        # ====================================================

        records_after = df.count()

        print(
            f"\nNew records after cleaning: "
            f"{records_after}"
        )

        print(
            f"Records removed from new batch: "
            f"{records_before - records_after}"
        )

        logger.info(
            f"New records after cleaning: "
            f"{records_after}"
        )

        # ====================================================
        # SCHEMA AFTER CLEANING
        # ====================================================

        print(
            "\nSchema after cleaning:"
        )

        df.printSchema()

        # ====================================================
        # DATA QUALITY
        # ====================================================

        print(
            "\nRunning data quality checks..."
        )

        quality_passed = (
            run_quality_checks(df)
        )

        # ====================================================
        # QUALITY FAILURE
        # ====================================================

        if not quality_passed:

            logger.error(
                "Data quality checks failed."
            )

            raise DataQualityError(
                "Data quality checks failed. "
                "Curated data will not be written "
                "and checkpoint will not be updated."
            )

        logger.info(
            "Data quality checks passed."
        )

        # ====================================================
        # DEDUPLICATION
        # ====================================================

        print("\n")
        print("=" * 60)
        print("DEDUPLICATION")
        print("=" * 60)

        # ----------------------------------------------------
        # CURRENT BATCH DUPLICATES
        # ----------------------------------------------------

        print(
            "\nRemoving duplicate incidents "
            "within current batch..."
        )

        df = remove_batch_duplicates(
            df
        )

        # ----------------------------------------------------
        # EXISTING CURATED DATA
        # ----------------------------------------------------

        if CURATED_DIR.exists():

            print(
                "\nExisting curated data found."
            )

            print(
                "Checking new records against "
                "existing curated data..."
            )

            try:

                curated_df = (
                    spark.read
                    .parquet(
                        str(CURATED_DIR)
                    )
                )

                df = remove_existing_records(
                    df,
                    curated_df
                )

            except Exception as e:

                logger.exception(
                    "Failed while checking "
                    "existing curated data."
                )

                raise PipelineWriteError(
                    "Unable to perform "
                    "incremental deduplication."
                ) from e

        else:

            print(
                "\nNo existing curated data found."
            )

            print(
                "Treating current batch as "
                "the initial dataset."
            )

        # ====================================================
        # DEDUPLICATION COUNT
        # ====================================================

        deduplicated_count = df.count()

        print(
            f"\nRecords remaining after "
            f"deduplication: "
            f"{deduplicated_count}"
        )

        logger.info(
            f"Records remaining after "
            f"deduplication: "
            f"{deduplicated_count}"
        )

        # ====================================================
        # NO NEW RECORDS AFTER DEDUPLICATION
        # ====================================================

        if deduplicated_count == 0:

            print(
                "\nNo genuinely new incidents "
                "remain after deduplication."
            )

            logger.info(
                "No genuinely new incidents "
                "remain after deduplication."
            )

            updated_processed_files = (
                add_processed_files(
                    processed_files,
                    new_files
                )
            )

            save_checkpoint(
                CHECKPOINT_FILE,
                updated_processed_files
            )

            print(
                "\nCheckpoint updated successfully."
            )

            logger.info(
                "Checkpoint updated successfully "
                "after duplicate-only batch."
            )

            return

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

        df = add_previous_incident(
            df
        )

        print(
            "2. Adding next incident timestamp..."
        )

        df = add_next_incident(
            df
        )

        print(
            "3. Adding incident row number..."
        )

        df = add_row_number(
            df
        )

        print(
            "4. Adding incident rank..."
        )

        df = add_rank(
            df
        )

        print(
            "5. Adding dense rank..."
        )

        df = add_dense_rank(
            df
        )

        print(
            "6. Calculating minutes since previous incident..."
        )

        df = add_minutes_since_previous(
            df
        )

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
        # WINDOW RESULTS
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
            "No reference dataset is currently "
            "required for the police incident pipeline."
        )

        logger.info(
            "Join module loaded successfully."
        )

        # ====================================================
        # PARTITIONING
        # ====================================================

        print("\n")
        print("=" * 60)
        print("PARTITIONING")
        print("=" * 60)

        initial_partitions = (
            get_partition_count(
                df
            )
        )

        print(
            f"\nInitial number of partitions: "
            f"{initial_partitions}"
        )

        print(
            "\nRepartitioning data..."
        )

        df = repartition_data(
            df,
            INITIAL_REPARTITION_COUNT
        )

        repartitioned_partitions = (
            get_partition_count(
                df
            )
        )

        print(
            f"Partitions after repartition: "
            f"{repartitioned_partitions}"
        )

        print(
            "\nRepartitioning by location..."
        )

        df = repartition_by_column(
            df,
            INITIAL_REPARTITION_COUNT,
            "location"
        )

        location_partitions = (
            get_partition_count(
                df
            )
        )

        print(
            f"Partitions after location repartition: "
            f"{location_partitions}"
        )

        print(
            "\nReducing partitions using coalesce..."
        )

        df = coalesce_data(
            df,
            FINAL_COALESCE_COUNT
        )

        final_partitions = (
            get_partition_count(
                df
            )
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
        # CACHE / PERSISTENCE
        # ====================================================

        print("\n")
        print("=" * 60)
        print("CACHING AND PERSISTENCE")
        print("=" * 60)

        print(
            "\nPersisting transformed DataFrame..."
        )

        df = persist_data(
            df
        )

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

        current_partitions = (
            get_optimization_partition_count(
                df
            )
        )

        print(
            f"\nPartitions: "
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
        # WRITE CURATED DATA
        # ====================================================

        print("\n")
        print("=" * 60)
        print("WRITING CURATED DATA")
        print("=" * 60)

        try:

            write_curated_data(
                df,
                CURATED_DIR,
                mode="append"
            )

        except Exception as e:

            logger.exception(
                "Failed to write curated data."
            )

            raise PipelineWriteError(
                f"Failed to write curated data: {e}"
            ) from e

        logger.info(
            "New curated data written successfully."
        )

        # ====================================================
        # RELEASE CACHE
        # ====================================================

        unpersist_data(
            df
        )

        # ====================================================
        # READ COMPLETE CURATED DATA
        # ====================================================

        print(
            "\nReading complete curated dataset "
            "for analytics..."
        )

        try:

            full_df = (
                spark.read
                .parquet(
                    str(CURATED_DIR)
                )
            )

        except Exception as e:

            logger.exception(
                "Failed to read complete curated data."
            )

            raise PipelineWriteError(
                f"Failed to read curated data: {e}"
            ) from e

        full_records = (
            full_df.count()
        )

        print(
            f"Total curated records: "
            f"{full_records}"
        )

        logger.info(
            f"Total curated records: "
            f"{full_records}"
        )

        # ====================================================
        # ANALYTICS
        # ====================================================

        print("\n")
        print("=" * 60)
        print("ANALYTICS AGGREGATIONS")
        print("=" * 60)

        print(
            "\nIncidents by Type:"
        )

        type_df = incidents_by_type(
            full_df
        )

        type_df.show(
            truncate=False
        )

        print(
            "\nIncidents by Month:"
        )

        month_df = incidents_by_month(
            full_df
        )

        month_df.show(
            truncate=False
        )

        print(
            "\nTop Locations:"
        )

        location_df = incidents_by_location(
            full_df
        )

        location_df.show(
            TOP_RESULT_ROWS,
            truncate=False
        )

        print(
            "\nIncidents by Hour:"
        )

        hour_df = incidents_by_hour(
            full_df
        )

        hour_df.show(
            truncate=False
        )

        print(
            "\nTop Subtypes:"
        )

        subtype_df = incidents_by_subtype(
            full_df
        )

        subtype_df.show(
            TOP_RESULT_ROWS,
            truncate=False
        )

        logger.info(
            "All aggregations completed successfully."
        )

        # ====================================================
        # WRITE ANALYTICS
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
            "All analytics data written successfully."
        )

        # ====================================================
        # UPDATE CHECKPOINT
        # ====================================================

        print(
            "\nUpdating incremental checkpoint..."
        )

        updated_processed_files = (
            add_processed_files(
                processed_files,
                new_files
            )
        )

        save_checkpoint(
            CHECKPOINT_FILE,
            updated_processed_files
        )

        print(
            "Checkpoint updated successfully."
        )

        print(
            f"Total processed files now: "
            f"{len(updated_processed_files)}"
        )

        logger.info(
            "Incremental checkpoint updated successfully."
        )

        # ====================================================
        # FINAL SUMMARY
        # ====================================================

        print("\n")
        print("=" * 60)
        print(
            "INCREMENTAL PYSPARK PIPELINE "
            "COMPLETED SUCCESSFULLY"
        )
        print("=" * 60)

        print(
            f"\nNew input records: "
            f"{records_before}"
        )

        print(
            f"New cleaned records: "
            f"{records_after}"
        )

        print(
            f"New records removed: "
            f"{records_before - records_after}"
        )

        print(
            f"New files processed: "
            f"{len(new_files)}"
        )

        print(
            f"Total processed files: "
            f"{len(updated_processed_files)}"
        )

        print(
            f"Total curated records: "
            f"{full_records}"
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
            "Incremental PySpark pipeline "
            "completed successfully."
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