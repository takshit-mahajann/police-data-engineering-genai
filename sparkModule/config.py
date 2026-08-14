from pathlib import Path


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "ingestion" / "raw"

OUTPUT_DIR = Path(__file__).resolve().parent / "output"

CURATED_DIR = OUTPUT_DIR / "curated"

ANALYTICS_DIR = OUTPUT_DIR / "analytics"


# ============================================================
# SPARK CONFIGURATION
# ============================================================

APP_NAME = "CambridgePoliceData"

INITIAL_REPARTITION_COUNT = 8

FINAL_COALESCE_COUNT = 4


# ============================================================
# WINDOW CONFIGURATION
# ============================================================

ROLLING_WINDOW_DAYS = 7


# ============================================================
# DISPLAY CONFIGURATION
# ============================================================

WINDOW_RESULT_ROWS = 30

TOP_RESULT_ROWS = 20