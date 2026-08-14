import logging
from pathlib import Path


# ============================================================
# LOG DIRECTORY
# ============================================================

LOG_DIR = Path(__file__).resolve().parent / "logs"

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# LOGGER
# ============================================================

def get_logger(
    name: str = "CambridgePolicePipeline"
):

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    # --------------------------------------------------------
    # FILE HANDLER
    # --------------------------------------------------------

    file_handler = logging.FileHandler(
        LOG_DIR / "pipeline.log",
        encoding="utf-8"
    )

    file_handler.setLevel(
        logging.INFO
    )

    file_handler.setFormatter(
        formatter
    )

    # --------------------------------------------------------
    # CONSOLE HANDLER
    # --------------------------------------------------------

    console_handler = logging.StreamHandler()

    console_handler.setLevel(
        logging.INFO
    )

    console_handler.setFormatter(
        formatter
    )

    logger.addHandler(
        file_handler
    )

    logger.addHandler(
        console_handler
    )

    return logger