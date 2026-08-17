import json
from pathlib import Path


# ============================================================
# CHECKPOINT MANAGEMENT
# ============================================================

def load_checkpoint(checkpoint_path):
    """
    Load the list of files that have already been processed.

    If the checkpoint does not exist, this is treated as the
    first pipeline run.
    """

    checkpoint_path = Path(checkpoint_path)

    if not checkpoint_path.exists():
        return set()

    try:

        with open(
            checkpoint_path,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        processed_files = data.get(
            "processed_files",
            []
        )

        return set(processed_files)

    except (json.JSONDecodeError, OSError) as error:

        raise RuntimeError(
            f"Unable to read checkpoint file: {error}"
        ) from error


# ============================================================
# SAVE CHECKPOINT
# ============================================================

def save_checkpoint(
    checkpoint_path,
    processed_files
):
    """
    Save successfully processed files to the checkpoint.
    """

    checkpoint_path = Path(checkpoint_path)

    checkpoint_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    data = {
        "processed_files": sorted(
            list(processed_files)
        )
    }

    try:

        with open(
            checkpoint_path,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                data,
                file,
                indent=4
            )

    except OSError as error:

        raise RuntimeError(
            f"Unable to save checkpoint: {error}"
        ) from error


# ============================================================
# FIND ALL RAW FILES
# ============================================================

def find_raw_files(raw_dir):
    """
    Find all JSON files inside the raw data directory.
    """

    raw_dir = Path(raw_dir)

    if not raw_dir.exists():
        raise FileNotFoundError(
            f"Raw data directory does not exist: {raw_dir}"
        )

    return sorted(
        [
            file
            for file in raw_dir.rglob("*.json")
            if file.is_file()
        ]
    )


# ============================================================
# FIND NEW FILES
# ============================================================

def get_new_files(
    raw_dir,
    processed_files
):
    """
    Return only JSON files that have not been processed yet.
    """

    all_files = find_raw_files(raw_dir)

    new_files = [
        file
        for file in all_files
        if str(file.resolve()) not in processed_files
    ]

    return new_files


# ============================================================
# UPDATE CHECKPOINT
# ============================================================

def add_processed_files(
    processed_files,
    files
):
    """
    Add successfully processed files to the checkpoint set.
    """

    updated_files = set(processed_files)

    for file in files:

        updated_files.add(
            str(
                Path(file).resolve()
            )
        )

    return updated_files