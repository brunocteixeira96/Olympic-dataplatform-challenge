import sys
from datetime import datetime

def get_args(keys):
    """
    Associate command-line argument values with their corresponding keys.

    Example:
        Command:
            python -m olympic_pipeline.main data/input data/reference data 2026-06-21

        Keys:
            ["input_path", "output_path", "batch_date"]

        Result:
            {
                "input_path": "data/input",
                "reference_path": "data/reference"
                "output_path": "data",
                "batch_date": "2026-06-21",
            }
    """
    d = {}
    for i in range(len(keys)):
        d[keys[i]] = sys.argv[i + 1]
    return d

def validate_arguments(keys):
    """Validate that the expected number of arguments was provided."""

    expected_argument_count = len(keys)
    received_argument_count = len(sys.argv) - 1

    if received_argument_count != expected_argument_count:
        raise ValueError(
            "Invalid number of arguments.\n"
            f"Expected: {expected_argument_count}\n"
            f"Received: {received_argument_count}\n\n"
            "Usage:\n"
            "python -m olympic_pipeline.main "
            "<input_path> <reference_path> <output_path> <batch_date>\n\n"
            "Example:\n"
            "python -m olympic_pipeline.main data/input data/reference data 2026-06-21"
        )


def validate_input_directory(input_path):
    """Validate that the input directory and required files exist."""

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input directory does not exist: {input_path}"
        )

    if not input_path.is_dir():
        raise ValueError(
            f"Input path is not a directory: {input_path}"
        )

    required_files = [
        "athlete_events.csv",
        "noc_regions.csv",
    ]

    for filename in required_files:
        file_path = input_path / filename

        if not file_path.is_file():
            raise FileNotFoundError(
                f"Required file does not exist: {file_path}"
            )

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} | {message}")