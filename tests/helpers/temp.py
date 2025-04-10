import os
from pathlib import Path

TEMP_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", ".temp"
)


def ensure_temp_directory():
    """
    Ensure that the temporary directory exists.
    """
    if not os.path.exists(TEMP_PATH):
        os.makedirs(TEMP_PATH)


def create_temp_file(file_name: str) -> str:
    """
    Creates a temporary file in the temporary directory.

    Args:
        file_name (str): The name of the file to create.

    Returns:
        str: The path to the created file.
    """
    ensure_temp_directory()
    Path(os.path.join(TEMP_PATH, file_name)).touch()


def clean_temp_directory():
    """
    Clean the temporary directory.
    """
    if os.path.exists(TEMP_PATH):
        for file in os.listdir(TEMP_PATH):
            file_path = os.path.join(TEMP_PATH, file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    os.rmdir(file_path)
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")
