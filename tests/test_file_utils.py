import pytest
import pandas as pd
from utils.file_utils import detect_file_format

# Test cases for supported file extensions (and their case-insensitive variants)
SUPPORTED_EXTENSIONS_CASES = [
    # CSV
    ("test_data.csv", pd.read_csv, "to_csv", {}),
    ("test_data.CSV", pd.read_csv, "to_csv", {}),
    # TXT (treated as CSV)
    ("test_data.txt", pd.read_csv, "to_csv", {}),
    ("test_data.TXT", pd.read_csv, "to_csv", {}),
    # XLSX
    ("test_data.xlsx", pd.read_excel, "to_excel", {"engine": "openpyxl"}),
    ("test_data.XLSX", pd.read_excel, "to_excel", {"engine": "openpyxl"}),
    # XLS
    ("test_data.xls", pd.read_excel, "to_excel", {"engine": "openpyxl"}),
    ("test_data.XLS", pd.read_excel, "to_excel", {"engine": "openpyxl"}),
    # Parquet
    ("test_data.parquet", pd.read_parquet, "to_parquet", {}),
    ("test_data.PARQUET", pd.read_parquet, "to_parquet", {}),
    # PKL/Pickle
    ("test_data.pkl", pd.read_pickle, "to_pickle", {}),
    ("test_data.PKL", pd.read_pickle, "to_pickle", {}),
    ("test_data.pickle", pd.read_pickle, "to_pickle", {}),
    ("test_data.PICKLE", pd.read_pickle, "to_pickle", {}),
    # JSON
    ("test_data.json", pd.read_json, "to_json", {"orient": "records"}),
    ("test_data.JSON", pd.read_json, "to_json", {"orient": "records"}),
    # Feather
    ("test_data.feather", pd.read_feather, "to_feather", {}),
    ("test_data.FEATHER", pd.read_feather, "to_feather", {}),
]

@pytest.mark.parametrize("file_path, expected_reader, expected_writer, expected_params", SUPPORTED_EXTENSIONS_CASES)
def test_detect_supported_file_formats(file_path, expected_reader, expected_writer, expected_params):
    """
    Tests detect_file_format for various supported file extensions, including case-insensitivity.
    """
    result = detect_file_format(file_path)
    assert result['reader'] == expected_reader
    assert result['writer'] == expected_writer
    assert result['params'] == expected_params

def test_detect_unsupported_file_format():
    """
    Tests detect_file_format with an unsupported file extension,
    expecting a ValueError.
    """
    unsupported_file_path = "test_data.unsupported"
    with pytest.raises(ValueError) as excinfo:
        detect_file_format(unsupported_file_path)
    assert "不支援的檔案格式: .unsupported" in str(excinfo.value) # Check for the specific error message
    # For English message, if applicable: assert "Unsupported file format: .unsupported" in str(excinfo.value)


def test_detect_file_format_no_extension():
    """
    Tests detect_file_format with a filename that has no extension,
    expecting a ValueError.
    """
    file_path_no_ext = "test_data_no_extension"
    with pytest.raises(ValueError) as excinfo:
        detect_file_format(file_path_no_ext)
    # The error message might vary slightly based on os.path.splitext behavior with no ext
    # It usually returns an empty string for the extension.
    assert "不支援的檔案格式: " in str(excinfo.value) or "Unsupported file format: " in str(excinfo.value)

def test_detect_file_format_hidden_file_with_extension():
    """
    Tests detect_file_format with a hidden file that has a supported extension.
    e.g. .myfile.csv
    """
    file_path = ".hiddenfile.csv"
    result = detect_file_format(file_path)
    assert result['reader'] == pd.read_csv
    assert result['writer'] == "to_csv"
    assert result['params'] == {}

def test_detect_file_format_complex_path_supported_extension():
    """
    Tests detect_file_format with a more complex file path (multiple dots, directory)
    and a supported extension.
    """
    file_path = "/path/to/some.complex.filename.parquet"
    result = detect_file_format(file_path)
    assert result['reader'] == pd.read_parquet
    assert result['writer'] == "to_parquet"
    assert result['params'] == {}

# To run these tests, navigate to the project root in your terminal and run:
# python -m pytest tests/test_file_utils.py
# Ensure pytest and pandas are installed in your environment.
# (You might need to set PYTHONPATH=. or export PYTHONPATH=.)
# Example: PYTHONPATH=. pytest tests/test_file_utils.py
# or: python -m pytest
# if your tests directory is discoverable by pytest.
