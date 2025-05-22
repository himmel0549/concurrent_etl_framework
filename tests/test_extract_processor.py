import pytest
import pandas as pd
import time # Import time for mocking
from unittest.mock import MagicMock, patch

from processors.extract import ExtractProcessor, _thread_local
from core.context import ETLContext
from core.stats import ETLStats # Required to instantiate ETLContext properly

@pytest.fixture
def mock_etl_context(mocker):
    """Fixture to create a mock ETLContext with a mock ETLStats."""
    mock_stats = mocker.MagicMock(spec=ETLStats)
    context = ETLContext(stats=mock_stats)
    return context

def test_extract_processor_successful_file_read_string_path(mocker, mock_etl_context):
    """
    Tests ExtractProcessor.process with a string file_path for a successful read.
    """
    mock_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    file_path = "dummy/path/to/test_file.csv"

    # 1. Mock detect_file_format
    mock_detect_format = mocker.patch('processors.extract.detect_file_format')
    mock_reader_func = mocker.MagicMock(return_value=mock_df)
    mock_detect_format.return_value = {
        'reader': mock_reader_func,
        'params': {}
    }

    # 2. Mock pandas reader function (already done via mock_reader_func, but this ensures it's the one called)
    # No need to patch 'pandas.read_csv' directly if detect_file_format's result is controlled.

    # 3. Mock time.sleep
    mock_sleep = mocker.patch('time.sleep')

    processor = ExtractProcessor(context=mock_etl_context, processing_factor=0.01)

    # Action
    result_df = processor.process(file_path)

    # Assertions
    mock_detect_format.assert_called_once_with(file_path)
    mock_reader_func.assert_called_once_with(file_path) # No additional params from default mock
    
    mock_sleep.assert_called_once() # Called due to processing_factor > 0
    
    mock_etl_context.stats.file_processed.assert_called_once_with(file_path, len(mock_df))
    mock_etl_context.stats.record_error.assert_not_called()
    
    pd.testing.assert_frame_equal(result_df, mock_df)
    
    # Verify _thread_local handling (basic check: was it set and then cleared?)
    assert not hasattr(_thread_local, 'current_file'), "_thread_local.current_file was not cleared"

def test_extract_processor_successful_file_read_dict_path(mocker, mock_etl_context):
    """
    Tests ExtractProcessor.process with a dictionary file_info for a successful read.
    """
    mock_df = pd.DataFrame({'data': [5, 6]})
    file_dict = {'path': "another/file.xlsx", 'other_info': 'test'} # file_info dict

    mock_detect_format = mocker.patch('processors.extract.detect_file_format')
    mock_reader_func = mocker.MagicMock(return_value=mock_df)
    mock_detect_format.return_value = {
        'reader': mock_reader_func,
        'params': {'engine': 'openpyxl'} # Example params
    }
    mocker.patch('time.sleep')

    processor = ExtractProcessor(context=mock_etl_context)
    result_df = processor.process(file_dict, custom_arg='value') # Pass extra kwargs

    mock_detect_format.assert_called_once_with(file_dict['path'])
    # Reader is called with merged params: its own default params + kwargs from process()
    mock_reader_func.assert_called_once_with(file_dict['path'], engine='openpyxl', custom_arg='value')
    
    mock_etl_context.stats.file_processed.assert_called_once_with(file_dict['path'], len(mock_df))
    pd.testing.assert_frame_equal(result_df, mock_df)
    assert not hasattr(_thread_local, 'current_file'), "_thread_local.current_file was not cleared"

def test_extract_processor_file_read_exception(mocker, mock_etl_context):
    """
    Tests ExtractProcessor.process when the pandas reader function raises an exception.
    """
    file_path = "dummy/path/to/non_existent_file.csv"
    test_exception = FileNotFoundError("File not found for testing")

    mock_detect_format = mocker.patch('processors.extract.detect_file_format')
    mock_reader_func = mocker.MagicMock(side_effect=test_exception) # Mocked reader raises exception
    mock_detect_format.return_value = {
        'reader': mock_reader_func,
        'params': {}
    }
    mock_sleep = mocker.patch('time.sleep') # Mock sleep, though it might not be called if exception is early

    processor = ExtractProcessor(context=mock_etl_context)

    # Action and Assertion for exception
    with pytest.raises(FileNotFoundError, match="File not found for testing"):
        processor.process(file_path)

    # Further Assertions
    mock_detect_format.assert_called_once_with(file_path)
    mock_reader_func.assert_called_once_with(file_path) # Attempted to call
    
    # time.sleep might or might not be called depending on where the mocked reader fails.
    # If it fails before sleep, sleep is not called. If after, it is.
    # For this test, the mock_reader_func (e.g. pd.read_csv) fails, so sleep is not called.
    mock_sleep.assert_not_called()
        
    mock_etl_context.stats.file_processed.assert_not_called()
    mock_etl_context.stats.record_error.assert_called_once_with("FileNotFoundError")
    
    # Verify _thread_local handling (basic check: was it set and then cleared?)
    assert not hasattr(_thread_local, 'current_file'), "_thread_local.current_file was not cleared after exception"

def test_extract_processor_generic_exception_during_processing(mocker, mock_etl_context):
    """
    Tests ExtractProcessor.process when a generic exception occurs after file read
    but before stats update (e.g., during simulated processing time calculation if it were complex).
    This specific scenario is a bit hard to trigger with current code structure as sleep is simple.
    Let's simulate it by making the len(df) call fail.
    """
    mock_df = MagicMock(spec=pd.DataFrame) # Use MagicMock to mock __len__
    mock_df.__len__.side_effect = RuntimeError("Simulated error getting df length") # Error during stats prep
    
    file_path = "dummy/path/to/test_file.parquet"

    mock_detect_format = mocker.patch('processors.extract.detect_file_format')
    mock_reader_func = mocker.MagicMock(return_value=mock_df)
    mock_detect_format.return_value = {
        'reader': mock_reader_func,
        'params': {}
    }
    # We don't mock time.sleep here to let the processing_factor logic run,
    # but the error occurs before sleep is called.

    processor = ExtractProcessor(context=mock_etl_context, processing_factor=0.01)

    with pytest.raises(RuntimeError, match="Simulated error getting df length"):
        processor.process(file_path)

    mock_etl_context.stats.file_processed.assert_not_called()
    mock_etl_context.stats.record_error.assert_called_once_with("RuntimeError")
    assert not hasattr(_thread_local, 'current_file'), "_thread_local.current_file was not cleared"


@patch('processors.extract._thread_local', MagicMock()) # Patch _thread_local for direct assertion
def test_thread_local_management_explicit(mocker, mock_etl_context):
    """
    More explicit test for _thread_local management.
    This requires _thread_local to be patchable or inspectable.
    """
    # For this test, we assume _thread_local itself can be mocked or we are checking its attributes
    # The @patch decorator re-assigns _thread_local in the module for the duration of this test.
    
    mock_df = pd.DataFrame({'col1': [1]})
    file_path = "test.csv"

    mocker.patch('processors.extract.detect_file_format', return_value={
        'reader': lambda x, **kw: mock_df, 'params': {}
    })
    mocker.patch('time.sleep')
    
    # Access the patched _thread_local
    from processors.extract import _thread_local as patched_tl

    # Ensure it's clean before
    assert not hasattr(patched_tl, 'current_file')

    processor = ExtractProcessor(context=mock_etl_context)
    
    # This part is tricky because current_file is set and deleted within the same `process` call.
    # We can't easily assert its state *during* the call without modifying the source
    # or using more complex techniques like context managers within the source.
    # However, we can verify it's cleared by the end of the call.

    # We can use a side effect on a mocked function called during processing to check _thread_local
    def check_thread_local_during_read(*args, **kwargs):
        assert hasattr(patched_tl, 'current_file')
        assert patched_tl.current_file == file_path
        return mock_df

    mocker.patch('processors.extract.detect_file_format', return_value={
        'reader': check_thread_local_during_read, 'params': {}
    })
    
    processor.process(file_path)
    
    # Assert it's cleared after
    assert not hasattr(patched_tl, 'current_file')


# To run: pytest tests/test_extract_processor.py
# (Ensure PYTHONPATH is set if running from root, e.g., PYTHONPATH=. pytest ...)
# Or simply `pytest` if tests directory is configured.
# Make sure `tests/__init__.py` (if needed) and `utils/__init__.py`, `core/__init__.py` exist.
# The project structure implies these should be packages.
# If `processors` is a top-level dir, adjust imports or PYTHONPATH.
# Assuming `processors` is directly importable or part of a larger package.
# Based on previous `ls()` output, the project root seems to be the main package.
# So, imports like `from processors.extract import ExtractProcessor` should work if pytest runs from root.
# Or if the project is installed in editable mode (`pip install -e .`)
# The `PYTHONPATH=. pytest` command is a good way to ensure imports work from the root.
