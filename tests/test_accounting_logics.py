import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from datetime import datetime

from accounting_logics import accounting_transform

def test_accounting_transform_basic():
    """
    Tests basic transformation: 'period' and 'post_step' columns are added correctly,
    and original columns are retained.
    """
    # 1. Create a sample DataFrame
    data = {
        'date': [datetime(2023, 1, 15), datetime(2023, 2, 20), datetime(2024, 12, 5)],
        'amount': [100, 200, 300],
        'description': ['Sale A', 'Service B', 'Sale C']
    }
    input_df = pd.DataFrame(data)
    # Convert 'date' column to datetime objects if not already (though here they are)
    input_df['date'] = pd.to_datetime(input_df['date'])

    # 2. Call accounting_transform
    output_df = accounting_transform(input_df.copy()) # Use .copy() to avoid modifying input_df in place if transform does that

    # 3. Verify 'period' column
    assert 'period' in output_df.columns
    expected_periods = pd.Series(["2023-01", "2023-02", "2024-12"], name='period')
    pd.testing.assert_series_equal(output_df['period'], expected_periods, check_dtype=False)

    # 4. Verify 'post_step' column
    assert 'post_step' in output_df.columns
    assert (output_df['post_step'] == 'test').all()

    # 5. Verify original columns are retained
    assert 'date' in output_df.columns
    assert 'amount' in output_df.columns
    assert 'description' in output_df.columns
    pd.testing.assert_series_equal(input_df['amount'], output_df['amount']) # Check one original column's data

    # Check overall shape
    assert output_df.shape == (len(data['date']), len(data.keys()) + 2) # Original cols + 2 new cols

def test_accounting_transform_empty_dataframe():
    """
    Tests with an empty DataFrame (with a 'date' column defined but no rows).
    Ensures it runs without error and produces an empty DataFrame with the new columns.
    """
    # 1. Create an empty DataFrame with 'date' column
    data = {'date': pd.Series([], dtype='datetime64[ns]'), 'value': pd.Series([], dtype='int')}
    input_df = pd.DataFrame(data)

    # 2. Call accounting_transform
    output_df = accounting_transform(input_df.copy())

    # 3. Verify new columns exist and DataFrame is still empty
    assert 'period' in output_df.columns
    assert 'post_step' in output_df.columns
    assert output_df.empty
    assert len(output_df.columns) == 4 # 'date', 'value', 'period', 'post_step'

    # Check dtypes of new columns if possible (pandas might make them object for empty)
    # This is less critical for empty df, but good to be aware of
    # For example, output_df['period'].dtype might be 'object'

def test_accounting_transform_no_date_column():
    """
    Tests with a DataFrame that lacks the 'date' column.
    Verifies that an AttributeError is raised (from .dt accessor).
    """
    # 1. Create a DataFrame without 'date' column
    data = {'amount': [100, 200], 'description': ['Item X', 'Item Y']}
    input_df = pd.DataFrame(data)

    # 2. Call accounting_transform and expect an exception
    with pytest.raises(AttributeError) as excinfo:
        # The specific error comes from trying to access .dt on a non-datetime-like Series,
        # which happens if 'date' column is missing and pandas creates it as something else,
        # or if the column is there but not datetime.
        # If 'date' is strictly missing, it would be a KeyError first.
        # Let's check how `accounting_transform` handles `df['date']` when it's not there.
        # `df['date'].dt.strftime` will raise AttributeError if `df['date']` is not a datetime series.
        # If `df['date']` doesn't exist, it's a KeyError.
        # The current implementation `df['period'] = df['date'].dt.strftime('%Y-%m')`
        # will raise KeyError if 'date' column is missing.
        # If 'date' column exists but is not datetime, it will be AttributeError.
        # Let's assume the most likely failure: missing 'date' column leads to KeyError.
        # If the test reveals AttributeError due to how pandas handles missing keys in certain contexts,
        # we can adjust.
        # After reviewing `accounting_logics.py`, it directly accesses `df['date']`.
        # This will cause a KeyError if 'date' is not in `input_df.columns`.
        accounting_transform(input_df.copy())

    # As per the code `df['period'] = df['date'].dt.strftime('%Y-%m')`,
    # if 'date' column is missing, `df['date']` will raise a KeyError.
    # The subsequent `.dt` access would then not happen.
    assert isinstance(excinfo.value, KeyError)
    assert "'date'" in str(excinfo.value) # Check that the error message mentions the 'date' key

# To run these tests:
# PYTHONPATH=. pytest tests/test_accounting_logics.py
# or simply `pytest` from the project root.
