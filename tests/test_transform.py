import pytest
import pandas as pd
import io
import csv
from dags.transform import transform_data

@pytest.fixture
def sample_watch_data():
    return [
        {
            'name': 'Watch 1',
            'price': '₹1,23,456',
            'Case Size': '42mm',
            'Limited Edition': 'Yes',
            'Frequency': '28800 bph',
            'Lug Width': '20mm',
            'Power Reserve': '40 hours',
            'Case Thickness': '12.5mm',
            'Interchangeable Strap': 'Yes'
        },
        {
            'name': 'Watch 2',
            'price': '₹45,678',
            'Case Size': '38mm',
            'Limited Edition': 'No',
            'Frequency': '21600 bph',
            'Lug Width': '18mm',
            'Power Reserve': '72 hours',
            'Case Thickness': '11mm',
            'Interchangeable Strap': 'No'
        }
    ]

def test_transform_data_output_is_csv(sample_watch_data):

    result = transform_data(sample_watch_data)
    
    assert isinstance(result, str), "Output should be a string"
    
    try:
        df = pd.read_csv(io.StringIO(result))
        assert isinstance(df, pd.DataFrame)
    except Exception as e:
        pytest.fail(f"Failed to parse CSV: {e}")
