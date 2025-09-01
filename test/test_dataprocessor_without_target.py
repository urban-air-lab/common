import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from ual.data_processor import DataProcessor


@pytest.fixture
def sample_inputs():
    dates = pd.date_range(start=datetime.now(), periods=10, freq="min")
    return pd.DataFrame({
        "RAW_ADC_NO_W": np.random.rand(10),
        "RAW_ADC_NO_A": np.random.rand(10),
        "RAW_ADC_NO2_W": np.random.rand(10),
        "RAW_ADC_NO2_A": np.random.rand(10),
        "RAW_ADC_O3_W": np.random.rand(10),
        "RAW_ADC_O3_A": np.random.rand(10),
    }, index=dates)


def test_processor_initialization_without_targets(sample_inputs):
    processor = DataProcessor(inputs=sample_inputs)
    assert isinstance(processor.get_inputs(), pd.DataFrame)


def test_to_hourly_without_targets(sample_inputs):
    processor = DataProcessor(inputs=sample_inputs)
    processed = processor.to_hourly()
    assert isinstance(processed.get_inputs(), pd.DataFrame)


def test_remove_nan_without_targets(sample_inputs):
    sample_inputs.iloc[0, 0] = np.nan
    processor = DataProcessor(inputs=sample_inputs)
    processed = processor.remove_nan()
    assert not processed.get_inputs().isnull().values.any()


def test_calculate_w_a_difference(sample_inputs):
    gases = ["NO", "NO2", "O3"]
    processor = DataProcessor(inputs=sample_inputs)
    processed = processor.calculate_w_a_difference(gases)
    for gas in gases:
        assert f"{gas}_W_A" in processed.get_inputs().columns


def test_get_target_raises_error(sample_inputs):
    processor = DataProcessor(inputs=sample_inputs)
    with pytest.raises(ValueError, match="No targets were provided"):
        processor.get_targets()
