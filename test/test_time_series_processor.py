import numpy as np
import pandas as pd
import pytest

from ual.time_series_processor import TimeSeriesProcessor


@pytest.fixture
def sample_inputs():
    dates = pd.date_range(start="2026-01-01 00:00", periods=5, freq="min")
    return pd.DataFrame(
        {"a": [1.0, 2.0, 3.0, 4.0, 5.0], "b": [10.0, 20.0, 30.0, 40.0, 50.0]},
        index=dates,
    )


@pytest.fixture
def sample_targets(sample_inputs):
    return pd.DataFrame({"target": np.arange(5, dtype=float)}, index=sample_inputs.index)


def test_init_without_targets(sample_inputs):
    processor = TimeSeriesProcessor(inputs=sample_inputs)
    assert processor.get_inputs().equals(sample_inputs)
    assert processor.get_targets() is None


def test_init_rejects_non_datetime_inputs_index():
    with pytest.raises(TypeError):
        TimeSeriesProcessor(inputs=pd.DataFrame({"a": [1, 2, 3]}))


def test_init_rejects_non_datetime_targets_index(sample_inputs):
    with pytest.raises(TypeError):
        TimeSeriesProcessor(inputs=sample_inputs, targets=pd.DataFrame({"t": range(5)}))


def test_get_dataframe_without_targets(sample_inputs):
    processor = TimeSeriesProcessor(inputs=sample_inputs)
    assert processor.get_dataframe().equals(sample_inputs)


def test_get_dataframe_with_targets(sample_inputs, sample_targets):
    processor = TimeSeriesProcessor(inputs=sample_inputs, targets=sample_targets)
    df = processor.get_dataframe()
    assert list(df.columns) == ["a", "b", "target"]
    assert len(df) == 5


def test_add_timeseries_feature_columns(sample_inputs):
    processor = TimeSeriesProcessor(inputs=sample_inputs).add_timeseries_feature("min", steps=2)
    expected = {"a", "b", "a_t-1", "a_t+1", "a_t-2", "a_t+2", "b_t-1", "b_t+1", "b_t-2", "b_t+2"}
    assert set(processor.get_inputs().columns) == expected


def test_add_timeseries_feature_values(sample_inputs):
    inputs = (
        TimeSeriesProcessor(inputs=sample_inputs)
        .add_timeseries_feature("min", steps=1)
        .get_inputs()
    )
    np.testing.assert_array_equal(inputs["a_t-1"].to_numpy(), [np.nan, 1.0, 2.0, 3.0, 4.0])
    np.testing.assert_array_equal(inputs["a_t+1"].to_numpy(), [2.0, 3.0, 4.0, 5.0, np.nan])


def test_add_timeseries_feature_keeps_gaps_as_nan(sample_inputs):
    # drop the 3rd minute: its neighbours must become NaN instead of shifting rows
    gapped = sample_inputs.drop(sample_inputs.index[2])
    inputs = TimeSeriesProcessor(inputs=gapped).add_timeseries_feature("min").get_inputs()
    np.testing.assert_array_equal(inputs["a_t-1"].to_numpy(), [np.nan, 1.0, np.nan, 4.0])
    np.testing.assert_array_equal(inputs["a_t+1"].to_numpy(), [2.0, np.nan, 5.0, np.nan])


def test_add_timeseries_feature_does_not_touch_targets(sample_inputs, sample_targets):
    processor = TimeSeriesProcessor(inputs=sample_inputs, targets=sample_targets)
    processor.add_timeseries_feature("min")
    assert list(processor.get_targets().columns) == ["target"]
