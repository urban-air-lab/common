import pandas as pd
from unittest.mock import MagicMock, patch

import pytest

from ual.influx.Influx_db_connector import InfluxDBConnector


def test_init():
    InfluxDBConnector("http://x", "token", "org")


def test_query_dataframe():
    df = pd.DataFrame({
        "result": ["_result"], "host": ["h"], "topic": ["t"], "table": [0],
        "_start": [pd.Timestamp("2025-01-01")], "_stop": [pd.Timestamp("2025-01-02")],
        "_measurement": ["m"], "_time": [pd.Timestamp("2025-08-30T00:00:00Z")],
        "_field": ["value"], "_value": [1.2],
    })

    query_api_mock = MagicMock()
    query_api_mock.query_data_frame.return_value = df

    with patch("ual.influx.Influx_db_connector.InfluxDBClient.query_api",
               return_value=query_api_mock) as mock_query_api:
        connector = InfluxDBConnector("http://x", "token", "org")
        out = connector.query_dataframe('from(bucket:"x") |> range(start:-1h)')

    assert out.index.name == "_time"
    for col in ["result", "host", "topic", "table", "_start", "_stop", "_measurement", "_time"]:
        assert col not in out.columns
    assert "_value" in out.columns
    mock_query_api.assert_called_once()


def test_query_empty_dataframe():
    df = pd.DataFrame()
    query_api_mock = MagicMock()
    query_api_mock.query_data_frame.return_value = df

    with patch("ual.influx.Influx_db_connector.InfluxDBClient.query_api",
               return_value=query_api_mock) as mock_query_api:
        connector = InfluxDBConnector("http://x", "token", "org")
        out = connector.query_dataframe('from(bucket:"x") |> range(start:-1h)')

    assert out.empty


def test_query_dataframe_connection_error():
    df = pd.DataFrame()
    query_api_mock = MagicMock()
    query_api_mock.query_data_frame.return_value = df

    with patch("ual.influx.Influx_db_connector.InfluxDBClient.query_api",
               return_value=query_api_mock,
               side_effect=ConnectionError):
        with pytest.raises(ConnectionError):
            connector = InfluxDBConnector("http://x", "token", "org")
            connector.query_dataframe('from(bucket:"x") |> range(start:-1h)')
