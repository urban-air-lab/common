import json
from unittest.mock import patch
import pandas as pd
import random
import time
import pytest
from dotenv import load_dotenv

from ual.mqtt.mqtt_client import MQTTClient

load_dotenv()


def get_sensor_data():
    return pd.DataFrame(data={
        "temperature": [random.randint(15, 25), random.randint(15, 25), random.randint(15, 25)],
        "humidity": [random.randint(30, 60), random.randint(30, 60), random.randint(30, 60)],
        "timestamp": [time.time(), time.time(), time.time()]
    }, index=[0, 1, 3])


def test_init():
    mqtt_client = MQTTClient("MQTT_SERVER", 8887, "MQTT_USERNAME", "MQTT_PASSWORD")
    assert mqtt_client.auth == {'username': "MQTT_USERNAME", 'password': "MQTT_PASSWORD"}


def test_publish_single():
    data = get_sensor_data()
    with patch("ual.mqtt.mqtt_client.MQTTClient.publish_data") as mock_publish_data, \
            patch("ual.mqtt.mqtt_client.get_logger") as mock_get_logger:
        client = MQTTClient("MQTT_SERVER", 8887, "MQTT_USERNAME", "MQTT_PASSWORD")
        client.publish_data(data=data.to_dict(), topic="sensors/test-data/test")

        mock_publish_data.assert_called_once_with(
            data=data.to_dict(),
            topic="sensors/test-data/test",
        )
        assert mock_get_logger.return_value.info.called


def test_publish_single_connection_error():
    data = get_sensor_data()
    with (patch("ual.mqtt.mqtt_client.MQTTClient.publish_data", side_effect=ConnectionError),
          patch("ual.mqtt.mqtt_client.get_logger") as mock_get_logger,
          pytest.raises(ConnectionError)):
        client = MQTTClient("MQTT_SERVER", 8887, "MQTT_USERNAME", "MQTT_PASSWORD")
        client.publish_data(data=data.to_dict(), topic="sensors/test-data/test")
        assert mock_get_logger.return_value.error.called
