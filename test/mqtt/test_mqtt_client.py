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
    mqtt_client = MQTTClient("MQTT_SERVER", 1234, "MQTT_USERNAME", "MQTT_PASSWORD")
    assert mqtt_client.auth == {'username': "MQTT_USERNAME", 'password': "MQTT_PASSWORD"}


def test_publish_single():
    data = get_sensor_data()
    with patch("ual.mqtt.mqtt_client.publish.single") as mock_single, \
         patch("ual.mqtt.mqtt_client.get_logger") as mock_get_logger:

        client = MQTTClient("MQTT_SERVER", 1234, "MQTT_USERNAME", "MQTT_PASSWORD")
        client.publish_dataframe(data=data, topic="sensors/test-data/test")

        expected_payload = json.dumps(json.loads(data.to_json(orient='records')))

        mock_single.assert_called_once_with(
            topic="sensors/test-data/test",
            payload=expected_payload,
            hostname="MQTT_SERVER",
            port=1234,
            auth={"username": "MQTT_USERNAME", "password": "MQTT_PASSWORD"},
            qos=2,
        )
        assert mock_get_logger.return_value.info.called


def test_publish_single_connection_error():
    data = get_sensor_data()
    with (patch("ual.mqtt.mqtt_client.publish.single", side_effect=ConnectionError),
         patch("ual.mqtt.mqtt_client.get_logger") as mock_get_logger,
         pytest.raises(ConnectionError)):

        client = MQTTClient("MQTT_SERVER", 1234, "MQTT_USERNAME", "MQTT_PASSWORD")
        client.publish_dataframe(data=data, topic="sensors/test-data/test")
        assert mock_get_logger.return_value.error.called
