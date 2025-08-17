import os
import pandas as pd
import random
import time
import pytest

from ual.mqtt.mqtt_client import MQTTClient

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"


def get_sensor_data():
    return pd.DataFrame(data={
        "temperature": [random.randint(15, 25), random.randint(15, 25), random.randint(15, 25)],
        "humidity": [random.randint(30, 60), random.randint(30, 60), random.randint(30, 60)],
        "timestamp": [time.time(), time.time(), time.time()]
    }, index=[0, 1, 3])


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Test doesn't work in Github Actions.")
def test_publish_single():
    data = get_sensor_data()
    mqtt_client = MQTTClient()
    mqtt_client.publish_dataframe(data=data, topic="sensors/test-data/test")
