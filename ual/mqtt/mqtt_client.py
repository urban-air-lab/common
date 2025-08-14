import json
import os
import time
import logging
import pandas as pd
import paho.mqtt.publish as publish


class MQTTClient:
    def __init__(self):
        self.server = os.getenv("MQTT_SERVER")
        self.port = int(os.getenv("MQTT_PORT", 1883))
        self.auth = {'username': os.getenv("MQTT_USERNAME"), 'password': os.getenv("MQTT_PASSWORD")}

    def publish_dataframe(self, data: pd.DataFrame, topic: str, qos: int = 2) -> None:
        try:
            json_str = data.to_json(orient='records')
            payload = json.loads(json_str)
            payload = json.dumps(payload)
            publish.single(
                topic=topic,
                payload=payload,
                hostname=self.server,
                port=self.port,
                auth=self.auth,
                qos=qos
            )
            logging.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Published to {topic}: {payload}")
        except Exception as e:
            logging.error(f"Failed to publish to topic {topic}: {e}")
