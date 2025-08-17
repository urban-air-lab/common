import json
import os
import time
import logging
import pandas as pd
import paho.mqtt.publish as publish


class MQTTClient:
    def __init__(self, server: str, port: int, username: str, password: str):
        self.server: str = server
        self.port: int = port
        self.auth: dict = {'username': username, 'password': password}

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
