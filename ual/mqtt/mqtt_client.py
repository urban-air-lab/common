import json
import time
import pandas as pd
import paho.mqtt.publish as publish
from ual.logging import get_logger


class MQTTClient:
    def __init__(self, server: str, port: int, username: str, password: str):
        self.server: str = server
        self.port: int = port
        self.auth: dict = {'username': username, 'password': password}

        self.logger = get_logger()

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
            self.logger.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Published to {topic}: {payload}")
        except ConnectionError as e:
            self.logger.error(f"Failed to publish to topic {topic}", exc_info=True)
