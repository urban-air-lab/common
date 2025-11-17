import json
from typing import Dict, Any
import paho.mqtt.client as mqtt
from ual.logging import get_logger


class MQTTClient:
    def __init__(self, server: str, port: int, username: str, password: str, tls: bool = True):
        self.server: str = server
        self.port: int = port
        self.auth: dict = {'username': username, 'password': password}

        self.mqtt_connected = False
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.packet_counter = 0

        self.logger = get_logger()

        try:
            self.logger.info(f'Authenticating with user: {self.auth["username"]} on MQTT connection')
            self.client.username_pw_set(self.auth["username"], self.auth["password"])
        except AttributeError:
            self.logger.error("Using no authentication on MQTT connection")

        if tls:
            self.logger.info("using TLS for MQTT Connection")
            self.client.tls_set()

        try:
            self.client.connect(self.server, self.port)
        except Exception as e:
            self.logger.error(f"Can't connect to MQTT Broker:{self.server} at port:{self.port}, dump: {e}")

        self.client.loop_start()  # Start MQTT handling in a new thread

    def _get_next_packet_count(self) -> int:
        self.packet_counter += 1
        return self.packet_counter

    def get_connected(self) -> bool:
        return self.mqtt_connected

    def _on_connect(self, _client, _userdata, _flags, _rc) -> None:
        self.logger.info(f'Connected to MQTT Broker:, {self.server} at port: {self.port}')
        self.mqtt_connected = True

    def _on_disconnect(self, _client, _userdata, _rc) -> None:
        print(f'Disconnected from MQTT Broker: {self.server} at port: {self.port}')
        self.mqtt_connected = False

    def publish_data(self, data: Dict[str, Any], topic: str ) -> None:
        data["packet_count"] = self._get_next_packet_count()
        json_data = json.dumps(data, indent=4)
        try:
            self.client.publish(topic, json_data, qos=2)
            self.logger.info(f'mqtt publish: {data}')
        except Exception as e:
            self.logger.error("could not push to mqtt: ", e)

    def stop(self) -> None:
        self.client.loop_stop()
