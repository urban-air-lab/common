import json
from typing import Any

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

        self.logger = get_logger("mqtt_client")

        try:
            self.logger.info(f'Authenticating with user: {self.auth["username"]} on MQTT connection')
            self.client.username_pw_set(self.auth["username"], self.auth["password"])
        except AttributeError:
            self.logger.error("Using no authentication on MQTT connection")

        if tls:
            self.logger.info("using TLS for MQTT Connection")
            self.client.tls_set()

        try:
            self.client.connect(self.server, self.port, keepalive=60)
            self.client.reconnect_delay_set(min_delay=1, max_delay=60)
        except (OSError, ValueError) as e:
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

    def publish_data(self, data: dict[str, Any], topic: str ) -> None:
        data["packet_count"] = self._get_next_packet_count()
        json_data = json.dumps(data, indent=4)
        try:
            info = self.client.publish(topic, json_data, qos=2)
        except (ValueError, TypeError) as e:
            self.logger.error(f"could not push to mqtt: topic: {topic}, dump: {e}")
            return

        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            self.logger.error(f"could not push to mqtt: topic: {topic}, rc: {mqtt.error_string(info.rc)}")
            return

        self.logger.info(f'mqtt publish: topic: {topic}, data: {data}')

    def stop(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()
