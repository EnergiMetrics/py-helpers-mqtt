"""Paho-backed, publish-only MQTT connection."""

import os
import ssl
from threading import Event
from time import monotonic
from typing import Any, Literal

import paho.mqtt.client as mqtt
from loguru import logger
from paho.mqtt import MQTTException
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode
from pydantic import BaseModel

from .exceptions import MQTTConfigurationError, MQTTConnectionError, MQTTPublishError
from .models.config import MQTTConfig


class MQTTPublisher:
    """Connect to a broker and publish messages with configured defaults."""

    def __init__(self, config: MQTTConfig) -> None:
        self._config = config
        self._client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.reconnect_delay_set(min_delay=1, max_delay=60)
        self._connected = False
        self._loop_started = False
        self._connection_started = False
        self._tls_configured = False
        self._ever_connected = False
        self._closing = False
        self._explicit_connect = False
        self._shutdown_failed = False
        self._connect_event = Event()
        self._connect_reason: ReasonCode | None = None
        logger.debug(
            "MQTT publisher configured for {}:{}",
            config.broker.host,
            config.broker.port,
        )

    @property
    def is_connected(self) -> bool:
        """Whether the broker has accepted the current connection."""
        return self._connected

    def connect(self) -> None:
        """Configure the client and wait for the broker's connection response."""
        if self._shutdown_failed:
            raise MQTTConnectionError(
                "MQTT shutdown was incomplete; create a new publisher"
            )
        if self._connected:
            return
        if self._loop_started:
            # Paho already owns reconnection on its running network loop.
            self._connect_event.clear()
            logger.debug(
                "Waiting for MQTT reconnection to {}:{}",
                self._config.broker.host,
                self._config.broker.port,
            )
            if not self._connected and not self._connect_event.wait(
                self._config.connect_timeout_seconds
            ):
                logger.error("MQTT broker reconnection timed out")
                raise MQTTConnectionError("MQTT broker reconnection timed out")
            if not self._connected:
                logger.error("MQTT broker reconnection failed")
                raise MQTTConnectionError("MQTT broker reconnection failed")
            return
        password = os.environ.get("MQTT_PASSWORD")
        if password is None:
            logger.error("MQTT_PASSWORD environment variable is missing")
            raise MQTTConfigurationError(
                "MQTT_PASSWORD environment variable is missing"
            )

        self._closing = False
        self._explicit_connect = True
        self._connect_event.clear()
        self._connect_reason = None
        deadline = monotonic() + self._config.connect_timeout_seconds
        try:
            self._client.username_pw_set(self._config.authentication.username, password)
            if self._config.broker.tls.enabled and not self._tls_configured:
                logger.debug(
                    "MQTT TLS enabled with {} CA trust",
                    "custom" if self._config.broker.tls.ca_cert else "system",
                )
                self._client.tls_set_context(  # pyright: ignore[reportUnknownMemberType] -- Paho leaves context untyped
                    ssl.create_default_context(cafile=self._config.broker.tls.ca_cert)
                )
                self._tls_configured = True
            self._client.connect_timeout = self._config.connect_timeout_seconds
            result = self._client.connect(
                self._config.broker.host, self._config.broker.port
            )
            if result != mqtt.MQTT_ERR_SUCCESS:
                raise MQTTConnectionError(f"MQTT connection failed with code {result}")
            self._connection_started = True
            loop_result = self._client.loop_start()
            if loop_result != mqtt.MQTT_ERR_SUCCESS:
                raise MQTTConnectionError(
                    f"MQTT network loop failed with code {loop_result}"
                )
            self._loop_started = True
            if not self._connect_event.wait(max(0, deadline - monotonic())):
                raise MQTTConnectionError("MQTT broker connection timed out")
            if not self._connected:
                raise MQTTConnectionError(
                    f"MQTT broker did not accept connection: {self._connect_reason}"
                )
        except (OSError, ValueError, RuntimeError, MQTTException) as exc:
            logger.error(
                "Failed to connect to MQTT broker {}:{}",
                self._config.broker.host,
                self._config.broker.port,
            )
            self.disconnect()
            raise MQTTConnectionError("Failed to connect to MQTT broker") from exc
        except MQTTConnectionError as exc:
            logger.error("{}", exc)
            self.disconnect()
            raise
        self._explicit_connect = False
        logger.info(
            "MQTT connected to {}:{}",
            self._config.broker.host,
            self._config.broker.port,
        )

    def disconnect(self) -> None:
        """Stop the network loop, including after a partial startup."""
        was_active = self._connection_started or self._loop_started or self._connected
        self._closing = True
        self._explicit_connect = False
        shutdown_failed = False
        try:
            if self._connection_started or self._loop_started:
                result = self._client.disconnect()
                if result not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
                    shutdown_failed = True
                    logger.warning("MQTT disconnect returned code {}", result)
        except (OSError, RuntimeError, MQTTException) as exc:
            shutdown_failed = True
            logger.warning("MQTT disconnect failed: {}", exc)
        finally:
            if self._loop_started:
                try:
                    result = self._client.loop_stop()
                    if result != mqtt.MQTT_ERR_SUCCESS:
                        shutdown_failed = True
                        logger.warning(
                            "MQTT network loop stop returned code {}", result
                        )
                except (OSError, RuntimeError, MQTTException) as exc:
                    shutdown_failed = True
                    logger.warning("MQTT network loop stop failed: {}", exc)
            self._loop_started = False
            self._connection_started = False
            self._connected = False
            self._shutdown_failed = self._shutdown_failed or shutdown_failed
        if shutdown_failed:
            logger.warning("MQTT shutdown incomplete; create a new publisher")
        elif was_active:
            logger.info("MQTT disconnected")

    def _on_connect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _flags: mqtt.ConnectFlags,
        reason_code: ReasonCode,
        _properties: Properties | None,
    ) -> None:
        was_connected = self._connected
        was_ever_connected = self._ever_connected
        self._connected = reason_code == 0 and not self._closing
        if self._connected:
            self._ever_connected = True
        self._connect_reason = reason_code
        self._connect_event.set()
        if (
            self._connected
            and not was_connected
            and was_ever_connected
            and not self._explicit_connect
        ):
            logger.info(
                "MQTT reconnected to {}:{}",
                self._config.broker.host,
                self._config.broker.port,
            )
        elif not self._connected and not self._closing:
            logger.warning("MQTT broker rejected connection: {}", reason_code)

    def _on_disconnect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _flags: mqtt.DisconnectFlags,
        reason_code: ReasonCode,
        _properties: Properties | None,
    ) -> None:
        self._connected = False
        self._connect_event.set()
        if not self._closing:
            logger.warning("MQTT connection lost: {}; Paho will reconnect", reason_code)

    def publish(
        self,
        topic: str,
        payload: str | bytes | BaseModel,
        *,
        qos: Literal[0, 1, 2] | None = None,
        retain: bool | None = None,
    ) -> None:
        """Publish a message and wait for completion within the configured timeout."""
        if not self._connected or self._shutdown_failed:
            logger.error("MQTT publisher is not connected")
            raise MQTTPublishError("MQTT publisher is not connected")
        effective_qos = self._config.qos if qos is None else qos
        effective_retain = self._config.retain if retain is None else retain
        logger.debug(
            "Publishing MQTT topic {} (qos={}, retain={})",
            topic,
            effective_qos,
            effective_retain,
        )
        try:
            message = (
                payload.model_dump_json() if isinstance(payload, BaseModel) else payload
            )
            info = self._client.publish(
                topic, message, qos=effective_qos, retain=effective_retain
            )
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise MQTTPublishError(
                    f"MQTT publish failed for {topic}: code {info.rc}"
                )
            info.wait_for_publish(timeout=self._config.publish_timeout_seconds)
            if not info.is_published():
                raise MQTTPublishError(
                    f"MQTT publish timed out for {topic}; delivery is uncertain"
                )
        except MQTTPublishError as exc:
            logger.error("{}", exc)
            raise
        except (OSError, ValueError, RuntimeError, MQTTException) as exc:
            logger.error("Failed to publish MQTT topic {}", topic)
            raise MQTTPublishError(f"Failed to publish MQTT topic {topic}") from exc
