"""Errors raised by the MQTT publisher."""


class MQTTError(Exception):
    """Base error for expected MQTT helper failures."""


class MQTTConfigurationError(MQTTError):
    """MQTT configuration or credentials are invalid."""


class MQTTConnectionError(MQTTError):
    """A broker connection could not be established."""


class MQTTPublishError(MQTTError):
    """A message could not be published."""
