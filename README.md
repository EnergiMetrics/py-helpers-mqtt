# Energimetrics MQTT Helper

A reusable publish-only MQTT helper for Energimetrics Python applications.

## Install

```bash
uv add "energimetrics.helpers.mqtt @ git+https://github.com/EnergiMetrics/py-helpers-mqtt.git"
```

The package requires Python 3.14 or newer.

## Configure and publish

Set `MQTT_PASSWORD` in the application's process environment before connecting. The password is never part of `MQTTConfig` or a YAML configuration file.

```python
from pydantic import BaseModel

from energimetrics.helpers.mqtt import (
    MQTTAuthenticationConfig,
    MQTTBrokerConfig,
    MQTTConfig,
    MQTTError,
    MQTTPublisher,
    MQTTTLSConfig,
)


class ExamplePayload(BaseModel):
    value: int


config = MQTTConfig(
    broker=MQTTBrokerConfig(
        host="broker.example.com",
        port=8883,
        tls=MQTTTLSConfig(enabled=True),
    ),
    authentication=MQTTAuthenticationConfig(
        type="password",
        username="app",
    ),
)

publisher = MQTTPublisher(config)

try:
    publisher.connect()
    publisher.publish(
        topic="example/state",
        payload=ExamplePayload(value=1),
    )
    publisher.publish(
        topic="example/ready",
        payload="online",
        qos=0,
        retain=True,
    )
except MQTTError as exc:
    print(exc)
finally:
    publisher.disconnect()
```

`MQTTPublisher` also accepts `bytes` payloads. Pydantic models are serialized with `model_dump_json()`; strings and bytes are sent directly. Each publish uses the configured QoS and retain values unless overridden. The defaults are QoS 1 and `retain=False`. The application supplies complete topic names and owns its payload models. Configuration models are immutable; create a new config and publisher to change broker or TLS settings.

## Connection and failure behavior

TLS uses the operating system's CA trust store by default. Set `MQTTTLSConfig(ca_cert="/path/to/ca.pem", enabled=True)` to use a specific CA file. Certificate and hostname verification remain enabled.

`connect()` waits for broker acceptance. `is_connected` reflects Paho connection callbacks. Paho retries interrupted connections with backoff from 1 to 60 seconds; publishing while disconnected fails immediately. `disconnect()` is safe during cleanup after a failed connection attempt. If Paho cannot complete shutdown, create a new publisher before connecting again.

`MQTTConfigurationError`, `MQTTConnectionError`, and `MQTTPublishError` all inherit from `MQTTError`. Catch `MQTTError` to handle any expected helper failure, or catch a specific subclass when the application needs different recovery behavior.

`connect_timeout_seconds` (default 10) configures Paho's socket connection timeout and the broker-response wait; DNS resolution may take longer. `publish_timeout_seconds` (default 10) bounds the publication wait. **A timed-out publish may still be delivered later.** Retry only when duplicates are acceptable or the application can deduplicate messages.

This package publishes only. It does not subscribe, receive messages, build topics, or define application-specific payloads.
