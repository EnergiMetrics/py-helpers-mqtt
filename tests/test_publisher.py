"""Broker-free tests of the publishing contract."""

import socket
import ssl
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from paho.mqtt import client as mqtt
from pydantic import BaseModel, ValidationError

from energimetrics.helpers.mqtt import (
    MQTTBrokerConfig,
    MQTTConfig,
    MQTTConfigurationError,
    MQTTConnectionError,
    MQTTPublisher,
    MQTTPublishError,
    MQTTTLSConfig,
)


class Payload(BaseModel):
    value: int


@pytest.fixture
def config() -> MQTTConfig:
    return MQTTConfig.model_validate(
        {
            "broker": {
                "host": "broker.example.com",
                "port": 8883,
                "tls": {},
            },
            "authentication": {"type": "password", "username": "user"},
        }
    )


@pytest.fixture
def client() -> Iterator[MagicMock]:
    with patch("energimetrics.helpers.mqtt.publisher.mqtt.Client") as factory:
        mock = factory.return_value
        mock.connect.return_value = mqtt.MQTT_ERR_SUCCESS
        mock.loop_start.return_value = mqtt.MQTT_ERR_SUCCESS
        mock.disconnect.return_value = mqtt.MQTT_ERR_SUCCESS
        mock.loop_stop.return_value = mqtt.MQTT_ERR_SUCCESS
        mock.publish.return_value.rc = mqtt.MQTT_ERR_SUCCESS
        mock.publish.return_value.is_published.return_value = True
        yield mock


def accepted(client: MagicMock) -> None:
    client.on_connect(client, None, MagicMock(), 0, None)


def test_config_validation(config: MQTTConfig) -> None:
    for qos in (0, 1, 2):
        assert MQTTConfig.model_validate({**config.model_dump(), "qos": qos}).qos == qos
    for field, value in (
        ("qos", 3),
        ("authentication", {"type": "token", "username": "u"}),
        ("broker", {**config.broker.model_dump(), "tls": {"enabled": "invalid"}}),
        ("connect_timeout_seconds", float("inf")),
        ("publish_timeout_seconds", float("inf")),
    ):
        with pytest.raises(ValidationError):
            MQTTConfig.model_validate({**config.model_dump(), field: value})


def test_broker_structure_and_defaults(config: MQTTConfig) -> None:
    assert isinstance(config.broker, MQTTBrokerConfig)
    assert isinstance(config.broker.tls, MQTTTLSConfig)
    assert config.broker.tls.enabled is False
    assert config.qos == 1
    assert config.retain is False


def test_missing_password(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MQTT_PASSWORD", raising=False)
    with pytest.raises(MQTTConfigurationError, match="MQTT_PASSWORD") as error:
        MQTTPublisher(config).connect()
    assert "secret" not in str(error.value)
    client.connect.assert_not_called()


@pytest.mark.parametrize("custom_ca", [False, True])
def test_connect_tls(
    config: MQTTConfig,
    client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    custom_ca: bool,
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    ca_cert = (
        str(Path(__file__).parent / "fixtures" / "test_ca.pem") if custom_ca else None
    )
    tls_config = config.model_copy(
        update={
            "broker": config.broker.model_copy(
                update={
                    "tls": config.broker.tls.model_copy(
                        update={"enabled": True, "ca_cert": ca_cert}
                    )
                }
            )
        }
    )
    publisher = MQTTPublisher(tls_config)
    client.loop_start.side_effect = lambda: (
        accepted(client),
        mqtt.MQTT_ERR_SUCCESS,
    )[1]
    with patch(
        "energimetrics.helpers.mqtt.publisher.ssl.create_default_context",
        wraps=ssl.create_default_context,
    ) as create_context:
        publisher.connect()
    create_context.assert_called_once_with(cafile=ca_cert)
    client.username_pw_set.assert_called_once_with("user", "secret")
    client.connect.assert_called_once_with("broker.example.com", 8883)
    context = client.tls_set_context.call_args.args[0]
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    if custom_ca:
        assert len(context.get_ca_certs()) == 1
    client.tls_insecure_set.assert_not_called()
    client.reconnect_delay_set.assert_called_once_with(min_delay=1, max_delay=60)
    assert client.connect_timeout == config.connect_timeout_seconds
    assert publisher.is_connected
    publisher.disconnect()
    client.disconnect.assert_called_once()
    client.loop_stop.assert_called_once()
    assert not publisher.is_connected


def test_connection_failure(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    client.connect.return_value = mqtt.MQTT_ERR_NO_CONN
    with pytest.raises(MQTTConnectionError):
        MQTTPublisher(config).connect()
    client.loop_start.assert_not_called()
    client.tls_set_context.assert_not_called()


@pytest.mark.parametrize("failure_type", [ConnectionRefusedError, TimeoutError])
def test_initial_socket_failure_can_retry_with_real_client(
    config: MQTTConfig,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[OSError],
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    config = config.model_copy(update={"connect_timeout_seconds": 2.5})
    publisher = MQTTPublisher(config)
    failures = [failure_type("broker unavailable") for _ in range(2)]

    # Only replace opening the TCP connection. Paho's state transitions,
    # CONNECT/CONNACK processing, network loop and shutdown remain real.
    client_socket, broker_socket = socket.socketpair()
    with (
        client_socket,
        broker_socket,
        patch(
            "paho.mqtt.client.socket.create_connection",
            side_effect=[*failures, client_socket],
        ) as create_connection,
    ):
        try:
            for attempt, failure in enumerate(failures, start=1):
                with pytest.raises(
                    MQTTConnectionError, match=r"^Failed to connect to MQTT broker$"
                ) as error:
                    publisher.connect()
                assert error.value.__cause__ is failure
                assert create_connection.call_count == attempt
                assert not publisher.is_connected

            # A successful MQTT 3.1.1 CONNACK from the simulated broker.
            broker_socket.sendall(b"\x20\x02\x00\x00")
            publisher.connect()
            assert publisher.is_connected
            assert create_connection.call_count == 3
            for call in create_connection.call_args_list:
                assert call.args == ((config.broker.host, config.broker.port),)
                assert call.kwargs["timeout"] == config.connect_timeout_seconds
        finally:
            publisher.disconnect()
        assert not publisher.is_connected


def test_disconnect_callback(config: MQTTConfig, client: MagicMock) -> None:
    publisher = MQTTPublisher(config)
    accepted(client)
    assert publisher.is_connected
    client.on_disconnect(client, None, MagicMock(), 1, None)
    assert not publisher.is_connected


@pytest.mark.parametrize(
    "payload,expected",
    [("text", "text"), (b"bytes", b"bytes"), (Payload(value=1), '{"value":1}')],
)
def test_publish_payloads(
    config: MQTTConfig,
    client: MagicMock,
    payload: str | bytes | BaseModel,
    expected: str | bytes,
) -> None:
    publisher = MQTTPublisher(config)
    accepted(client)
    publisher.publish("unchanged/topic", payload)
    client.publish.assert_called_once_with(
        "unchanged/topic", expected, qos=1, retain=False
    )
    client.publish.return_value.wait_for_publish.assert_called_once_with(timeout=10)


def test_publish_overrides(config: MQTTConfig, client: MagicMock) -> None:
    publisher = MQTTPublisher(config)
    accepted(client)
    publisher.publish("topic", "value", qos=0, retain=True)
    client.publish.assert_called_once_with("topic", "value", qos=0, retain=True)


def test_publish_failures(config: MQTTConfig, client: MagicMock) -> None:
    publisher = MQTTPublisher(config)
    with pytest.raises(MQTTPublishError, match="not connected"):
        publisher.publish("topic", "value")
    accepted(client)
    client.publish.return_value.rc = mqtt.MQTT_ERR_NO_CONN
    with pytest.raises(MQTTPublishError):
        publisher.publish("topic", "value")
    client.publish.return_value.rc = mqtt.MQTT_ERR_SUCCESS
    client.publish.return_value.is_published.return_value = False
    with pytest.raises(MQTTPublishError, match="delivery is uncertain"):
        publisher.publish("topic", "value")


def test_broker_rejection(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    publisher = MQTTPublisher(config)

    def reject() -> int:
        client.on_connect(client, None, MagicMock(), 5, None)
        return mqtt.MQTT_ERR_SUCCESS

    client.loop_start.side_effect = reject
    with pytest.raises(MQTTConnectionError, match="did not accept"):
        publisher.connect()
    client.loop_stop.assert_called_once()
    assert not publisher.is_connected


def test_reconnect_updates_state(config: MQTTConfig, client: MagicMock) -> None:
    publisher = MQTTPublisher(config)
    accepted(client)
    client.on_disconnect(client, None, MagicMock(), 1, None)
    assert not publisher.is_connected
    accepted(client)
    assert publisher.is_connected


def test_publish_exception(config: MQTTConfig, client: MagicMock) -> None:
    publisher = MQTTPublisher(config)
    accepted(client)
    client.publish.side_effect = ValueError("invalid topic")
    with pytest.raises(MQTTPublishError, match="Failed to publish"):
        publisher.publish("topic", "value")


def test_connect_waits_for_existing_network_loop(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    with patch("energimetrics.helpers.mqtt.publisher.Event") as event_factory:
        publisher = MQTTPublisher(config)
        client.loop_start.side_effect = lambda: (
            accepted(client),
            mqtt.MQTT_ERR_SUCCESS,
        )[1]
        publisher.connect()
        client.on_disconnect(client, None, MagicMock(), 1, None)
        assert not publisher.is_connected

        def reconnect_during_wait(_timeout: float) -> bool:
            accepted(client)
            return True

        event_factory.return_value.wait.side_effect = reconnect_during_wait
        publisher.connect()
        assert event_factory.return_value.wait.call_count == 2
    assert publisher.is_connected
    client.connect.assert_called_once()
    client.loop_start.assert_called_once()
    client.loop_stop.assert_not_called()


def test_connect_sets_socket_timeout_before_blocking_call(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")

    def timeout(_host: str, _port: int) -> int:
        assert client.connect_timeout == config.connect_timeout_seconds
        raise TimeoutError("socket timed out")

    client.connect.side_effect = timeout
    with pytest.raises(MQTTConnectionError, match="Failed to connect"):
        MQTTPublisher(config).connect()
    client.loop_start.assert_not_called()


def test_tls_publisher_can_connect_again_after_disconnect(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    tls_config = config.model_copy(
        update={
            "broker": config.broker.model_copy(
                update={"tls": config.broker.tls.model_copy(update={"enabled": True})}
            )
        }
    )
    publisher = MQTTPublisher(tls_config)
    client.loop_start.side_effect = lambda: (
        accepted(client),
        mqtt.MQTT_ERR_SUCCESS,
    )[1]
    publisher.connect()
    publisher.disconnect()
    publisher.connect()
    assert publisher.is_connected
    assert client.connect.call_count == 2
    client.tls_set_context.assert_called_once()
    assert client.loop_start.call_count == 2


def test_loop_start_failure_closes_connected_socket(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    client.loop_start.return_value = mqtt.MQTT_ERR_INVAL
    with pytest.raises(MQTTConnectionError, match="network loop failed"):
        MQTTPublisher(config).connect()
    client.disconnect.assert_called_once()
    client.loop_stop.assert_not_called()


def test_unserializable_payload_raises_mqtt_publish_error(
    config: MQTTConfig, client: MagicMock
) -> None:
    class UnserializablePayload(BaseModel):
        value: object

    publisher = MQTTPublisher(config)
    accepted(client)
    with pytest.raises(MQTTPublishError, match="Failed to publish"):
        publisher.publish("topic", UnserializablePayload(value=object()))
    client.publish.assert_not_called()


def test_config_rejects_extra_fields_without_showing_secret(config: MQTTConfig) -> None:
    raw = config.model_dump()
    raw["authentication"]["password"] = "yaml-secret"
    with pytest.raises(ValidationError) as error:
        MQTTConfig.model_validate(raw)
    assert "yaml-secret" not in str(error.value)
    assert "password" in str(error.value)


def test_initial_broker_response_timeout(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    with patch("energimetrics.helpers.mqtt.publisher.Event") as event_factory:
        event_factory.return_value.wait.return_value = False
        with pytest.raises(MQTTConnectionError, match="connection timed out"):
            MQTTPublisher(config).connect()
    client.disconnect.assert_called_once()
    client.loop_stop.assert_called_once()


def test_reconnect_timeout_and_rejection(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    with patch("energimetrics.helpers.mqtt.publisher.Event") as event_factory:
        publisher = MQTTPublisher(config)
        client.loop_start.side_effect = lambda: (
            accepted(client),
            mqtt.MQTT_ERR_SUCCESS,
        )[1]
        publisher.connect()
        client.on_disconnect(client, None, MagicMock(), 1, None)
        event_factory.return_value.wait.return_value = False
        with pytest.raises(MQTTConnectionError, match="reconnection timed out"):
            publisher.connect()
        assert client.loop_stop.call_count == 0
        event_factory.return_value.wait.return_value = True
        with pytest.raises(MQTTConnectionError, match="reconnection failed"):
            publisher.connect()


def test_shutdown_errors_do_not_escape(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    publisher = MQTTPublisher(config)
    client.loop_start.side_effect = lambda: (
        accepted(client),
        mqtt.MQTT_ERR_SUCCESS,
    )[1]
    publisher.connect()
    client.disconnect.side_effect = OSError("disconnect failed")
    client.loop_stop.side_effect = OSError("loop stop failed")
    publisher.disconnect()
    assert not publisher.is_connected
    client.loop_stop.assert_called_once()
    with pytest.raises(MQTTConnectionError, match="create a new publisher"):
        publisher.connect()


def test_initial_connection_is_not_logged_as_reconnection(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    with patch("energimetrics.helpers.mqtt.publisher.logger.info") as log_info:
        publisher = MQTTPublisher(config)
        client.loop_start.side_effect = lambda: (
            accepted(client),
            mqtt.MQTT_ERR_SUCCESS,
        )[1]
        publisher.connect()
        assert not any("reconnected" in str(call) for call in log_info.call_args_list)
        client.on_disconnect(client, None, MagicMock(), 1, None)
        accepted(client)
        assert any("reconnected" in str(call) for call in log_info.call_args_list)


def test_idle_disconnect_does_not_claim_broker_disconnection(
    config: MQTTConfig, client: MagicMock
) -> None:
    with patch("energimetrics.helpers.mqtt.publisher.logger.info") as log_info:
        MQTTPublisher(config).disconnect()
    log_info.assert_not_called()
    client.disconnect.assert_not_called()


def test_config_is_immutable(config: MQTTConfig) -> None:
    tls_field = "ca_cert"
    broker_field = "host"
    with pytest.raises(ValidationError):
        setattr(config.broker.tls, tls_field, "/different/ca.pem")
    with pytest.raises(ValidationError):
        setattr(config.broker, broker_field, "other-broker")
    assert config.broker.host == "broker.example.com"


def test_shutdown_return_codes_prevent_reuse(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    publisher = MQTTPublisher(config)
    client.loop_start.side_effect = lambda: (
        accepted(client),
        mqtt.MQTT_ERR_SUCCESS,
    )[1]
    publisher.connect()
    client.loop_stop.return_value = mqtt.MQTT_ERR_INVAL
    publisher.disconnect()
    with pytest.raises(MQTTConnectionError, match="create a new publisher"):
        publisher.connect()
    client.connect.assert_called_once()


def test_explicit_second_connect_has_one_connection_log(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    with patch("energimetrics.helpers.mqtt.publisher.logger.info") as log_info:
        publisher = MQTTPublisher(config)
        client.loop_start.side_effect = lambda: (
            accepted(client),
            mqtt.MQTT_ERR_SUCCESS,
        )[1]
        publisher.connect()
        publisher.disconnect()
        log_info.reset_mock()
        publisher.connect()
        messages = [str(call) for call in log_info.call_args_list]
        assert len(messages) == 1
        assert "MQTT connected" in messages[0]
        assert "reconnected" not in messages[0]


def test_disconnect_error_code_prevents_reuse(
    config: MQTTConfig, client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MQTT_PASSWORD", "secret")
    publisher = MQTTPublisher(config)
    client.loop_start.side_effect = lambda: (
        accepted(client),
        mqtt.MQTT_ERR_SUCCESS,
    )[1]
    publisher.connect()
    client.disconnect.return_value = mqtt.MQTT_ERR_INVAL
    publisher.disconnect()
    accepted(client)  # A late callback must not make this instance usable again.
    assert not publisher.is_connected
    with pytest.raises(MQTTConnectionError, match="create a new publisher"):
        publisher.connect()
