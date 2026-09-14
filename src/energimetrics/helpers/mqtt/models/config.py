"""Reusable MQTT publishing configuration."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class MQTTAuthenticationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["password"]
    username: str = Field(min_length=1)


class MQTTTLSConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    enabled: bool = False
    ca_cert: str | None = None


class MQTTBrokerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    tls: MQTTTLSConfig


class MQTTConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    broker: MQTTBrokerConfig
    authentication: MQTTAuthenticationConfig
    qos: Literal[0, 1, 2] = 1
    retain: bool = False
    connect_timeout_seconds: float = Field(default=10, gt=0, allow_inf_nan=False)
    publish_timeout_seconds: float = Field(default=10, gt=0, allow_inf_nan=False)
