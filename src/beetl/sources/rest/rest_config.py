from typing import Annotated, Literal, Optional, Union

from pydantic import ConfigDict, Field, model_validator

from ...errors import ConfigValidationError, ConfigValueError
from ...sources.interface import (
    SourceConfig,
    SourceConfigArguments,
    SourceConnectionArguments,
)
from ...validation import ValidationBaseModel


class Oauth2Arguments(ValidationBaseModel):
    model_config = ConfigDict(extra="forbid")

    flow: Annotated[str, Field(default="")]
    token_url: Annotated[str, Field(default="")]
    authorization_url: Annotated[str, Field(default="")]
    token_body: Annotated[dict, Field(default={})]
    token_path: Annotated[str, Field(default="access_token")]
    valid_to_path: Annotated[str, Field(default="expires_on")]


class RestAuthenticationArguments(ValidationBaseModel):
    model_config = ConfigDict(extra="forbid")

    basic: Annotated[Optional[bool], Field(default=False)]
    basic_user: Annotated[Optional[str], Field(default=None)]
    basic_pass: Annotated[Optional[str], Field(default=None)]
    bearer: Annotated[Optional[bool], Field(default=False)]
    bearer_prefix: Annotated[str, Field(default="Bearer")]
    bearer_token: Annotated[Optional[str], Field(default=None)]
    oauth2: Annotated[Optional[bool], Field(default=False)]
    oauth2_settings: Annotated[Optional[Oauth2Arguments], Field(default=None)]

    @model_validator(mode="before")
    def propagate_nested_location(cls, values: dict):
        cls.propagate_location("oauth2_settings", values)
        return values

    @model_validator(mode="after")
    def validate_authentication(cls, instance: "RestAuthenticationArguments"):
        if not any([instance.basic, instance.bearer, instance.oauth2]):
            raise ValueError("At least one authentication method must be set")
        if sum([instance.basic, instance.bearer, instance.oauth2]) > 1:
            raise ValueError("Only one authentication method can be set")
        return instance


class RestSettingsArguments(ValidationBaseModel):
    model_config = ConfigDict(extra="forbid")
    base_url: Annotated[str, Field(min_length=1)]
    ignore_certificates: Annotated[bool, Field(default=False)]
    authentication: Annotated[
        Optional[RestAuthenticationArguments], Field(default=None)
    ]

    # TODO: These should be moved to the sync config as they are dependant on wether the source is a destination or not
    soft_delete: Annotated[bool, Field(default=False)]
    deleted_field: Annotated[str, Field(default="deleted")]
    deleted_value: Annotated[Union[None, str, int], Field(default=None)]

    @model_validator(mode="before")
    def propagate_nested_location(cls, values: dict):
        cls.propagate_location("authentication", values)
        return values

    @model_validator(mode="after")
    def validate_soft_delete(
        cls,
        instance: "RestConfigArguments.ConnectionArguments.SettingsArguments",
    ):
        if not instance.soft_delete:
            return instance

        errors = []
        required_fields = ["deleted_field", "deleted_value"]
        for field in required_fields:
            if not getattr(instance, field):
                errors.append(
                    ConfigValueError(
                        field,
                        f"Field '{field}' is required when using 'soft_delete' is set to True",
                        instance.location,
                    )
                )
        if errors:
            raise ConfigValidationError(errors)

        return instance


class ConnectionArguments(SourceConnectionArguments):
    settings: RestSettingsArguments

    @model_validator(mode="before")
    def propagate_nested_location(cls, values: dict):
        cls.propagate_location("settings", values)
        return values


class RestConfigArguments(SourceConfigArguments):
    connection: ConnectionArguments
    type: Annotated[Literal["Rest"], Field(default="Rest")] = "Rest"


class RestConfig(SourceConfig):
    """The connection configuration class used for MySQL sources"""

    base_url: str
    ignore_certificates: bool
    authentication: Optional[RestAuthenticationArguments]
    soft_delete: bool
    deleted_field: str
    deleted_value: Union[None, str, int]

    def __init__(self, arguments: RestConfigArguments):
        super().__init__(arguments)

        settings = arguments.connection.settings

        self.base_url = settings.base_url
        self.authentication = settings.authentication or None
        self.ignore_certificates = settings.ignore_certificates
        self.soft_delete = settings.soft_delete
        self.deleted_field = settings.deleted_field
        self.deleted_value = settings.deleted_value
