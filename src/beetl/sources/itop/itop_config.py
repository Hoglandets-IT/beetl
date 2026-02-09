from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..interface import SourceConfig, SourceConfigArguments


class ItopConfigArguments(SourceConfigArguments):
    class ItopConnectionArguments(BaseModel):
        model_config = ConfigDict(extra="forbid")

        host: Annotated[str, Field(min_length=1)]
        username: Annotated[str, Field(min_length=1)]
        password: Annotated[str, Field(min_length=1)]
        verify_ssl: Annotated[str, Field(default="true")]
        skip_credentials_verification: Annotated[
            str,
            Field(
                default="false",
                description="Enable this if you run an itop version where the check_credendials api call has not been adapted to the new php dynamic properties deprication. The fix is not present in the latest itop version 3.2.2. The request will fail completely otherwise.",
            ),
        ]

        @model_validator(mode="before")
        def transform_input(cls, values):
            settings: dict[str, str] = values.get("settings", {})
            transformed_values: dict[str, str] = {}
            transformed_values["host"] = settings.get("host", None)
            transformed_values["username"] = settings.get("username", None)
            transformed_values["password"] = settings.get("password", None)
            transformed_values["verify_ssl"] = settings.get("verify_ssl", "true")
            transformed_values["skip_credentials_verification"] = settings.get(
                "skip_credentials_verification", "false"
            )

            if type(transformed_values["verify_ssl"]) is bool:
                transformed_values["verify_ssl"] = str(
                    transformed_values["verify_ssl"]
                ).lower()

            if type(transformed_values["skip_credentials_verification"]) is bool:
                transformed_values["skip_credentials_verification"] = str(
                    transformed_values["skip_credentials_verification"]
                ).lower()
            return transformed_values

    type: Annotated[Literal["Itop"], Field(default="Itop")] = "Itop"
    connection: ItopConnectionArguments


class ItopConfig(SourceConfig):
    """The connection configuration class used for iTop sources"""

    host: str
    username: str
    password: str
    verify_ssl: bool
    skip_credentials_verification: bool

    def __init__(self, arguments: ItopConfigArguments):
        super().__init__(arguments)
        self.host = arguments.connection.host
        self.username = arguments.connection.username
        self.password = arguments.connection.password
        self.verify_ssl = arguments.connection.verify_ssl == "true"
        self.skip_credentials_verification = (
            arguments.connection.skip_credentials_verification == "true"
        )
