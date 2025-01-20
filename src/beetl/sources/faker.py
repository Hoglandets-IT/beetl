from typing import Annotated, Any, Optional
from polars import DataFrame as POLARS_DF
from pydantic import ConfigDict, Field, model_validator
from .interface import (
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
    register_source,
)


class FakerSourceConfiguration(SourceInterfaceConfiguration):
    def __init__(self, **extra):
        super().__init__(**extra)


class FakerSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for faker sources"""

    # Arbitrary types are allow since dataframe is not a pydantic type
    model_config = ConfigDict(arbitrary_types_allowed=True)

    faker: Annotated[list[dict[str, Any]], Field(min_length=1)]
    data: POLARS_DF

    @model_validator(mode='before')
    def customize_fields(cls, values):
        faker = values.get('faker', [])
        values['data'] = POLARS_DF(faker)
        return values


@register_source("faker", FakerSourceConfiguration, FakerSourceConnectionSettings)
class FakerSource(SourceInterface):
    ConnectionSettingsClass = FakerSourceConnectionSettings
    SourceConfigClass = FakerSourceConfiguration

    """ A source for faker data """

    def _configure(self):
        raise Exception("Not yet implemented")

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> POLARS_DF:
        return self.connection_settings.data

    def insert(self, data: POLARS_DF):
        print("Inserting data into static source...")
        print(data)
        return len(data)

    def update(self, data: POLARS_DF):
        print("Updating data in static source...")
        print(data)
        return len(data)

    def delete(self, data: POLARS_DF):
        print("Deleting data from static source")
        print(data)
        return len(data)
