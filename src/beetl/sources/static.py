from typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict, model_validator
from polars import DataFrame
from .interface import (
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class StaticSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for static sources"""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


class StaticSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for static sources"""

    # Arbitrary types are allowed since dataframe is not a pydantic type
    model_config = ConfigDict(
        arbitrary_types_allowed=True)

    # Data source is saved as field in order to automatically validate against the schema
    static: List[dict[str, Any]]
    data: DataFrame

    @model_validator(mode='before')
    def customize_fields(cls, values):
        static = values.get('static', [])
        values['data'] = DataFrame(static)
        return values


@ register_source("static", StaticSourceConfiguration, StaticSourceConnectionSettings)
class StaticSource(SourceInterface):
    ConnectionSettingsClass = StaticSourceConnectionSettings
    SourceConfigClass = StaticSourceConfiguration

    """ A source for static data """

    def _configure(self):
        pass

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> DataFrame:
        return self.connection_settings.data

    def insert(self, data: DataFrame):
        print("Inserting data into static source...")
        print(data)
        return len(data)

    def update(self, data: DataFrame):
        print("Updating data in static source...")
        print(data)
        return len(data)

    def delete(self, data: DataFrame):
        print("Deleting data from static source")
        print(data)
        return len(data)
