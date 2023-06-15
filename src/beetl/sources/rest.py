import polars as pl
from enum import Enum
import sqlalchemy as sqla
from typing import List
from .interface import (
    register_source,
    SourceInterface,
    ColumnDefinition,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)


class RestSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MySQL sources"""

    pass


class RestRequest:
    """Configuration representing a REST request"""

    # Request type, URL and Query Arguments
    # type: post/get/put/patch/delete
    rtype: str = None
    url: str
    qargs: dict = None

    # Headers
    headers: dict = None

    # Body types (only one should be set)
    # Replacement will occur on the insert, update, delete functions
    # according to the columns in the source
    # The syntax for this is {{colname}}
    json_body: dict = None
    post_body: dict = None
    plain_body: dict = None

    # Authentication Methods

    # Basic (str with user:pass)
    auth_basic: str = None

    # Bearer (str with prefix and token)
    auth_bearer: str = None


class RestSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for MySQL sources"""

    base_url: str
    query_data: RestRequest = None
    insert_data: RestRequest = None
    update_data: RestRequest = None
    delete_data: RestRequest = None

    connection_string: str
    query: str = None
    table: str = None

    def __init__(self, settings: dict):
        if settings.get("connection_string", False):
            self.connection_string = settings["connection_string"]
            return

        self.connection_string = "mysql+pymysql://"
        f"{settings['username']}:{settings['password']}"
        f"@{settings['host']}:{settings['port']}/{settings['database']}"


@register_source("mysql", RestSourceConfiguration, RestSourceConnectionSettings)
class RestSource(SourceInterface):
    ConnectionSettingsClass = RestSourceConnectionSettings
    SourceConfigClass = RestSourceConfiguration

    """ A source for MySQL data """

    def _configure(self):
        raise Exception("Not yet implemented")

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(
        self, params=None, customQuery: str = None, returnData: bool = True
    ) -> pl.DataFrame:
        pass

    def _insert(
        self, data: pl.DataFrame, table: str = None, connection_string: str = None
    ):
        raise NotImplementedError("Insert not implemented")

    def insert(self, data: pl.DataFrame):
        raise NotImplementedError("Insert not implemented")

    def update(self, data: pl.DataFrame):
        raise NotImplementedError("Update not implemented")

    def delete(self, data: pl.DataFrame):
        raise NotImplementedError("Delete not implemented")
