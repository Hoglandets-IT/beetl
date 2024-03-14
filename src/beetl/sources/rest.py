import polars as pl
import pandas as pd
from enum import Enum
import sqlalchemy as sqla
from typing import Dict, List
from .interface import (
    register_source,
    SourceInterface,
    ColumnDefinition,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
)
import requests

class RestResponse:
    length: str
    items: str
    
    def __init__(self, length: str, items: str):
        self.length = length
        self.items = items

class RestRequest:
    path: str = None
    method: str = "GET"
    body_type: str = "application/json"
    body = None
    return_type: str = "application/json"
    response: RestResponse = None
    
    def __init__(self, path: str, method: str = "GET", body_type: str = "application/json", body = None, return_type: str = "application/json", response: dict = None):
        self.path = path
        self.method = method
        self.body_type = body_type
        self.body = body
        self.return_type = return_type
        self.response = response
        
class RestSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MySQL sources"""
    request: RestRequest
    
    def __init__(self, columns: list, request: dict):
        super().__init__(columns)
        self.request = RestRequest(**request)

class RestAuthentication:
    basic: bool = False
    basic_user: str = None
    basic_pass: str = None
    bearer: bool = False
    bearer_prefix: str = "Bearer"
    bearer_token: str = None
    
    def __init__(self, basic: bool = False, basic_user: str = None, basic_pass: str = None, bearer: bool = False, bearer_prefix: str = "Bearer", bearer_token: str = None):
        self.basic = basic
        self.basic_user = basic_user
        self.basic_pass = basic_pass
        self.bearer = bearer
        self.bearer_prefix = bearer_prefix
        self.bearer_token = bearer_token

class RestSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for MySQL sources"""

    base_url: str
    authentication: RestAuthentication = None
    ignore_certificates: bool = False
    
    client = None
    
    def __init__(self, settings: dict):
        if settings.get("base_url", None) is not None:
            self.base_url = settings["base_url"]
        
        if settings.get("authentication", None) is not None:
            self.authentication = RestAuthentication(**settings["authentication"])
        
        if settings.get("ignore_certificates", None) is not None:
            self.ignore_certificates = settings["ignore_certificates"]


@register_source("rest", RestSourceConfiguration, RestSourceConnectionSettings)
class RestSource(SourceInterface):
    ConnectionSettingsClass = RestSourceConnectionSettings
    SourceConfigClass = RestSourceConfiguration
    source_configuration: RestSourceConfiguration
    connection_settings: RestSourceConnectionSettings
    client = None
    """ A source for MySQL data """

    def _configure(self):
        if self.client is None:
            self.client = requests.Session()
            if self.connection_settings.authentication.basic:
                self.client.auth = (self.connection_settings.authentication.basic_user, self.connection_settings.authentication.basic_pass)
            
            if self.connection_settings.authentication.bearer:
                self.client.headers.update({"Authorization": f"{self.connection_settings.authentication.bearer_prefix} {self.connection_settings.authentication.bearer_token}"})

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(
        self, params=None, customQuery: str = None, returnData: bool = True
    ) -> pl.DataFrame:
        request = {
            "method": self.source_configuration.request.method,
            "url": self.connection_settings.base_url + self.source_configuration.request.path,
            "headers": {"Content-Type": self.source_configuration.request.body_type},
            "verify": (not self.connection_settings.ignore_certificates)
        }
        
        if self.source_configuration.request.body_type == "application/json" and self.source_configuration.request.body is not None:
            request["json"] = self.source_configuration.request.body
        
        else:
            request["data"] = self.source_configuration.request.body
        
        
        response = self.client.request(**request)
        
        if response.status_code != 200:
            raise Exception("Failed request")
        
        if self.source_configuration.request.return_type == "application/json":
            response_data = response.json()
        else:
            raise Exception("Not implemented")
        
        if len(self.source_configuration.request.response["items"]) > 0:
            for key in self.source_configuration.request.response["items"].split("."):
                response_data = response_data[key]

        if len(response_data) == 0:
            return pl.DataFrame()

        pd_df = pd.json_normalize(response_data)
        return pl.from_pandas(pd_df)

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
