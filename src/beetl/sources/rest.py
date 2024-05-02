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

class BodyOptions:
    accepts_multiple: bool = False
    object_path: str = ""
    
    def __init__(self, accepts_multiple: bool = False, object_path: str = ""):
        self.accepts_multiple = accepts_multiple
        self.object_path = object_path

class RestRequest:
    path: str = None
    method: str = "GET"
    body_type: str = "application/json"
    body = None
    body_options: BodyOptions = None
    return_type: str = "application/json"
    response: RestResponse = None
    
    def __init__(self, path: str = None, method: str = "GET", body_type: str = "application/json", body = None, body_options: dict = None, return_type: str = "application/json", response: dict = None):
        self.path = path
        self.method = method
        self.body_type = body_type
        self.body = body
        self.return_type = return_type
        if body_options is not None:
            self.body_options = BodyOptions(**body_options)
        self.response = response
        
class RestSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MySQL sources"""
    listRequest: RestRequest
    createRequest: RestRequest
    updateRequest: RestRequest
    deleteRequest: RestRequest
    
    def __init__(self, columns: list, listRequest: dict = None, createRequest: dict = None, updateRequest: dict = None, deleteRequest: dict = None):
        super().__init__(columns)
        self.listRequest = RestRequest(**listRequest)
        self.createRequest = RestRequest(**createRequest if createRequest is not None else {})
        self.updateRequest = RestRequest(**updateRequest if updateRequest is not None else {})
        self.deleteRequest = RestRequest(**deleteRequest if deleteRequest is not None else {})

class RestAuthentication:
    basic: bool = False
    basic_user: str = None
    basic_pass: str = None
    bearer: bool = False
    bearer_prefix: str = "Bearer"
    bearer_token: str = None

    
    def __init__(self, basic: bool = False, basic_user: str = None, basic_pass: str = None, bearer: bool = False, bearer_prefix: str = "Bearer", bearer_token: str = None, soft_delete: bool = False, delete_field: str = "deleted", deleted_value = None):
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
    soft_delete: bool = False
    deleted_field: str = "deleted"
    deleted_value = None
    client = None
    
    def __init__(self, settings: dict):
        if settings.get("base_url", None) is not None:
            self.base_url = settings["base_url"]
        
        if settings.get("authentication", None) is not None:
            self.authentication = RestAuthentication(**settings["authentication"])
        
        if settings.get("ignore_certificates", None) is not None:
            self.ignore_certificates = settings["ignore_certificates"]
        
        self.soft_delete = settings.get("soft_delete", False)
        self.deleted_field = settings.get("deleted_field", "deleted")
        self.deleted_value = settings.get("deleted_value", None)


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
            "method": self.source_configuration.listRequest.method,
            "url": self.connection_settings.base_url + self.source_configuration.listRequest.path,
            "headers": {"Content-Type": self.source_configuration.listRequest.body_type},
            "verify": (not self.connection_settings.ignore_certificates)
        }

        if self.source_configuration.listRequest.return_type != "":
            request["headers"]["Accept"] = self.source_configuration.listRequest.return_type
        
        if self.source_configuration.listRequest.body_type == "application/json" and self.source_configuration.listRequest.body is not None:
            request["json"] = self.source_configuration.listRequest.body
        
        else:
            request["data"] = self.source_configuration.listRequest.body
        
        
        response = self.client.request(**request)
        
        if response.status_code != 200:
            raise Exception("Failed request")
        
        if self.source_configuration.listRequest.return_type == "application/json":
            response_data = response.json()
        else:
            raise Exception("Not implemented")
        
        if len(self.source_configuration.listRequest.response["items"]) > 0:
            for key in self.source_configuration.listRequest.response["items"].split("."):
                response_data = response_data[key]

        if len(response_data) == 0:
            return pl.DataFrame()

        pd_df = pd.json_normalize(response_data)
        return pl.from_pandas(pd_df)

    def _insert(
        self, data: pl.DataFrame, 
    ):
        request = {
            "method": self.source_configuration.createRequest.method,
            "url": self.connection_settings.base_url + self.source_configuration.createRequest.path,
            "headers": {"Content-Type": self.source_configuration.createRequest.body_type},
            "verify": (not self.connection_settings.ignore_certificates)
        }
        
        body_template = {}
        body_path = []
        
        if self.source_configuration.createRequest.body_options.object_path != "":
            body_template = self.source_configuration.createRequest.body
            body_path = self.source_configuration.createRequest.body_options.object_path.split(".")
        
        if self.source_configuration.createRequest.body_options.accepts_multiple:
            body = dict(body_template)
            allItems = []
            for item in data.iter_rows(named=True):
                allItems.append(item)
            
            if len(body_path) == 1:
                body[body_path[0]] = allItems
            elif len(body_path) == 2:
                body[body_path[0]][body_path[1]] = allItems
            elif len(body_path) == 3:
                body[body_path[0]][body_path[1]][body_path[2]] = allItems
            elif len(body_path) == 4:
                body[body_path[0]][body_path[1]][body_path[2]][body_path[3]] = allItems
            else:
                body = allItems

            request["json"] = body
            
            response = self.client.request(**request)
            if response.status_code > 299:
                raise Exception("Insertion failed: ", response.text)
        
        else:
            for item in data.iter_rows(named=True):
                body = dict(body_template)
                
                if len(body_path) == 1:
                    if body_path[0] == "0":
                        body = [item]
                    else:
                        body[body_path[0]] = item
                elif len(body_path) == 2:
                    if body_path[1] == "0":
                        body[body_path[0]] = [item]
                    else:
                        body[body_path[0]][body_path[1]] = item
                elif len(body_path) == 3:
                    if body_path[2] == "0":
                        body[body_path[0]][body_path[1]] = [item]
                    else:
                        body[body_path[0]][body_path[1]][body_path[2]] = item
                elif len(body_path) == 4:
                    if body_path[3] == "0":
                        body[body_path[0]][body_path[1]][body_path[2]] = [item]
                    else:
                        body[body_path[0]][body_path[1]][body_path[2]][body_path[3]] = item
                else:
                    body = item

                if self.source_configuration.createRequest.body_type == "application/json":
                    request["json"] = body
                else:
                    request["data"] = body
                
                response = self.client.request(**request)
                if response.status_code > 299:
                    print("Request failed: ", response.text)
        

    def insert(self, data: pl.DataFrame):
        return self._insert(data)
        raise NotImplementedError("Insert not implemented")

    def update(self, data: pl.DataFrame):
        raise NotImplementedError("Update not implemented")

    def delete(self, data: pl.DataFrame):
        if self.connection_settings.soft_delete != True and self.source_configuration.deleteRequest.path == "":
            print("Delete not configured, skipping")
            return 0
        return 0