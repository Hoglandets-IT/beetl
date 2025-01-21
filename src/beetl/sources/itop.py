import os
import json
from typing import Annotated, Literal, Optional

# import asyncio
from pydantic import BaseModel, Field, model_validator
import urllib3
import polars as pl
import requests
import requests.adapters
from alive_progress import alive_bar
from .interface import (
    SourceInterfaceConfigurationArguments,
    SourceInterfaceConnectionSettingsArguments,
    register_source,
    SourceInterface,
    SourceInterfaceConfiguration,
    SourceInterfaceConnectionSettings,
    RequestThreader,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BULK_CUTOFF = 300

DATAMODELS_WITHOUT_SOFT_DELETE = (
    "NutanixCluster",
    "NutanixClusterHost",
    "NutanixVM",
    "NutanixVMDisk",
    "NutanixNetwork",
    "NutanixNetworkInterface",
)


class SoftDeleteArguments(BaseModel):
    enabled: Annotated[bool, Field(default=False)]
    field: Annotated[str, Field(min_length=1, default="status")]
    active_value: Annotated[str, Field(min_length=1, default="enabled")]
    inactive_value: Annotated[str, Field(min_length=1, default="inactive")]

    @model_validator(mode="before")
    def transform_input(cls, values):
        if not values["enabled"]:
            values["enabled"] = False
        return values


class ItopSourceConfigurationArguments(SourceInterfaceConfigurationArguments):

    datamodel: Annotated[str, Field(min_length=1)]
    oql_key: Annotated[str, Field(min_length=1)]
    soft_delete: Annotated[Optional[SoftDeleteArguments], Field(default=None)]
    link_columns: Annotated[list[str], Field(default=[])]
    comparison_columns: Annotated[list[str], Field(min_length=1)]
    unique_columns: Annotated[list[str], Field(min_length=1)]
    skip_columns: Annotated[Optional[list[str]], Field(default=[])]


class ItopSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for iTop sources"""

    datamodel: str
    oql_key: str
    soft_delete: Optional[SoftDeleteArguments]
    link_columns: list[str]
    comparison_columns: list[str]
    unique_columns: list[str]
    skip_columns: list[str]

    def __init__(self, arguments: ItopSourceConfigurationArguments):
        super().__init__(arguments)

        self.datamodel = arguments.datamodel
        self.oql_key = arguments.oql_key
        self.comparison_columns = arguments.comparison_columns
        self.unique_columns = arguments.unique_columns
        self.skip_columns = arguments.skip_columns
        self.soft_delete = arguments.soft_delete
        self.link_columns = arguments.link_columns

        # TODO: This can be made redundant by adding the model to the soft delete and only allowing the values to be models that support soft delete
        if arguments.soft_delete is not None:
            soft_delete_not_supported = arguments.soft_delete.enabled and arguments.datamodel in DATAMODELS_WITHOUT_SOFT_DELETE
            if soft_delete_not_supported:
                raise Exception(
                    f"Soft delete is not supported for {arguments.datamodel} datamodel"
                )


class ItopSourceConnectionSettingsArguments(SourceInterfaceConnectionSettingsArguments):
    class ItopConnectionArguments(BaseModel):
        host: Annotated[str, Field(min_length=1)]
        username: Annotated[str, Field(min_length=1)]
        password: Annotated[str, Field(min_length=1)]
        verify_ssl: Annotated[str, Field(default="true")]

        @model_validator(mode="before")
        def transform_input(cls, values):
            settings: dict[str, str] = values.get("settings", {})
            transformed_values: dict[str, str] = {}
            transformed_values['host'] = settings.get("host", None)
            transformed_values['username'] = settings.get("username", None)
            transformed_values['password'] = settings.get("password", None)
            transformed_values['verify_ssl'] = settings.get(
                "verify_ssl", "true")

            if type(transformed_values['verify_ssl']) is bool:
                transformed_values['verify_ssl'] = str(
                    transformed_values['verify_ssl']).lower()
            return transformed_values

    type: Literal["Itop"] = "Itop"
    connection: ItopConnectionArguments


class ItopSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for iTop sources"""

    host: str
    username: str
    password: str
    verify_ssl: bool

    def __init__(self, arguments: ItopSourceConnectionSettingsArguments):
        super().__init__(arguments)
        self.host = arguments.connection.host
        self.username = arguments.connection.username
        self.password = arguments.connection.password
        self.verify_ssl = arguments.connection.verify_ssl == 'true'


@ register_source("itop", ItopSourceConfiguration, ItopSourceConnectionSettings)
class ItopSource(SourceInterface):
    ConnectionSettingsArguments = ItopSourceConnectionSettingsArguments
    ConnectionSettingsClass = ItopSourceConnectionSettings
    SourceConfigArguments = ItopSourceConfigurationArguments
    SourceConfigClass = ItopSourceConfiguration

    source_configuration: ItopSourceConfiguration = None
    connection_settings: ItopSourceConnectionSettings = None
    auth_data: dict = None

    """ A source for Combodo iTop Data data """

    def soft_delete_active(self) -> bool:
        """Check if the soft_delete option is active

        Returns:
            bool: True/False
        """
        if self.source_configuration.soft_delete is None:
            return False

        return self.source_configuration.soft_delete.enabled

    def _configure(self):
        self.auth_data = {
            "auth_user": self.connection_settings.username,
            "auth_pwd": self.connection_settings.password,
        }

        self.url = f"https://{self.connection_settings.host}/webservices/rest.php?version=1.3&login_mode=form"

        # Test Connection
        files = self._create_body(
            "core/check_credentials",
            {
                "user": self.connection_settings.username,
                "password": self.connection_settings.password,
            },
        )

        res = requests.post(
            self.url,
            files=files,
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl,
        )

        try:
            res_json = res.json()
            assert res_json["code"] == 0
        except AssertionError:
            raise Exception(
                f"Invalid response received from iTop Server: {res_json['code']}"
            )
        except requests.exceptions.JSONDecodeError:
            raise Exception(
                "Invalid response received from iTop Server, "
                "check your authentication settings: " + res.text
            )

    def _create_body(self, operation: str, params: dict = None):
        data = params or {}

        data["operation"] = operation

        return {"json_data": json.dumps(data)}

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def _query(self, params=None) -> pl.DataFrame:
        """
        In:
            Object: The type of object to retrieve from iTop
            OQL: OQL Query
            Output Fields: List of fields to return
        """
        oql = self.source_configuration.oql_key
        if self.soft_delete_active():
            soft_delete = self.source_configuration.soft_delete
            if f"{soft_delete.field.lower()}" not in oql.lower():
                combining_word = "WHERE" if "WHERE" not in oql.upper() else "AND"
                oql += (
                    f" {combining_word} {soft_delete.field} "
                    + f"= '{soft_delete.active_value}'"
                )

        all_colums = (
            self.source_configuration.unique_columns
            + self.source_configuration.comparison_columns
            + self.source_configuration.link_columns
        )
        files = self._create_body(
            "core/get",
            {
                "class": self.source_configuration.datamodel,
                "key": oql,
                "output_fields": ",".join(all_colums),
            },
        )

        res = requests.post(
            self.url,
            files=files,
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl,
        )

        if not res.ok or res.json()["code"] != 0:
            raise Exception(
                f"Error retrieving {self.source_configuration.datamodel}"
                f"data from iTop: {res.text}"
            )

        re_obj = []

        try:
            res_json = res.json()
            objects = res_json.get("objects", None)
            if objects is None:
                return pl.DataFrame()

            for _, item in res_json.get("objects", {}).items():
                if "id" in all_colums:
                    re_obj.append({"id": item["key"], **item["fields"]})
                elif "key" in all_colums:
                    re_obj.append({"key": item["key"], **item["fields"]})
                else:
                    re_obj.append(item["fields"])

            return pl.DataFrame(re_obj)

        except Exception as e:
            raise Exception(
                "An error occured while trying to decode the response", e
            ) from e

    def create_item(self, type: str, data: dict, comment: str = ""):

        body = self._create_body(
            "core/create",
            {
                "comment": comment,
                "class": type,
                "output_fields": "*",
                "fields": {
                    x: y
                    for x, y in data.items()
                    if y is not None and x not in self.source_configuration.link_columns
                },
            },
        )

        res = requests.post(
            self.url,
            files=body,
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl,
        )

        if not res.ok:
            print("Not created: ", res.text, body)
            return False

        try:
            res_json = res.json()
            assert res_json["code"] == 0
        except Exception as e:
            print("Not Created", e, res.text, body)
            print(res.text + str(e))
            return False

        return True

    def update_item(self, type: str, data: dict, comment: str = ""):
        identifiers = {}

        for column_name in tuple(data.keys()):
            if column_name in self.source_configuration.unique_columns:
                identifiers[column_name] = data.pop(column_name)

        # if len(identifiers) == 0:
        #    dKeys = tuple(data.keys())
        #    for column_name in self.source_configuration.unique_columns:
        #        if column_name in dKeys:
        #            identifiers[column_name] = data.pop(column_name)

        where_clause_conditions = " AND ".join(
            [f"{key} = '{value}'" for key, value in identifiers.items()]
        )

        body = self._create_body(
            "core/update",
            {
                "comment": comment,
                "class": type,
                "key": f"SELECT {type} WHERE {where_clause_conditions}",
                "fields": {
                    key: value for key, value in data.items() if value is not None
                },
            },
        )

        res = requests.post(
            self.url,
            files=body,
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl,
        )

        if not res.ok:
            print("Not updated: ", res.text, body)
            return False

        try:
            res_json = res.json()
            assert res_json["code"] == 0
        except Exception as e:
            print("Not Updated", e, res.text, body)
            return False

        return True

    def delete_item(self, type: str, data: dict, comment: str = ""):
        identifiers = {}

        for key in tuple(data.keys()):
            if key in self.source_configuration.unique_columns:
                identifiers[key] = data.pop(key)

        where_clause_conditions = " AND ".join(
            [f"{key} = '{value}'" for key, value in identifiers.items()]
        )

        body = self._create_body(
            "core/delete",
            {
                "comment": comment,
                "class": type,
                "key": f"SELECT {type} WHERE {where_clause_conditions}",
            },
        )

        res = requests.post(
            self.url,
            files=body,
            data=self.auth_data,
            verify=self.connection_settings.verify_ssl,
        )

        if not res.ok:
            print("Not deleted: ", res.text, body)
            return False

        try:
            res_json = res.json()
            assert res_json["code"] == 0
        except Exception as e:
            print("Not Deleted", e, res.text, body)
            return False

        return True

    def insert(self, data: pl.DataFrame):
        insertData = data.clone()

        insert_cols = tuple(
            self.source_configuration.unique_columns
            + self.source_configuration.comparison_columns
        )

        for column in data.columns:
            if column not in insert_cols:
                insertData.drop_in_place(column)

        iters = (
            {
                "type": self.source_configuration.datamodel,
                "data": x,
                "comment": "Created via API Sync",
            }
            for x in insertData.iter_rows(named=True)
        )

        with alive_bar(len(data)) as pr_bar:
            if len(data) > BULK_CUTOFF:
                with RequestThreader(50) as needle:
                    for _ in needle.submitAndWait(self.create_item, iters):
                        pr_bar()
            else:
                for item in iters:
                    self.create_item(**item)
                    pr_bar()

        return len(data)

    def update(self, data: pl.DataFrame):
        updateData = data.clone()

        update_cols = tuple(
            column_name
            for column_name in self.source_configuration.unique_columns
            + self.source_configuration.comparison_columns
            if column_name not in (self.source_configuration.skip_columns or [])
        )

        for column in data.columns:
            if column not in update_cols:
                updateData.drop_in_place(column)

        iters = (
            {
                "type": self.source_configuration.datamodel,
                "data": x,
                "comment": "Updated via API Sync",
            }
            for x in updateData.iter_rows(named=True)
        )

        with alive_bar(len(data)) as pr_bar:
            if len(data) > BULK_CUTOFF:
                with RequestThreader(50) as needle:
                    for _ in needle.submitAndWait(self.update_item, iters):
                        pr_bar()
            else:
                for item in iters:
                    self.update_item(**item)
                    pr_bar()

        return len(data)

    def delete(self, data: pl.DataFrame):
        deleteData = data.clone()
        deleteFunc = self.delete_item
        deleteMessage = "Deleted via API Sync"

        if self.soft_delete_active():
            soft_delete = self.source_configuration.soft_delete

            deleteData = deleteData.with_columns(
                pl.Series(
                    soft_delete.field,
                    [soft_delete.inactive_value] * len(deleteData),
                )
            )
            deleteFunc = self.update_item
            deleteMessage = "Soft deletion via API Sync"

        for column_name in deleteData.columns:
            if column_name in self.source_configuration.link_columns:
                deleteData.drop_in_place(column_name)

        iters = (
            {
                "type": self.source_configuration.datamodel,
                "data": x,
                "comment": deleteMessage,
            }
            for x in deleteData.iter_rows(named=True)
        )

        with alive_bar(len(data)) as pr_bar:
            if len(data) > BULK_CUTOFF:
                with RequestThreader(100) as needle:
                    for _ in needle.submitAndWait(deleteFunc, iters):
                        pr_bar()
            else:
                for item in iters:
                    deleteFunc(**item)
                    pr_bar()

        return len(data)
