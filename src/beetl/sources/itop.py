import os
import json

# import asyncio
import urllib3
import polars as pl
import requests
import requests.adapters
from alive_progress import alive_bar
from .interface import (
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


class ItopSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for iTop sources"""

    datamodel: str = None
    oql_key: str = None
    soft_delete: dict = None
    link_columns: list = None
    comparison_columns: list = None
    unique_columns: list = None
    skip_columns: list = None

    def __init__(
        self,
        datamodel: str = None,
        oql_key: str = None,
        soft_delete: dict = None,
        comparison_columns: list = None,
        unique_columns: list = None,
        link_columns: list = [],
        skip_columns: list = None,
    ):
        super().__init__()
        self.datamodel = datamodel
        self.oql_key = oql_key

        if comparison_columns is None:
            raise Exception("Comparison columns must be provided")
        self.comparison_columns = comparison_columns

        if unique_columns is None:
            raise Exception("Unique columns must be provided")
        self.unique_columns = unique_columns

        self.skip_columns = skip_columns

        if soft_delete is not None:
            if (
                soft_delete.get("enabled", False)
                and datamodel in DATAMODELS_WITHOUT_SOFT_DELETE
            ):
                raise Exception(
                    f"Soft delete is not supported for {datamodel} datamodel"
                )

        self.soft_delete = soft_delete

        self.link_columns = link_columns


class ItopSourceConnectionSettings(SourceInterfaceConnectionSettings):
    """The connection configuration class used for iTop sources"""

    host: str
    username: str = None
    password: str = None
    verify_ssl: bool = True

    def __init__(self, settings: dict):
        self.host = settings.get("host", False)
        self.username = settings.get("username", False)
        self.password = settings.get("password", False)
        self.verify_ssl = settings.get("verify_ssl", "true") == "true"


@register_source("itop", ItopSourceConfiguration, ItopSourceConnectionSettings)
class ItopSource(SourceInterface):
    ConnectionSettingsClass = ItopSourceConnectionSettings
    SourceConfigClass = ItopSourceConfiguration
    auth_data: dict = None

    """ A source for Combodo iTop Data data """

    def soft_delete_active(self) -> bool:
        """Check if the soft_delete option is active

        Returns:
            bool: True/False
        """
        if self.source_configuration.soft_delete is None:
            return False

        return self.source_configuration.soft_delete.get("enabled", False)

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
            if f"{soft_delete.get('field', 'status').lower()}" not in oql.lower():
                combining_word = "WHERE" if "WHERE" not in oql.upper() else "AND"
                oql += (
                    f" {combining_word} {soft_delete.get('field', 'status')} "
                    + f"= '{soft_delete.get('active_value', 'enabled')}'"
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
                    soft_delete.get("field", "status"),
                    [soft_delete.get("inactive_value", "inactive")] * len(deleteData),
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
