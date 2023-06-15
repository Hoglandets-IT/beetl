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

BULK_CUTOFF = 500


class ItopSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for iTop sources"""

    datamodel: str = None
    oql_key: str = None
    has_foreign: bool = None
    soft_delete: dict = None

    def __init__(
        self,
        columns: list,
        datamodel: str = None,
        oql_key: str = None,
        soft_delete: dict = None,
        comparison_columns: list = None,
    ):
        super().__init__(columns)
        self.datamodel = datamodel
        self.oql_key = oql_key
        self.has_foreign = False
        self.comparison_columns = (
            self.get_output_fields_for_request()
            if comparison_columns is None
            else comparison_columns
        )
        self.soft_delete = soft_delete

    def get_output_fields_for_request(self):
        output = []
        for field in self.columns:
            if field.custom_options is not None:
                if field.custom_options.get("itop", {}).get("comparison_field", False):
                    self.has_foreign = True
                    output.append(field.custom_options["itop"]["comparison_field"])
                    continue

            if not field.unique:
                output.append(field.name)

        return output


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

        self.url = os.path.join(
            f"https://{self.connection_settings.host}",
            "webservices",
            "rest.php?version=1.3&login_mode=form",
        )

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
        data = {}
        if params is not None:
            data = params

        data["operation"] = operation

        if params is not None:
            for key, value in params.items():
                data[key] = value

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
            if f"where {soft_delete.get('field', 'status').lower()}" not in oql:
                oql += (
                    f" WHERE {soft_delete.get('field', 'status')} "
                    + f"= '{soft_delete.get('active_value', 'enabled')}'"
                )

        files = self._create_body(
            "core/get",
            {
                "class": self.source_configuration.datamodel,
                "key": oql,
                "output_fields": ",".join(
                    self.source_configuration.unique_columns
                    + self.source_configuration.comparison_columns
                ),
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

            if objects is not None:
                for _, item in res_json.get("objects", {}).items():
                    re_obj.append({"id": item["key"], **item["fields"]})
                return pl.DataFrame(re_obj)
            return pl.DataFrame()

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
                "fields": {x: y for x, y in data.items() if y is not None},
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

        for k in tuple(data.keys()):
            if k in self.source_configuration.unique_columns:
                identifiers[k] = data.pop(k)

        wh_string = " AND ".join(
            [f"{key} = '{value}'" for key, value in identifiers.items()]
        )

        body = self._create_body(
            "core/update",
            {
                "comment": comment,
                "class": type,
                "key": f"SELECT {type} WHERE {wh_string}",
                "fields": {x: y for x, y in data.items() if y is not None},
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

        for k in tuple(data.keys()):
            if k in self.source_configuration.unique_columns:
                identifiers[k] = data.pop(k)

        wh_string = " AND ".join(
            [f"{key} = '{value}'" for key, value in identifiers.items()]
        )

        body = self._create_body(
            "core/delete",
            {
                "comment": comment,
                "class": type,
                "key": f"SELECT {type} WHERE {wh_string}",
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

        insert_cols = tuple(x.name for x in self.source_configuration.columns)

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
            x.name for x in self.source_configuration.columns if x.skip_update is False
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
