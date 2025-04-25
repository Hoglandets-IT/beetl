import json

import polars as pl
import requests
import requests.adapters
import urllib3
from alive_progress import alive_bar

from ...typings import PolarTypeOverridesParameters
from ..interface import SourceInterface
from ..registrated_source import register_source
from ..request_threader import RequestThreader
from .itop_config import ItopConfig, ItopConfigArguments
from .itop_sync import ItopSync, ItopSyncArguments

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BULK_CUTOFF = 300


@register_source("Itop")
class ItopSource(SourceInterface):
    ConfigArgumentsClass = ItopConfigArguments
    ConfigClass = ItopConfig
    SyncArgumentsClass = ItopSyncArguments
    SyncClass = ItopSync

    source_configuration: ItopSync = None
    connection_settings: ItopConfig = None
    auth_data: dict = None
    soft_deleted_items: pl.DataFrame = None

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
        soft_deleted_oql = None
        if self.soft_delete_active():
            soft_delete = self.source_configuration.soft_delete
            if f"{soft_delete.field.lower()}" in oql.lower():
                raise Exception(
                    "Soft delete where clause is managed by beetl please remove it from the OQL."
                )

            combining_word = "WHERE" if "WHERE" not in oql.upper() else "AND"
            oql += (
                f" {combining_word} {soft_delete.field} "
                + f"= '{soft_delete.active_value}'"
            )

            soft_deleted_oql = self.source_configuration.oql_key + (
                f" {combining_word} {soft_delete.field} "
                + f"= '{soft_delete.inactive_value}'"
            )

        all_colums = (
            self.source_configuration.unique_columns
            + self.source_configuration.comparison_columns
            + self.source_configuration.link_columns
        )

        if soft_deleted_oql:
            self.soft_deleted_items = self._run_oql_query(
                self.source_configuration.datamodel,
                soft_deleted_oql,
                self.source_configuration.unique_columns,
            )

        return self._run_oql_query(
            self.source_configuration.datamodel,
            oql,
            all_colums,
            self.source_configuration.type_overrides,
        )

    def _run_oql_query(
        self,
        data_model: str,
        oql: str,
        all_colums: tuple[str],
        type_overrides: PolarTypeOverridesParameters = {},
    ) -> pl.DataFrame:
        files = self._create_body(
            "core/get",
            {
                "class": data_model,
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

            return pl.DataFrame(re_obj, schema_overrides=type_overrides)

        except Exception as e:
            raise Exception(
                "An error occured while trying to decode the response", e
            ) from e

    def create_item(self, type: str, data: dict, comment: str = ""):
        self.set_foreign_key_none_values_to_itop_unassigned(data)

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

        where_clause_conditions = " AND ".join(
            [f"{key} = '{value}'" for key, value in identifiers.items()]
        )

        self.set_foreign_key_none_values_to_itop_unassigned(data)

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

    def set_foreign_key_none_values_to_itop_unassigned(self, data):
        for key, value in data.items():
            if key in self.source_configuration.foreign_key_columns:
                if value is None:
                    data[key] = 0

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

    def _insert(self, data: pl.DataFrame):
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

    def insert(self, data: pl.DataFrame):
        if (
            self.soft_delete_active()
            and self.soft_deleted_items is not None
            and len(self.soft_deleted_items) > 0
        ):
            # When soft delete is active we need to update the soft deleted items that are being reinserted.
            # We do this to avoid creating duplicates in iTop.
            self.update(
                data.join(
                    self.soft_deleted_items,
                    on=self.source_configuration.unique_columns,
                    how="inner",
                )
            )

            self._insert(
                data.join(
                    self.soft_deleted_items,
                    on=self.source_configuration.unique_columns,
                    how="anti",
                )
            )
        else:
            self._insert(data)

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

        update_cols = set(
            column_name
            for column_name in self.source_configuration.unique_columns
            + self.source_configuration.comparison_columns
            if column_name not in (self.source_configuration.skip_columns or [])
        )

        deleteData = deleteData.select(update_cols)

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

    def store_diff(self, diff):
        raise NotImplementedError("The iTop source does not support diff storage")
