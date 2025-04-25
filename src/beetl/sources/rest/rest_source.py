import pandas as pd
import polars as pl
import requests
from pyarrow.lib import ArrowInvalid

from ..interface import SourceInterface
from ..registrated_source import register_source
from .rest_config import RestConfig, RestConfigArguments
from .rest_sync import RestSync, RestSyncArguments


@register_source("Rest")
class RestSource(SourceInterface):
    ConfigArgumentsClass = RestConfigArguments
    ConfigClass = RestConfig
    SyncArgumentsClass = RestSyncArguments
    SyncClass = RestSync

    source_configuration: RestSync
    connection_settings: RestConfig
    client = None
    authValidTo = None

    """ A source for MySQL data """

    def _configure(self):
        if self.client is None:
            self.client = requests.Session()
            if not self.connection_settings.authentication:
                return
            if self.connection_settings.authentication.basic:
                self.client.auth = (
                    self.connection_settings.authentication.basic_user,
                    self.connection_settings.authentication.basic_pass,
                )

            if self.connection_settings.authentication.bearer:
                self.client.headers.update(
                    {
                        "Authorization": f"{self.connection_settings.authentication.bearer_prefix} {self.connection_settings.authentication.bearer_token}"
                    }
                )

            if self.connection_settings.authentication.oauth2:
                if (
                    self.connection_settings.authentication.oauth2_settings.flow
                    == "client_credentials"
                ):
                    response = self.client.post(
                        self.connection_settings.authentication.oauth2_settings.token_url,
                        data=self.connection_settings.authentication.oauth2_settings.token_body,
                    )
                    if response.status_code != 200:
                        raise Exception("Failed to authenticate: ", response.text)

                    response_data = response.json()
                    self.client.headers.update(
                        {
                            "Authorization": f"{self.connection_settings.authentication.bearer_prefix} {response_data[self.connection_settings.authentication.oauth2_settings.token_path]}"
                        }
                    )

    def _connect(self):
        if self.authValidTo is None:
            return True

        if self.authValidTo < pd.Timestamp.now():
            return self._configure()

    def _disconnect(self):
        pass

    def _query(
        self, params=None, customQuery: str = None, returnData: bool = True
    ) -> pl.DataFrame:
        self._connect()

        lrSettings = self.source_configuration.listRequest

        request = {
            "method": lrSettings.method,
            "url": self.connection_settings.base_url + lrSettings.path,
            "headers": {"Content-Type": lrSettings.body_type},
            "verify": (not self.connection_settings.ignore_certificates),
            "params": {},
        }

        if lrSettings.return_type != "":
            request["headers"]["Accept"] = lrSettings.return_type

        if lrSettings.body_type == "application/json" and lrSettings.body is not None:
            request["json"] = lrSettings.body

        if lrSettings.query is not None:
            request["params"] = lrSettings.query

        else:
            request["data"] = lrSettings.body

        if lrSettings.pagination is not None and lrSettings.pagination.enabled:
            response_data = []
            page = lrSettings.pagination.startPage
            pageSize = lrSettings.pagination.pageSize
            total = 1

            while len(response_data) < total:
                request["params"][lrSettings.pagination.pageQuery] = page
                request["params"][lrSettings.pagination.pageSizeQuery] = pageSize

                response = self.client.request(**request)
                if response.status_code != 200:
                    if len(response_data) > 0:
                        break
                    raise Exception("Failed request")

                if lrSettings.return_type == "application/json":
                    page_response_data = response.json()
                else:
                    raise Exception("Not implemented")

                if lrSettings.pagination.totalQueryIn != "":
                    if lrSettings.pagination.totalQueryIn == "body":
                        tq = page_response_data
                        for key in lrSettings.pagination.totalQuery.split("."):
                            tq = tq[key]

                        if tq > total:
                            total = tq

                    elif lrSettings.pagination.totalQueryIn == "header":
                        total = response.headers[lrSettings.pagination.totalQuery]

                if len(lrSettings.response.items) > 0:
                    for key in lrSettings.response.items.split("."):
                        page_response_data = page_response_data[key]

                if len(response_data) == 0 and len(page_response_data) < pageSize:
                    pageSize = len(page_response_data)

                response_data += page_response_data
                page += 1

        else:
            response = self.client.request(**request)

            if response.status_code != 200:
                raise Exception("Failed request: ", response.text)

            if self.source_configuration.listRequest.return_type == "application/json":
                response_data = response.json()
            else:
                raise Exception("Not implemented")

            if (
                self.source_configuration.listRequest.response
                and self.source_configuration.listRequest.response.items
                and len(self.source_configuration.listRequest.response.items) > 0
            ):
                for key in self.source_configuration.listRequest.response.items.split(
                    "."
                ):
                    response_data = response_data[key]

            if len(response_data) == 0:
                return pl.DataFrame()

        pd_df = pd.json_normalize(response_data)

        try:
            return pl.from_pandas(pd_df)
        except ArrowInvalid:
            for col in pd_df.columns:
                if pd_df[col].dtype == "object":
                    pd_df[col] = pd_df[col].astype("string")

            return pl.from_pandas(pd_df)

    def _insert(
        self,
        data: pl.DataFrame,
    ):
        self._connect()

        request = {
            "method": self.source_configuration.createRequest.method,
            "url": self.connection_settings.base_url
            + self.source_configuration.createRequest.path,
            "headers": {
                "Content-Type": self.source_configuration.createRequest.body_type
            },
            "verify": (not self.connection_settings.ignore_certificates),
        }

        body_template = {}
        body_path = []

        if self.source_configuration.createRequest.body_options.object_path != "":
            body_template = self.source_configuration.createRequest.body
            body_path = (
                self.source_configuration.createRequest.body_options.object_path.split(
                    "."
                )
            )

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
                        body[body_path[0]][body_path[1]][body_path[2]][
                            body_path[3]
                        ] = item
                else:
                    body = item

                if (
                    self.source_configuration.createRequest.body_type
                    == "application/json"
                ):
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
        if (
            self.connection_settings.soft_delete != True
            and self.source_configuration.deleteRequest.path == ""
        ):
            print("Delete not configured, skipping")
            return 0
        return 0

    def store_diff(self, diff):
        raise NotImplementedError("The rest source does not support diff storage")
