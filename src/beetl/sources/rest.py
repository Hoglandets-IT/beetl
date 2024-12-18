import polars as pl
import pandas as pd
from pyarrow.lib import ArrowInvalid
from .interface import (
    register_source,
    SourceInterface,
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


class PaginationSettings:
    enabled: bool = False
    pageSize: int = 10
    startPage: int = 0
    pageSizeQuery: str = "pageSize"
    pageQuery: str = "page"
    totalQuery: str = "page.count"
    queryIn: str = "query"
    totalQueryIn: str = "body"

    def __init__(self, **kwargs):
        self.enabled = kwargs.get("enabled", False)
        self.pageSizeQuery = kwargs.get("pageSizeQuery", "pageSize")
        self.pageSize = kwargs.get("pageSize", 10)
        self.startPage = kwargs.get("startPage", 0)
        self.pageQuery = kwargs.get("pageQuery", "page")
        self.totalQuery = kwargs.get("totalQuery", "count")
        self.queryIn = kwargs.get("queryIn", "query")


class RestRequest:
    path: str = None
    query: dict = {}
    method: str = "GET"
    pagination: PaginationSettings = None
    body_type: str = "application/json"
    body = None
    body_options: BodyOptions = None
    return_type: str = "application/json"
    response: RestResponse = None

    def __init__(self, path: str = None, query: dict = {}, method: str = "GET", body_type: str = "application/json", body=None, body_options: dict = None, return_type: str = "application/json", response: dict = None, pagination: dict = None):
        self.path = path
        self.method = method
        self.body_type = body_type
        self.body = body
        self.return_type = return_type
        if body_options is not None:
            self.body_options = BodyOptions(**body_options)
        if pagination is not None:
            self.pagination = PaginationSettings(**pagination)
        self.response = response
        self.query = query


class RestSourceConfiguration(SourceInterfaceConfiguration):
    """The configuration class used for MySQL sources"""
    listRequest: RestRequest
    createRequest: RestRequest
    updateRequest: RestRequest
    deleteRequest: RestRequest

    def __init__(self, listRequest: dict = None, createRequest: dict = None, updateRequest: dict = None, deleteRequest: dict = None):
        super().__init__()
        self.listRequest = RestRequest(**listRequest)
        self.createRequest = RestRequest(
            **createRequest if createRequest is not None else {})
        self.updateRequest = RestRequest(
            **updateRequest if updateRequest is not None else {})
        self.deleteRequest = RestRequest(
            **deleteRequest if deleteRequest is not None else {})


class Oauth2Settings:
    def __init__(self, flow: str = "", token_url: str = "", authorization_url: str = "", token_body: dict = {}, token_path: str = "access_token", valid_to_path: str = "expires_on"):
        self.flow = flow
        self.token_url = token_url
        self.authorization_url = authorization_url
        self.token_body = token_body
        self.token_path = token_path
        self.valid_to_path = valid_to_path


class RestAuthentication:
    basic: bool = False
    basic_user: str = None
    basic_pass: str = None
    bearer: bool = False
    bearer_prefix: str = "Bearer"
    bearer_token: str = None
    oauth2: bool = False
    oauth2_settings: Oauth2Settings = None

    # def __init__(self, basic: bool = False, basic_user: str = None, basic_pass: str = None, bearer: bool = False, bearer_prefix: str = "Bearer", bearer_token: str = None, soft_delete: bool = False, delete_field: str = "deleted", deleted_value = None):

    def __init__(self, **kvargs):
        self.basic = kvargs.get("basic", False)
        self.basic_user = kvargs.get("basic_user", None)
        self.basic_pass = kvargs.get("basic_pass", None)
        self.bearer = kvargs.get("bearer", False)
        self.bearer_prefix = kvargs.get("bearer_prefix", "Bearer")
        self.bearer_token = kvargs.get("bearer_token", None)
        self.oauth2 = kvargs.get("oauth2", False)
        self.oauth2_settings = kvargs.get("oauth2_settings", {})
        if self.oauth2:
            self.oauth2_settings = Oauth2Settings(**kvargs["oauth2_settings"])


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
            self.authentication = RestAuthentication(
                **settings["authentication"])

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
    authValidTo = None

    """ A source for MySQL data """

    def _configure(self):
        if self.client is None:
            self.client = requests.Session()
            if not self.connection_settings.authentication:
                return
            if self.connection_settings.authentication.basic:
                self.client.auth = (self.connection_settings.authentication.basic_user,
                                    self.connection_settings.authentication.basic_pass)

            if self.connection_settings.authentication.bearer:
                self.client.headers.update(
                    {"Authorization": f"{self.connection_settings.authentication.bearer_prefix} {self.connection_settings.authentication.bearer_token}"})

            if self.connection_settings.authentication.oauth2:
                if self.connection_settings.authentication.oauth2_settings.flow == "client_credentials":
                    response = self.client.post(
                        self.connection_settings.authentication.oauth2_settings.token_url,
                        data=self.connection_settings.authentication.oauth2_settings.token_body
                    )
                    if response.status_code != 200:
                        raise Exception(
                            "Failed to authenticate: ", response.text)

                    response_data = response.json()
                    self.client.headers.update(
                        {"Authorization": f"{self.connection_settings.authentication.bearer_prefix} {response_data[self.connection_settings.authentication.oauth2_settings.token_path]}"})

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
            "params": {}
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

                if len(lrSettings.response["items"]) > 0:
                    for key in lrSettings.response["items"].split("."):
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

            if (self.source_configuration.listRequest.response and
                self.source_configuration.listRequest.response["items"] and
                    len(self.source_configuration.listRequest.response["items"]) > 0):
                for key in self.source_configuration.listRequest.response["items"].split("."):
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
        self, data: pl.DataFrame,
    ):
        self._connect()

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
            body_path = self.source_configuration.createRequest.body_options.object_path.split(
                ".")

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
                body[body_path[0]][body_path[1]
                                   ][body_path[2]][body_path[3]] = allItems
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
                        body[body_path[0]][body_path[1]
                                           ][body_path[2]][body_path[3]] = item
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
