
from typing import Union
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready
import json

import urllib.error
import urllib.parse
import urllib.request
import random


class RestContainer(DockerContainer):
    endpoint: str
    port: int

    def __init__(self, mocks: dict, image: str = "ghcr.io/hoglandets-it/json-server:latest", **kwargs):
        super().__init__(image, **kwargs)
        self.port = random.randint(1024, 65353)

        self.with_bind_ports(3000, self.port)
        self.endpoint = list(mocks.keys())[0]
        json_data = json.dumps(mocks)
        self.with_env("JSON_DATA", json_data)

    def start(self) -> "RestContainer":
        super().start()

        host = self.get_container_host_ip()
        self._connect(host, self.port, self.endpoint)

        return self

    @wait_container_is_ready(urllib.error.URLError)
    def _connect(self, host: str, port: str, endpoint: str) -> None:
        url = urllib.parse.urlunsplit(
            ("http", f"{host}:{port}", f"{endpoint}", "", ""))
        urllib.request.urlopen(url, timeout=1)

    def get_base_url(self) -> str:
        host = self.get_container_host_ip()
        port = self.port
        return f"http://{host}:{port}"

    def get_url_for_endpoint(self, endpoint: str) -> str:
        return f"{self.get_base_url()}/{endpoint}"

    def get(self, endpoint: str) -> dict:
        url = self.get_url_for_endpoint(endpoint)
        response = urllib.request.urlopen(url)
        if (response.code != 200):
            raise Exception("Failed to get data")
        return json.load(response)

    def get_single(self, endpoint: str, id: Union[str, int]) -> dict:
        base_url = self.get_url_for_endpoint(endpoint)
        url = f"{base_url}/{id}"
        response = urllib.request.urlopen(url)
        if (response.code != 200):
            raise Exception("Failed to get data")
        return json.load(response)

    def post(self, endpoint: str, data: dict) -> dict:
        url = self.get_url_for_endpoint(endpoint)
        data = json.dumps(data).encode()
        req = urllib.request.Request(url, data=data, headers={
                                     'Content-Type': 'application/json'})
        response = urllib.request.urlopen(req)
        if (response.code != 201):
            raise Exception("Failed to post data")
        return json.load(response)

    def delete(self, endpoint: str, id: Union[str, int]) -> dict:
        base_url = self.get_url_for_endpoint(endpoint)
        url = f"{base_url}/{id}"
        req = urllib.request.Request(url, method='DELETE')
        response = urllib.request.urlopen(req)
        if (response.code != 200):
            raise Exception("Failed to post data")
        return json.load(response)
