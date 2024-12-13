import os
import yaml
from dataclasses import dataclass


@dataclass
class ItopSecrets:
    url: str
    username: str
    password: str

    def is_valid(self):
        return self.url and self.username and self.password


class Secrets:
    itop: ItopSecrets

    def __init__(self, itop: dict[str, str]):
        self.itop = ItopSecrets(**itop)


def get_test_secrets():
    try:
        with open(os.path.join(os.getcwd(), "test.secrets.yaml")) as f:
            yaml_string = f.read()
    except FileNotFoundError:
        raise Exception(
            "No test.secrets.yaml found, please copy test.secrets.template.yaml to test.secrets.yaml and fill in the values."
        )

    if not yaml_string:
        raise Exception(
            "No secrets found in test.secrets.yaml, please copy the contents from test.secrets.template.yaml and fill in the values."
        )

    secrets_dict = yaml.safe_load(yaml_string)
    return Secrets(**secrets_dict)
