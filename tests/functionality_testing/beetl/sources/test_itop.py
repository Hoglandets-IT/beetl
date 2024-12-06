import unittest
from src.beetl.beetl import Beetl, BeetlConfig
from tests.configurations.itop import organizations_from_static_to_itop
from tests.helpers.secrets import get_test_secrets

top_level: "Hoglandet"
skip_tests = False
try:
    secrets = get_test_secrets()
except:
    skip_tests = True


@unittest.skipIf(skip_tests, "No iTop secrets provided")
class TestItopSource(unittest.TestCase):
    def test_itop(self):
        config_dict = organizations_from_static_to_itop(
            secrets.itop.url, secrets.itop.username, secrets.itop.password
        )
        config = BeetlConfig(config_dict)
        beetl_instance = Beetl(config)
        # Friday: tests just passed so the config can be parsed, test carefully

        # Don't run this yet
        # result = beetl_instance.sync(config)
