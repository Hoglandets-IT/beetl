import unittest
from faker import Faker
from src.beetl.beetl import Beetl, BeetlConfig
from tests.configurations.rest import to_static
from tests.helpers.manual_result import ManualResult
from tests.project_testcontainers.rest import RestContainer
USERS = "users"

endpoints = {
    USERS: []
}


class TestRestSource(unittest.TestCase):
    faker: Faker = Faker()

    def insert_random_users(self, count, rest: RestContainer):
        for _ in range(count):
            rest.post(USERS, {"id": self.faker.uuid4(),
                      "name": self.faker.name(), "email": self.faker.email(), "address": {"city": self.faker.city(), "street": self.faker.street_name()}})

    def test_sync_between_rest_and_static_sources(self):
        """The rest source only supports being used as a source and not as a destination. This test will only test that we can extract data."""
        with RestContainer(endpoints) as rest:
            # Arrange
            self.insert_random_users(10, rest)
            config = BeetlConfig(to_static(rest.get_base_url(), f"/{USERS}"))
            beetl_instance = Beetl(config)

            # Act
            inserted_ten_result = beetl_instance.sync()

            # Assert
            self.assertEqual(inserted_ten_result, ManualResult(10, 0, 0))
