from unittest import TestCase

from src.beetl.transformers.interface import TransformerSchemaBase
from src.beetl.transformers.structs_schema import StructTransformerSchema


class UnitTestStructTransformersSchema(TestCase):
    """Theses tests makes sure that we're not breaking the schema for the structs transformer without noticing it.

    Altering these tests is a good way to double check that the changes you made are as expected.
    """

    def test_staticfield__with_valid_input__model_is_valid(self):
        cls = StructTransformerSchema.StaticField
        input = {
            "transformer": "structs.staticfield",
            "config": {
                "field": "field1",
                "value": "hello",
                "only_add_if_missing": True,
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_jsonpath__with_valid_input__model_is_valid(self):
        cls = StructTransformerSchema.Jsonpath
        input = {
            "transformer": "structs.jsonpath",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "jsonPath": "$this.path",
                "defaultValue": "value",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_compose_struct__with_valid_input__model_is_valid(self):
        cls = StructTransformerSchema.ComposeStruct
        input = {
            "transformer": "structs.compose_struct",
            "config": {
                "outField": "field2",
                "map": {"field1": "field2"},
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_compose_list_of_structs__with_valid_input__model_is_valid(self):
        cls = StructTransformerSchema.ComposeListOfStructs
        input = {
            "transformer": "structs.compose_list_of_structs",
            "config": {
                "outField": "field2",
                "map": {"field1": "field2"},
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def assertValidatesSuccessfully(self, input, cls: TransformerSchemaBase):
        result = cls(**input)

        self.assertIsNotNone(result)
