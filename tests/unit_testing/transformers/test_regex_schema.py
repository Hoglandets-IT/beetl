from unittest import TestCase

from src.beetl.transformers.interface import TransformerSchemaBase
from src.beetl.transformers.regex_schema import RegexTransformerSchema


class UnitTestRegexTransformersSchema(TestCase):
    """Theses tests makes sure that we're not breaking the schema for the regex transformer without noticing it.

    Altering these tests is a good way to double check that the changes you made are as expected.
    """

    def test_replace__with_valid_input__model_is_valid(self):
        cls = RegexTransformerSchema.Replace
        input = {
            "transformer": "regex.replace",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "query": "hello",
                "replace": "world",
                "maxN": 1,
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def assertValidatesSuccessfully(self, input, cls: TransformerSchemaBase):
        result = cls(**input)

        self.assertIsNotNone(result)
