from unittest import TestCase

from src.beetl.transformers.integer_schema import IntegerTransformerSchema
from src.beetl.transformers.interface import TransformerSchemaBase


class UnitTestIntegerTransformersSchema(TestCase):
    """Theses tests makes sure that we're not breaking the schema for the integer transformer without noticing it.

    Altering these tests is a good way to double check that the changes you made are as expected.
    """

    def test_divide__with_valid_input__model_is_valid(self):
        cls = IntegerTransformerSchema.Divide
        input = {
            "transformer": "int.divide",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "factor": 2,
                "inType": "Int64",
                "outType": "Int64",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_fillna__with_valid_input__model_is_valid(self):
        cls = IntegerTransformerSchema.Fillna
        input = {
            "transformer": "int.fillna",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "value": 2,
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_to_int64__with_valid_input__model_is_valid(self):
        cls = IntegerTransformerSchema.ToInt64
        input = {
            "transformer": "int.to_int64",
            "config": {
                "inField": "field1",
                "outField": "field2",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def assertValidatesSuccessfully(self, input, cls: TransformerSchemaBase):
        result = cls(**input)

        self.assertIsNotNone(result)
