from unittest import TestCase

from src.beetl.transformers.interface import TransformerSchemaBase
from src.beetl.transformers.misc_schema import MiscTransformerSchema


class UnitTestMiscTransformersSchema(TestCase):
    """Theses tests makes sure that we're not breaking the schema for the misc transformer without noticing it.

    Altering these tests is a good way to double check that the changes you made are as expected.
    """

    def test_sam_from_dn__with_valid_input__model_is_valid(self):
        cls = MiscTransformerSchema.SamFromDn
        input = {
            "transformer": "misc.sam_from_dn",
            "config": {
                "inField": "field1",
                "outField": "field2",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def assertValidatesSuccessfully(self, input, cls: TransformerSchemaBase):
        result = cls(**input)

        self.assertIsNotNone(result)
