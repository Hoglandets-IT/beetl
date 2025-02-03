from unittest import TestCase

from src.beetl.transformers.interface import TransformerSchemaBase
from src.beetl.transformers.itop_schema import ItopTransformerSchema


class UnitTestItopTransformersSchema(TestCase):
    """Theses tests makes sure that we're not breaking the schema for the itop transformer without noticing it.

    Altering these tests is a good way to double check that the changes you made are as expected.
    """

    def test_orgcode__with_valid_input__model_is_valid(self):
        cls = ItopTransformerSchema.Orgcode
        input = {
            "transformer": "itop.orgcode",
            "config": {
                "inFields": ["field1", "field2"],
                "outField": "field3",
                "toplevel": "my_org",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_orgtree__with_valid_input__model_is_valid(self):
        cls = ItopTransformerSchema.Orgtree
        input = {
            "transformer": "itop.orgtree",
            "config": {
                "treeFields": ["field1", "field2"],
                "toplevel": "my_org",
                "name_field": "name",
                "path_field": "orgpath",
                "code_field": "code",
                "parent_field": "parent_code",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_relations__with_valid_input__model_is_valid(self):
        cls = ItopTransformerSchema.Relations
        input = {
            "transformer": "itop.relations",
            "config": {
                "field_relations": [
                    {
                        "source_field": "field1",
                        "source_comparison_field": "field2",
                        "foreign_class_type": "Organization",
                        "foreign_comparison_field": "field3",
                        "use_like_operator": True,
                    }
                ]
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def assertValidatesSuccessfully(self, input, cls: TransformerSchemaBase):
        result = cls(**input)

        self.assertIsNotNone(result)
