from unittest import TestCase

from src.beetl.transformers.frames_schema import FramesTransformerSchema
from src.beetl.transformers.interface import TransformerSchemaBase


class UnitTestFrameTransformersSchema(TestCase):
    """Theses tests makes sure that we're not breaking the schema for the frame transformer without noticing it.

    Altering these tests is a good way to double check that the changes you made are as expected.
    """

    def test_filter__with_valid_input__model_is_valid(self):
        cls = FramesTransformerSchema.Filter
        input = {
            "transformer": "frames.filter",
            "config": {"filter": {"column": "value"}, "reverse": True},
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_conditional__with_valid_input__model_is_valid(self):
        cls = FramesTransformerSchema.Conditional
        input = {
            "transformer": "frames.conditional",
            "config": {
                "conditionField": "field1",
                "ifTrue": "value",
                "ifFalse": "value",
                "targetField": "field2",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_rename_columns__with_dict_input__model_is_valid(self):
        cls = FramesTransformerSchema.RenameColumns
        input = {
            "transformer": "frames.rename_columns",
            "config": {
                "columns": {"old_name": "new_name"},
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_rename_columns__with_mapping_list_input__model_is_valid(self):
        cls = FramesTransformerSchema.RenameColumns
        input = {
            "transformer": "frames.rename_columns",
            "config": {
                "columns": [{"from": "old_name", "to": "new_name"}],
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_copy_columns__with_valid_input__model_is_valid(self):
        cls = FramesTransformerSchema.CopyColumns
        input = {
            "transformer": "frames.copy_columns",
            "config": {
                "columns": [{"from": "old_name", "to": "new_name"}],
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_drop_columns__with_valid_input__model_is_valid(self):
        cls = FramesTransformerSchema.DropColumns
        input = {
            "transformer": "frames.drop_columns",
            "config": {
                "columns": ["column1", "column2"],
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_distinct__with_valid_input__model_is_valid(self):
        cls = FramesTransformerSchema.Distinct
        input = {
            "transformer": "frames.distinct",
            "config": {
                "columns": ["column1", "column2"],
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_extract_nested_rows__with_valid_input__model_is_valid(self):
        cls = FramesTransformerSchema.ExtractNestedRows
        input = {
            "transformer": "frames.extract_nested_rows",
            "config": {
                "iterField": "items",
                "fieldMap": {
                    "Name": "name",
                    "Age": "age",
                    "FavoriteColor": "favorite_color",
                },
                "colMap": {"Id": "id"},
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def assertValidatesSuccessfully(self, input, cls: TransformerSchemaBase):
        result = cls(**input)

        self.assertIsNotNone(result)
