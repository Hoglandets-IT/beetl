from unittest import TestCase

from pydantic import ValidationError

from src.beetl.transformers.interface import TransformerSchemaBase
from src.beetl.transformers.strings_schema import StringTransformerSchema


class UnitTestStringTransformersSchema(TestCase):
    """Theses tests makes sure that we're not breaking the schema for the strings transformer without noticing it.

    Altering these tests is a good way to double check that the changes you made are as expected.
    """

    def test_staticfield__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.StaticField
        input = {
            "transformer": "strings.staticfield",
            "config": {"field": "field1", "value": "hello"},
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_set_default__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.SetDefaults
        input = {
            "transformer": "strings.set_default",
            "config": {"inField": "field1", "defaultValue": "hello"},
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_strip__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Strip
        input = {
            "transformer": "strings.strip",
            "config": {
                "inField": "field1",
                "stripChars": "hello",
                "outField": "field2",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_lowercase__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Lowercase
        input = {
            "transformer": "strings.lowercase",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "inOutMap": {"hello": "world"},
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_uppercase__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Uppercase
        input = {
            "transformer": "strings.uppercase",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "inOutMap": {"hello": "world"},
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_uppercase_infield_validator__both_map_and_infield_is_missing__exception_is_raised(
        self,
    ):
        cls = StringTransformerSchema.Uppercase
        input = {
            "transformer": "strings.uppercase",
            "config": {"outField": "field2"},
        }

        callable_function = lambda: self.assertValidatesSuccessfully(input, cls)

        self.assertRaises(ValidationError, callable_function)

    def test_match_contains__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.MatchContains
        input = {
            "transformer": "strings.match_contains",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "match": "hello",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_join__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Join
        input = {
            "transformer": "strings.join",
            "config": {
                "inFields": ["field1", "field2"],
                "outField": "field3",
                "separator": ",",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_join_listfield__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.JoinListfield
        input = {
            "transformer": "strings.join_listfield",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "separator": ",",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_split__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Split
        input = {
            "transformer": "strings.split",
            "config": {
                "inField": "field1",
                "outFields": ["first", "second"],
                "separator": ",",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_split_into_listfield__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.SplitIntoListfield
        input = {
            "transformer": "strings.split_into_listfield",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "separator": ",",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_quote__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Quote
        input = {
            "transformer": "strings.quote",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "quote": "'",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_replace__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Replace
        input = {
            "transformer": "strings.replace",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "search": "search",
                "replace": "replace",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_replace_all__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.ReplaceAll
        input = {
            "transformer": "strings.replace_all",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "search": "search",
                "replace": "replace",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_substring__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Substring
        input = {
            "transformer": "strings.substring",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "start": 0,
                "length": 0,
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_add_prefix__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.AddPrefix
        input = {
            "transformer": "strings.add_prefix",
            "config": {
                "inField": "field1",
                "outField": "field2",
                "prefix": "prefix",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_cast__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Cast
        input = {
            "transformer": "strings.cast",
            "config": {
                "inField": "field1",
                "outField": "field2",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_hash__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.Hash
        input = {
            "transformer": "strings.hash",
            "config": {
                "inField": "field1",
                "outField": "field2",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def test_to_object_id__with_valid_input__model_is_valid(self):
        cls = StringTransformerSchema.ToObjectId
        input = {
            "transformer": "strings.to_object_id",
            "config": {
                "inField": "field1",
            },
        }

        self.assertValidatesSuccessfully(input, cls)

    def assertValidatesSuccessfully(self, input, cls: TransformerSchemaBase):
        result = cls(**input)

        self.assertIsNotNone(result)
