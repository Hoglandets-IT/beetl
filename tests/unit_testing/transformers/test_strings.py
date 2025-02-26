import unittest

from polars import DataFrame, Series

from src.beetl.transformers.strings import StringTransformer


class UnitTestStringTransformers(unittest.TestCase):
    def test_format__when_passed_format_string__string_is_formatted(self):
        # arrange
        inField = "field1"
        outField = "field2"
        format_string = "_{value}_goodbye"
        expected = "_hello_goodbye"
        data = DataFrame().with_columns(Series(inField, ["hello"]))
        transformer = StringTransformer()

        # act
        result = transformer.format(data, inField, outField, format_string)

        # assert

        resultingString = result[outField][0]
        self.assertEqual(expected, resultingString)

    def test_format__when_passed_no_format_string__string_is_formatted(self):
        # arrange
        inField = "field1"
        outField = "field2"
        expected = "hello"
        data = DataFrame().with_columns(Series(inField, [expected]))
        transformer = StringTransformer()

        # act
        result = transformer.format(data, inField, outField)

        # assert

        resultingString = result[outField][0]
        self.assertEqual(expected, resultingString)

    def test_hash__with_mode_always__hash_is_created_regardless_of_input(self):
        for value in ["", None, "value"]:
            # arrange
            in_fields = ["input1", "input2"]
            out_field = "hash"
            data = DataFrame()
            for field in in_fields:
                data = data.with_columns(Series(field, [value]))

            transformer = StringTransformer()
            config = {
                "inFields": in_fields,
                "outField": out_field,
                "hashWhen": "always",
            }

            # act
            result = transformer.hash(data, **config)

            # assert
            resultingHash = result[out_field][0]
            self.assertIsNotNone(resultingHash)

    def test_hash__with_mode_if_any_value_and_one_value_is_none__hash_is_created(self):
        # arrange
        in_fields = ["field1", "field2"]
        out_field = "hash"
        data = DataFrame()
        data = data.with_columns(Series("field1", ["value"]))
        data = data.with_columns(Series("field2", [None]))

        transformer = StringTransformer()
        config = {
            "inFields": in_fields,
            "outField": out_field,
            "hashWhen": "any-value-is-populated",
        }
        # act
        result = transformer.hash(data, **config)

        # assert
        resultingHash = result[out_field][0]
        self.assertIsNotNone(resultingHash)

    def test_hash__with_mode_if_any_value_and_all_values_are_none__hash_is_not_created(
        self,
    ):
        # arrange
        in_fields = ["field1", "field2"]
        out_field = "hash"
        data = DataFrame()
        data = data.with_columns(Series("field1", [None]))
        data = data.with_columns(Series("field2", [None]))

        transformer = StringTransformer()
        config = {
            "inFields": in_fields,
            "outField": out_field,
            "hashWhen": "any-value-is-populated",
        }
        # act
        result = transformer.hash(data, **config)

        # assert
        resultingHash = result[out_field][0]
        self.assertIsNone(resultingHash)

    def test_hash__with_mode_if_all_values_are_populated_and_all_values_are_none__hash_is_not_created(
        self,
    ):
        # arrange
        in_fields = ["field1", "field2"]
        out_field = "hash"
        data = DataFrame()
        data = data.with_columns(Series("field1", [None]))
        data = data.with_columns(Series("field2", [None]))

        transformer = StringTransformer()
        config = {
            "inFields": in_fields,
            "outField": out_field,
            "hashWhen": "all-values-are-populated",
        }
        # act
        result = transformer.hash(data, **config)

        # assert
        resultingHash = result[out_field][0]
        self.assertIsNone(resultingHash)

    def test_hash__with_mode_if_all_values_are_populated_and_one_value_is_none__hash_is_not_created(
        self,
    ):
        # arrange
        in_fields = ["field1", "field2"]
        out_field = "hash"
        data = DataFrame()
        data = data.with_columns(Series("field1", [None]))
        data = data.with_columns(Series("field2", ["value"]))

        transformer = StringTransformer()
        config = {
            "inFields": in_fields,
            "outField": out_field,
            "hashWhen": "all-values-are-populated",
        }
        # act
        result = transformer.hash(data, **config)

        # assert
        resultingHash = result[out_field][0]
        self.assertIsNone(resultingHash)

    def test_hash__with_mode_if_all_values_are_populated_and_all_values_are_populated__hash_is_not_created(
        self,
    ):
        # arrange
        in_fields = ["field1", "field2"]
        out_field = "hash"
        data = DataFrame()
        data = data.with_columns(Series("field1", ["value"]))
        data = data.with_columns(Series("field2", ["value"]))

        transformer = StringTransformer()
        config = {
            "inFields": in_fields,
            "outField": out_field,
            "hashWhen": "all-values-are-populated",
        }
        # act
        result = transformer.hash(data, **config)

        # assert
        resultingHash = result[out_field][0]
        self.assertIsNotNone(resultingHash)
