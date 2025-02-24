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

    def test_hash__when_passed_populated_fields__hash_is_created(self):
        # arrange
        in_fields = ["input1", "input2"]
        out_field = "hash"
        data = DataFrame()
        for field in in_fields:
            data = data.with_columns(Series(field, ["value"]))

        transformer = StringTransformer()

        # act
        result = transformer.hash(data, in_fields, out_field)

        # assert
        resultingHash = result[out_field][0]
        self.assertEqual(resultingHash, "8230c65c2c84fece204598b8af732c11c31ea0a8")

    def test_hash__when_passed_fields_with_none_values__hash_is_not_created(self):
        # arrange
        in_fields = ["field1", "field2"]
        out_field = "hash"
        data = DataFrame()
        for field in in_fields:
            data = data.with_columns(Series(field, [None]))

        transformer = StringTransformer()
        config = {
            "inFields": in_fields,
            "outField": out_field,
            "hashEmptyValues": False,
        }
        # act
        result = transformer.hash(data, **config)

        # assert
        resultingHash = result[out_field][0]
        self.assertIsNone(resultingHash)

    def test_hash__when_passed_fields_with_empty_string_values__hash_is_not_created(
        self,
    ):
        # arrange
        in_fields = ["field1", "field2"]
        out_field = "hash"
        data = DataFrame()
        for field in in_fields:
            data = data.with_columns(Series(field, [""]))

        transformer = StringTransformer()
        config = {
            "inFields": in_fields,
            "outField": out_field,
            "hashEmptyValues": False,
        }

        # act
        result = transformer.hash(data, **config)

        # assert
        resultingHash = result[out_field][0]
        self.assertIsNone(resultingHash)

    def test_hash__when_passed_fields_with_empty_string_values_and_hash_empty_fields_set_to_true__hash_is_created(
        self,
    ):
        # arrange
        in_fields = ["field1", "field2"]
        out_field = "hash"
        data = DataFrame()
        for field in in_fields:
            data = data.with_columns(Series(field, [""]))

        transformer = StringTransformer()
        config = {"inFields": in_fields, "outField": out_field, "hashEmptyValues": True}

        # act
        result = transformer.hash(data, **config)

        # assert
        resultingHash = result[out_field][0]
        self.assertEqual(resultingHash, "da39a3ee5e6b4b0d3255bfef95601890afd80709")

    def test_hash__when_passed_fields_with_none_values_and_hash_empty_fields_set_to_true__hash_is_created(
        self,
    ):
        # arrange
        in_fields = ["field1", "field2"]
        out_field = "hash"
        data = DataFrame()
        for field in in_fields:
            data = data.with_columns(Series(field, [None]))

        transformer = StringTransformer()
        config = {
            "inFields": in_fields,
            "outField": out_field,
            "hashEmptyValues": True,
        }

        # act
        result = transformer.hash(data, **config)

        # assert
        resultingHash = result[out_field][0]
        self.assertEqual(resultingHash, "da39a3ee5e6b4b0d3255bfef95601890afd80709")

    def test_hash__when_passed_both_inFields_and_inField__inField_is_appended_to_inFields(
        self,
    ):
        # arrange
        in_field = "field2"
        in_fields = ["field1"]
        out_field = "hash"
        data = DataFrame()
        data = data.with_columns(Series(in_field, ["value"]))
        for field in in_fields:
            data = data.with_columns(Series(field, ["value"]))

        transformer = StringTransformer()
        config = {
            "inField": in_field,
            "inFields": in_fields,
            "outField": out_field,
            "hashEmptyValues": False,
        }

        # act
        result = transformer.hash(data, **config)

        # assert
        resulting_hash = result[out_field][0]
        field_1_and_2_hash = "8230c65c2c84fece204598b8af732c11c31ea0a8"
        self.assertEqual(resulting_hash, field_1_and_2_hash)
