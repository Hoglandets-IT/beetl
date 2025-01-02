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
        result = transformer.format(
            data, inField, outField, format_string)

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
        result = transformer.format(
            data, inField, outField)

        # assert

        resultingString = result[outField][0]
        self.assertEqual(expected, resultingString)
