
import unittest

from polars import DataFrame, Series, Float64, Utf8

from src.beetl.transformers.integer import IntegerTransformer


class UnitTestIntegerTransformers(unittest.TestCase):

    def test_fillna__integer_field_with_nulls__nulls_are_filled_with_value(self):
        # arrange
        inField = "field1"
        value = 1
        data = DataFrame().with_columns(Series(inField, [None, None, value]))
        transformer = IntegerTransformer()

        # act
        result = transformer.fillna(data, inField, value=value)

        # assert
        self.assertEqual([value, value, value],
                         result[inField].to_list())

    def test_fillna__integer_field_with_nans_and_is_nan_supported_type__nans_are_filled_with_value(self):
        # arrange
        inField = "field1"
        value = 1
        nan = float("nan")
        data = DataFrame().with_columns(
            Series(inField, [nan, nan, value], dtype=Float64))
        transformer = IntegerTransformer()

        # act
        result = transformer.fillna(data, inField, value=value)

        # assert
        self.assertEqual([value, value, value],
                         result[inField].to_list())

    def test_fillna__integer_field_with_nans_and_is_not_nan_supported_type__nans_are_not_filled_with_value(self):
        # arrange
        inField = "field1"
        value = "value"
        nan_value = float("nan")
        data = DataFrame().with_columns(
            Series(inField, [nan_value, nan_value, value], dtype=Utf8, strict=False))
        transformer = IntegerTransformer()

        # act
        result = transformer.fillna(data, inField, value=value)

        # assert
        nan_string = 'NaN'
        self.assertEqual([nan_string, nan_string, value],
                         result[inField].to_list())
