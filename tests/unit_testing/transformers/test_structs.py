import unittest

from polars import DataFrame, Series

from src.beetl.transformers.structs import StructTransformers


class UnitTestStructsTransformers(unittest.TestCase):
    def test_jsonpath_single_dollar_seletor(self):
        # arrange
        inField = "field1"
        outField = "field2"
        jsonPath = "$"
        defaultValue = "default"
        expected = "value"
        data = DataFrame().with_columns(Series(inField, [expected]))
        transformer = StructTransformers()

        # act
        result = transformer.jsonpath(data, inField, outField, jsonPath, defaultValue)

        # assert
        resultingString = result[outField][0]
        self.assertEqual(expected, resultingString)

    def test_jsonpath_property_in_object(self):
        # arrange
        inField = "field1"
        outField = "field2"
        jsonPath = "$.nested_property"
        defaultValue = "default"
        expected = "value"
        data = DataFrame().with_columns(
            Series(inField, [{"nested_property": expected}])
        )
        transformer = StructTransformers()

        # act
        result = transformer.jsonpath(data, inField, outField, jsonPath, defaultValue)

        # assert
        resultingString = result[outField][0]
        self.assertEqual(expected, resultingString)

    def test_jsonpath_property_in_object_in_list(self):
        # arrange
        inField = "field1"
        outField = "field2"
        jsonPath = "$.*.nested_property"
        defaultValue = "default"
        value = "value"
        expected = [value]
        data = DataFrame().with_columns(Series(inField, [[{"nested_property": value}]]))
        transformer = StructTransformers()

        # act
        result = transformer.jsonpath(data, inField, outField, jsonPath, defaultValue)

        # assert
        resultingString = list(result[outField][0])
        self.assertListEqual(expected, resultingString)
