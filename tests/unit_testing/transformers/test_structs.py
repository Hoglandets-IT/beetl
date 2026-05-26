import unittest

from polars import DataFrame, Series

from src.beetl.transformers.structs import StructTransformers


class UnitTestStructsTransformers(unittest.TestCase):
    def test_jsonpath__single_dollar_selector__sets_out_field_to_in_field(self):
        # arrange
        inField = "field1"
        outField = "field2"
        jsonPath = "$"
        defaultValue = "default"
        expected = "value"
        data = DataFrame().with_columns(Series(inField, [expected]))
        transformer = StructTransformers()

        # act
        result = transformer.jsonpath(
            data, inField, outField, jsonPath, defaultValue)

        # assert
        resultingString = result[outField][0]
        self.assertEqual(expected, resultingString)

    def test_jsonpath__property_selector_on_root__sets_out_field_to_value_of_property_on_object(self):
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
        result = transformer.jsonpath(
            data, inField, outField, jsonPath, defaultValue)

        # assert
        resultingString = result[outField][0]
        self.assertEqual(expected, resultingString)

    def test_jsonpath__property_selector_on_object_in_list__sets_out_field_to_value_of_property_on_object_in_list(self):
        # arrange
        inField = "field1"
        outField = "field2"
        jsonPath = "$.*.nested_property"
        defaultValue = "default"
        value = "value"
        expected = [value]
        data = DataFrame().with_columns(
            Series(inField, [[{"nested_property": value}]]))
        transformer = StructTransformers()

        # act
        result = transformer.jsonpath(
            data, inField, outField, jsonPath, defaultValue)

        # assert
        resultingString = list(result[outField][0])
        self.assertListEqual(expected, resultingString)

    def test_staticfield__with_string_value__field_is_added_with_value(self):
        field = "field2"
        value = "value"
        data = DataFrame([{"field1": 1}])
        transformer = StructTransformers()

        result = transformer.staticfield(data, field, value)

        self.assertEqual(value, result[field][0])

    def test_staticfield__with_none_value__field_is_added_with_value(self):
        field = "field2"
        value = None
        data = DataFrame([{"field1": 1}])
        transformer = StructTransformers()

        result = transformer.staticfield(data, field, value)

        self.assertEqual(value, result[field][0])

    def test_staticfield__field_already_exists__field_is_overwritten(self):
        field = "field1"
        value = "value"
        data = DataFrame([{"field1": 1}])
        transformer = StructTransformers()

        result = transformer.staticfield(data, field, value)

        self.assertEqual(value, result[field][0])

    def test_staticfield__field_already_exists_and_only_add_if_missing_is_true__field_is_not_overwritten(self):
        field = "field1"
        value = "value"
        original_value = 1
        data = DataFrame([{"field1": original_value}])
        transformer = StructTransformers()
        only_add_if_missing = True

        result = transformer.staticfield(
            data, field, value, only_add_if_missing=only_add_if_missing)

        self.assertEqual(original_value, result[field][0])
