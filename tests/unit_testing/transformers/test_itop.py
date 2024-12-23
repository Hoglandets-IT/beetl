
import unittest

from polars import DataFrame, Series

from src.beetl.transformers.itop import ItopTransformer


class UnitTestItopTransformers(unittest.TestCase):
    def test_orgcode__single_field__sets_out_field_to_hash_of_field(self):
        # arrange
        inFields = ["field1"]
        value = "a"
        outField = "code"
        toplevel = "Beetl"
        expected = "e4d19a8f608ba8bf1d0dd26583174f12dd4a2290"
        data = DataFrame().with_columns(Series(inFields[0], [value]))
        transformer = ItopTransformer()

        # act
        result = transformer.orgcode(data, inFields, outField, toplevel)

        # assert
        resultingString = result[outField][0]
        self.assertEqual(expected, resultingString)

    def test_orgcode__multi_field__sets_out_field_to_hash_of_fields(self):
        # arrange
        inFields = ["field1", "field2"]
        values = ["a", "b"]
        outField = "code"
        toplevel = "Beetl"
        expected = "34613675289b022b729fde2f9ba67b331aafcb99"
        data = DataFrame().with_columns(
            Series(inFields[0], [values[0]]), Series(inFields[1], [values[1]]))
        transformer = ItopTransformer()

        # act
        result = transformer.orgcode(data, inFields, outField, toplevel)

        # assert
        resultingString = result[outField][0]
        self.assertEqual(expected, resultingString)

    def test_orgcode__when_passed_differently_cased_values__casing_is_normalized_before_hashing(self):
        # arrange
        inFields = ["field1"]
        value = "a"
        outField = "code"
        toplevel = "Beetl"
        data = DataFrame().with_columns(Series(inFields[0], [value]))
        transformer = ItopTransformer()

        # act
        uppercaseData = DataFrame().with_columns(
            Series(inFields[0], [value.upper()]))
        uppercaseResult = transformer.orgcode(
            uppercaseData, inFields, outField, toplevel)
        uppercaseHash = uppercaseResult[outField][0]

        lowercaseData = DataFrame().with_columns(
            Series(inFields[0], [value.lower()]))
        lowercaseResult = transformer.orgcode(
            lowercaseData, inFields, outField, toplevel)
        lowercaseHash = lowercaseResult[outField][0]

        # assert
        self.assertEqual(uppercaseHash, lowercaseHash)

    def test_orgcode__when_comparing_hash_to_hash_from_same_value_plus_trailing_space__hashes_are_different(self):
        # arrange
        inFields = ["field1"]
        value = "a"
        outField = "code"
        toplevel = "Beetl"
        data = DataFrame().with_columns(Series(inFields[0], [value]))
        transformer = ItopTransformer()

        # act
        no_space_data = DataFrame().with_columns(
            Series(inFields[0], [value]))
        no_space_result = transformer.orgcode(
            no_space_data, inFields, outField, toplevel)
        no_space_hash = no_space_result[outField][0]

        with_space_data = DataFrame().with_columns(
            Series(inFields[0], [value + " "]))
        with_space_result = transformer.orgcode(
            with_space_data, inFields, outField, toplevel)
        with_space_hash = with_space_result[outField][0]

        # assert
        self.assertNotEqual(with_space_hash, no_space_hash)
