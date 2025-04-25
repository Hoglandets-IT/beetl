import unittest

from polars import DataFrame, Series

from src.beetl.transformers.regex import RegexTransformer


class UnitTestRegexTransformers(unittest.TestCase):
    def test_regex__match_single__regex_expression_equals_to_in_field(self):
        # arrange
        in_field = "field1"
        out_field = "field2"

        data = DataFrame().with_columns(
            Series(in_field, ["Fakturareferens: 12300  Eksjö, 575 22 Sweden"])
        )

        expression = r"Fakturareferens:\s*(\d+)"
        expected = "12300"

        transformer = RegexTransformer
        config = {"inField": in_field, "outField": out_field, "query": expression}

        # act
        result = transformer.match_single(data, **config)

        # assert
        self.assertEqual(expected, result[out_field][0])
