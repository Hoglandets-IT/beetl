from typing import Any, List
import polars as pl
from .interface import TransformerInterface, register_transformer_class
import json


@register_transformer_class("structs")
class StructTransformers(TransformerInterface):
    @staticmethod
    def staticfield(data: pl.DataFrame, field: str, value):
        """Add a struct field to the DataFrame

        Args:
            data (pl.DataFrame): The dataFrame to modify
            field (str): The field to add
            value (str): The value of the field to add

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        data = data.with_columns(pl.Series(field, [value] * len(data)))
        return data

    @staticmethod
    def jsonpath(data: pl.DataFrame, inField: str, outField: str, jsonPath: str, defaultValue: str = ""):
        """
        Retrieve a value from a jsonpath inside of a column and add it to a new column

        Args:
            data (pl.DataFrame): The dataFrame to modify
            field (str): The field to extract the jsonpath from
            outField (str): The field to add the extracted value to
            jsonPath (str): The jsonpath to extract
        """
        split_jsonpath = jsonPath.split(".")

        def get_value_at_jsonpath(fData: Any, start_at: int = 0):
            if isinstance(fData, str):
                fData = json.loads(fData)

            try:
                for index, selector in enumerate(split_jsonpath[start_at:], start=start_at):
                    if selector == "$":
                        continue
                    if selector == "*":
                        if not isinstance(fData, list) and not isinstance(fData, pl.Series):
                            raise NotImplementedError(
                                "Wildcard selector is only supported for lists")
                        fData = [get_value_at_jsonpath(fData[j], index + 1)
                                 for j in range(len(fData))]
                        return fData
                    try:
                        ix = int(selector)
                        fData = fData[ix]
                    except:
                        fData = fData[selector]
                return fData
            except:
                return defaultValue

        dCol = data[inField]
        outCol = []
        for i in range(len(dCol)):
            outCol.append(get_value_at_jsonpath(dCol[i]))

        data = data.with_columns(pl.Series(outField, outCol))

        return data
