from typing import List
import polars as pl
from .interface import TransformerInterface, register_transformer_class


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
