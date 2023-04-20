from typing import List
import polars as pl
from .interface import (
    FieldTransformerInterface,
    register_transformer
)

class FrameFieldTransformer(FieldTransformerInterface):
    @staticmethod
    @register_transformer('field', 'frames', 'drop_columns')
    def drop_columns(data: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Drop columns from a DataFrame

        Args:
            data (pl.DataFrame): Drop columns from this dataframe
            columns (List[str]): The columns to dr op

        Returns:
            pl.DataFrame: The dataframe without the columns
        """
        return data.drop(columns)
    