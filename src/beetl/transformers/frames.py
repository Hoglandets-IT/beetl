from typing import List
import polars as pl
from .interface import (
    TransformerInterface,
    register_transformer
)

class FrameTransformer(TransformerInterface):
    @staticmethod
    @register_transformer('frames', 'rename_columns')
    def rename_columns(data: pl.DataFrame, columns: List[dict]):
        """Rename columns in the dataset

        Args:
            data (pl.DataFrame): The dataset
            columns (List[dict]): A list of dicts (from: col1, to: col2)

        Returns:
            pl.DataFrame: DataFrame with renamed columns
        """
        for column in columns:
            data.rename(column['from'], column['to'])
        return data
    
    @staticmethod
    @register_transformer('frames', 'copy_columns')
    def copy_columns(data: pl.DataFrame, columns: List[dict]):
        """Copies a given column to another column

        Args:
            data (pl.DataFrame): The dataset
            columns (List[dict]): A list of dicts (from: col1, to: col2)

        Returns:
            pl.DataFrame: DataFrame with renamed columns
        """
        for column in columns:
            data[column['to']] = data[column['from']]
        
        return data  
    
    
    @staticmethod
    @register_transformer('frames', 'drop_columns')
    def drop_columns(data: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Drop columns from a DataFrame

        Args:
            data (pl.DataFrame): Drop columns from this dataframe
            columns (List[str]): The columns to dr op

        Returns:
            pl.DataFrame: The dataframe without the columns
        """
        return data.drop(columns)
    