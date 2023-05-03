import polars as pl
from .interface import (
    FieldTransformerInterface,
    register_transformer
)

class StringFieldTransformer(FieldTransformerInterface):
    @staticmethod
    @register_transformer('field', 'strings', 'lowercase')
    def lowercase(data: pl.DataFrame, inField: str, outField: str) -> pl.DataFrame:
        """ Transform all values in a column to lowercase and insert 
            them into another (or the same) column

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to take in
            outField (str): The field to put the result in

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        __class__._validate_fields(data.columns, inField)
        
        data = data.with_columns(data[inField].str.to_lowercase().alias(outField))
        return data
    
    @staticmethod
    @register_transformer('field', 'strings', 'uppercase')
    def uppercase(data: pl.DataFrame, inField: str, outField: str) -> pl.DataFrame:
        """ Transform all values in a column to uppercase 
            and insert them into another (or the same) column

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to take in
            outField (str): The field to put the result in

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        data = data.with_columns(data[inField].str.to_uppercase().alias(outField))
        return data
    
    @staticmethod
    @register_transformer('field', 'strings', 'join')
    def join(data: pl.DataFrame, inFields: list, outField: str, separator: str = '') -> pl.DataFrame:
        """Join multiple columns together

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inFields (list): The fields to join
            outField (str): The field to put the result in
            separator (str, optional): The separator to use. Defaults to ''.

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        nCol = pl.concat_str(data[inFields], separator=separator).alias(outField)

        return data.with_columns(nCol.alias(outField))

    @staticmethod
    @register_transformer('field', 'strings', 'split')
    def split(data: pl.DataFrame, inField: str, outFields: list, separator: str = '') -> pl.DataFrame:
        """Split a column into multiple fields

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to split
            outField (str): The fields to put the result in (in order)
            separator (str, optional): The separator to use. Defaults to ''.

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        amount = len(outFields)
        data = data.with_columns(
            data[inField]
            .str.splitn(separator, amount)
            .struct.unnest()
            .rename({f'field_{i}': fld for i, fld in enumerate(outFields)})
        )

        return data
        