import polars as pl
from .interface import TransformerInterface, register_transformer_class

@register_transformer_class("int")
class IntegerTransformer(TransformerInterface):
    @staticmethod
    def divide(data: pl.DataFrame, inField: str, outField: str, factor: int, inType: str = "Int64", outType: str = "Int32") -> pl.DataFrame:
        """Divide the numbers in a given column with the given factor to a rounded integer

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to process
            outField (str): The field to put the output into
            factor (int): The field to divide by

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        inType = getattr(pl, inType)
        outType = getattr(pl, outType)
        
        data = data.with_columns((data[inField].cast(inType) / factor).round(0).cast(outType).alias(outField))
        
        return data