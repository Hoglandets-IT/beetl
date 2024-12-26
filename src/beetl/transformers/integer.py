import polars as pl
from .interface import TransformerInterface, register_transformer_class

NAN_SUPPORTED_TYPES = [pl.Decimal, pl.Float32, pl.Float64, pl.Int8,
                       pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]


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

        data = data.with_columns(
            (data[inField].cast(inType) / factor).round(0).cast(outType).alias(outField))

        return data

    @staticmethod
    def fillna(data: pl.DataFrame, inField: str, outField: str = None, value: int = 0) -> pl.DataFrame:
        """Fill the missing NaN and null values in a given column with the given value

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to process
            outField (str): The field to put the output into
            value (int): The value to fill

        Returns:
            pl.DataFrame: The resulting DataFrame
        """

        data = data.with_columns(data[inField].fill_null(
            value).alias(outField or inField))

        if data[inField].dtype in NAN_SUPPORTED_TYPES:
            data = data.with_columns(data[inField].fill_nan(
                value).alias(outField or inField))

        return data

    @staticmethod
    def to_int64(data: pl.DataFrame, inField: str, outField: str = None) -> pl.DataFrame:
        """Convert the numbers in a given column to Int64

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to process
            outField (str): The field to put the output into

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        old_series = data[inField]
        new_series = old_series.cast(pl.Int64)
        series_with_alias = new_series.alias(outField or inField)
        data = data.with_columns(series_with_alias)

        return data
