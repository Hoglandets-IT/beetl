import polars as pl
from .interface import (
    TransformerInterface,
    register_transformer,
    register_transformer_class,
)
from bson import ObjectId


@register_transformer_class("strings")
class StringTransformer(TransformerInterface):
    @staticmethod
    def staticfield(data: pl.DataFrame, field: str, value: str) -> pl.DataFrame:
        """Add a static field to the DataFrame

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to process
            stripChar (str): The character to strip

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        data = data.with_columns(pl.Series(field, [value] * len(data)))
        return data

    @staticmethod
    def set_default(data: pl.DataFrame, inField: str, defaultValue: str) -> pl.DataFrame:
        """Set a default value for a column if the current value is null

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to process
            defaultValue (str): The default value

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        __class__._validate_fields(data.columns, inField)

        data = data.with_columns(data[inField].fill_null(defaultValue))
        return data

    @staticmethod
    def strip(data: pl.DataFrame, inField: str, stripChars: str, outField: str = None) -> pl.DataFrame:
        """Strip all given characters from a column

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to process
            stripChar (str): The character to strip

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        __class__._validate_fields(data.columns, inField)

        data = data.with_columns(data[inField].str.strip(stripChars).alias(
            outField if outField is not None else inField))
        return data

    @staticmethod
    def lowercase(data: pl.DataFrame, inField: str = "", outField: str = "", inOutMap: dict = {}) -> pl.DataFrame:
        """Transform all values in a column to lowercase and insert
            them into another (or the same) column

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to take in
            outField (str): The field to put the result in

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        __class__._validate_fields(data.columns, inField)

        if len(inOutMap) == 0:
            inOutMap = {
                inField: outField if outField is not None and outField != "" else inField}

        for inf, outf in inOutMap.items():
            data = data.with_columns(data[inf].str.to_lowercase().alias(outf))

        return data

    @staticmethod
    def uppercase(data: pl.DataFrame, inField: str = "", outField: str = "", inOutMap: dict = {}) -> pl.DataFrame:
        """Transform all values in a column to uppercase
            and insert them into another (or the same) column

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to take in
            outField (str): The field to put the result in

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        if len(inOutMap) == 0:
            inOutMap = {
                inField: outField if outField is not None and outField != "" else inField}

        for inf, outf in inOutMap.items():
            data = data.with_columns(data[inf].str.to_uppercase().alias(outf))

        return data

    @staticmethod
    def match_contains(data: pl.DataFrame, inField: str, match: str, outField: str = "") -> pl.DataFrame:
        """Match a value in a column and insert a boolean value in another column

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to take in
            matchValue (str): The value to match
            outField (str): The field to put the result in
        """

        return data.with_columns(data[inField].str.contains(match).alias(outField if outField is not None and outField != "" else inField))

    @staticmethod
    def join(
        data: pl.DataFrame, inFields: list, outField: str, separator: str = ""
    ) -> pl.DataFrame:
        """Join multiple columns together

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inFields (list): The fields to join
            outField (str): The field to put the result in
            separator (str, optional): The separator to use. Defaults to ''.

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        nCol = pl.concat_str(
            data[inFields], separator=separator).alias(outField)

        return data.with_columns(nCol.alias(outField))

    @staticmethod
    def join_listfield(
        data: pl.DataFrame, inField: str, outField: str, separator: str = ","
    ) -> pl.DataFrame:
        try:
            if isinstance(type(data[inField].dtype), type(pl.List)):
                data = data.with_columns(
                    data[inField].list.join(separator).alias(outField))
                return data

            data = data.with_columns(
                data[inField].arr.join(separator).alias(outField))
            return data

        except Exception:
            data = data.with_columns(data[inField].cast(
                pl.List).arr.join(separator).alias(outField))

            return data

    @staticmethod
    def split(
        data: pl.DataFrame, inField: str, outFields: list, separator: str = ""
    ) -> pl.DataFrame:
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
            .rename({f"field_{i}": fld for i, fld in enumerate(outFields)})
        )

        return data

    @staticmethod
    def split_into_listfield(
        data: pl.DataFrame, inField: str, outField: str = None, separator: str = ""
    ) -> pl.DataFrame:
        """Split a column into list field

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to split
            outField (str): The field to put the result into
            separator (str, optional): The separator to use. Defaults to ''.

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        __class__._validate_fields(data.columns, [inField])

        old_column = data[inField]
        new_column = old_column.str.split(separator).alias(outField or inField)

        data = data.with_columns(new_column)

        return data

    @staticmethod
    def quote(data: pl.DataFrame, inField: str, outField: str = None, quote: str = "'"):
        """Quotes the given column values"""

        tmpData = StringTransformer.staticfield(data, "ccQuoteFld", quote)

        new = StringTransformer.join(
            tmpData, ["ccQuoteFld", inField, "ccQuoteFld"], outField, separator=""
        )

        return new

    @staticmethod
    def replace(data: pl.DataFrame, search: str, replace: str, inField: str, outField: str = None):
        """Replace a section in a string with another section"""

        return data.with_columns(data[inField].str.replace(search, replace).alias(outField if outField is not None else inField))

    @staticmethod
    def replace_all(data: pl.DataFrame, search: str, replace: str, inField: str, outField: str = None):
        """Replace all matching sections in a string with another section"""

        return data.with_columns(data[inField].str.replace_all(search, replace).alias(outField if outField is not None else inField))

    @staticmethod
    def substring(data: pl.DataFrame, inField: str, outField: str = None, start: int = 0, length: int = None):
        """Returns a substring of the given string column"""

        return data.with_columns(data[inField].str.slice(start, length).alias(outField if outField is not None else inField))

    @staticmethod
    def add_prefix(data: pl.DataFrame, inField: str, prefix: str, outField: str = None):
        """Add a prefix to the given column"""

        return data.with_columns(pl.concat_str(pl.Series("ccTempField", [prefix] * len(data)), data[inField]).alias(outField if outField is not None else inField))

    @staticmethod
    def cast(data: pl.DataFrame, inField: str, outField: str = ""):
        """Cast a column to a different type"""

        if outField == "":
            outField = inField

        return data.with_columns(data[inField].cast(pl.Utf8).alias(outField))

    @staticmethod
    def hash(data: pl.DataFrame, inField: str, outField: str = ""):
        """Hash a column"""

        if outField == "":
            outField = inField

        return data.with_columns(data[inField].hash().cast(pl.Utf8).alias(outField))

    @staticmethod
    def to_object_id(data: pl.DataFrame, inField: str) -> pl.DataFrame:
        """Convert the given field to a bson.ObjectId

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to convert

        Returns:
            pl.DataFrame: The resulting DataFrame
        """

        data = data.with_columns(
            data[inField].map_elements(lambda oid: ObjectId(oid), return_dtype=pl.Object))

        return data

    @staticmethod
    def format(data: pl.DataFrame, inField: str, outField: str = "", format_string: str = "{value}"):
        """
        Formats the value in a string

        Args:
            data (pl.DataFrame): The dataFrame to modify
            inField (str): The field to format
            outField (str): The field to put the result in
            format_string (str): The format string using {value} as the placeholder for the value. Defaults to "{value}". Example: "_{value}_.

        Returns:
            pl.DataFrame: The resulting DataFrame
        """

        if not outField:
            outField = inField

        data = data.with_columns(
            data[inField].map_elements(lambda val: format_string.format(**{"value": val}), return_dtype=pl.Utf8).alias(outField))
        return data
