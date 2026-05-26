from uuid import NAMESPACE_DNS, uuid5

import polars as pl
from .interface import TransformerInterface, register_transformer_class


def dn_to_samaccountname(dn):
    return dn.split(",")[0].split("=")[1]


@register_transformer_class("misc")
class MiscTransformer(TransformerInterface):

    @staticmethod
    def sam_from_dn(
        data: pl.DataFrame, inField: str, outField: str = None
    ) -> pl.DataFrame:
        """Get the SAM account name from a distinguished name

        Args:
            data (pl.DataFrame): The data to be modified
            inField (str): The field to work on
            outField (str, optional): The field to add/replace. Defaults to None.

        Returns:
            pl.DataFrame: Dataframe with the modified column
        """

        data = data.with_columns(
            data[inField]
            .str.splitn(",", 2)
            .struct.unnest()["field_0"]
            .str.splitn("=", 2)
            .struct.unnest()["field_1"]
            .alias(outField if outField is not None else inField)
        )

        return data
    
    @staticmethod
    def lookup(data: pl.DataFrame,
        inField: str,
        outField: str,
        mapping: dict[str, str],
        caseInsensitive: bool = True,
    ) -> pl.DataFrame:
        """Maps values from one column to another using a lookup dictionary.

        Args:
            data (pl.DataFrame): The data to be modified
            inField (str): Source field containing values to map
            outField (str): Target field to create or replace
            mapping (dict[str, str]): Key/value mapping dictionary
            caseInsensitive (bool, optional): Perform case insensitive matching.
                Defaults to True.

        Returns:
            pl.DataFrame: Dataframe with mapped values in outField
        """

        __class__._validate_fields(data.columns, inField)

        lookup_map = {
        (str(k).upper() if caseInsensitive else str(k)): v
        for k, v in mapping.items()
        }
        expr = pl.col(inField).cast(pl.Utf8)
        if caseInsensitive:
            expr = expr.str.to_uppercase()

        return data.with_columns(
            expr.replace(lookup_map).alias(outField)
    )

    @staticmethod
    def guid_from_fields(
        data: pl.DataFrame,
        inFields: list[str],
        outField: str,
        separator: str = "|",
        namespace: str = "beetl",
    ) -> pl.DataFrame:
        
        """Creates a deterministic UUIDv5 from one or more fields.

         Args:
             data (pl.DataFrame): The data to be modified
             inFields (list[str]): Fields used to generate the GUID
             outField (str): Target field to create or replace
             separator (str, optional): Separator used when concatenating values.
                 Defaults to "|".
             namespace (str, optional): Namespace prefix used in UUID generation.
                 Defaults to "beetl".

         Returns:
             pl.DataFrame: Dataframe with generated GUID values in outField
        """
        __class__._validate_fields(data.columns, inFields)

        def make_guid(values: dict) -> str:
            parts = ["" if values[field] is None else str(values[field]) for field in inFields]
            return str(uuid5(NAMESPACE_DNS, namespace + ":" + separator.join(parts)))

        return data.with_columns(
            pl.struct(inFields)
            .map_elements(make_guid, return_dtype=pl.Utf8)
            .alias(outField)
    )
