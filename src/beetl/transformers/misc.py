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
