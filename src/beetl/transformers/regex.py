import polars as pl
from .interface import TransformerInterface, register_transformer_class


@register_transformer_class("regex")
class RegexTransformer(TransformerInterface):
    @staticmethod
    def replace(
        data: pl.DataFrame,
        query: str,
        replace: str,
        inField: str,
        outField: str = None,
        maxN: int = -1,
    ) -> pl.DataFrame:
        """Run a regex replace operation on the given field

        Args:
            data (pl.DataFrame): The data to be modified
            query (str): The regex query to run
            replace (str): The replacement string
            inField (str): The field to work on
            outField (str, optional): The field to add/replace. Defaults to None.

        Returns:
            pl.DataFrame: Dataframe with the modified column
        """

        data = data.with_columns(
            data[inField]
            .str.replace(query, replace, n=maxN)
            .alias(outField if outField is not None else inField)
        )

        return data
