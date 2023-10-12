from .interface import (
    register_transformer_class,
    TransformerInterface,
)
import polars as pl
from hashlib import sha1


def concat_and_sha(separator: str = "-", *args):
    retn = separator.join((x.lower() for x in args if x is not None))
    return sha1(retn.encode("utf-8")).hexdigest()


def never_none(value):
    if value is None or value == "":
        return "-"
    return value


@register_transformer_class("itop")
class ItopTransformer(TransformerInterface):
    @staticmethod
    def orgcode(data: pl.DataFrame, inFields: list, outField: str, toplevel: str):
        """Takes a number of columns, joins them and hashes the result"""

        def make_code(st):
            concat_and_sha("-", toplevel, *st.values())

        new = data.with_columns(pl.struct(inFields).apply(make_code).alias(outField))

        return new

    @staticmethod
    def orgtree(
        data: pl.DataFrame,
        treeFields: list,
        toplevel: str,
        name_field: str = "name",
        path_field: str = "orgpath",
        code_field: str = "code",
        parent_field: str = "parent_code",
    ):
        """Takes a number of columns and joins then together as an organization tree"""
        output = [
            {
                "status": "active",
                name_field: toplevel,
                path_field: f"Top->{toplevel}",
                code_field: concat_and_sha("-", toplevel),
                parent_field: None,
            }
        ]

        for i in range(len(treeFields)):
            for lvl in data[treeFields[: i + 1]].unique().iter_rows():
                lvls = [toplevel] + [never_none(x) for x in lvl]

                output.append(
                    {
                        "status": "active",
                        name_field: never_none(lvls[-1]),
                        path_field: "Top->" + "->".join(lvls),
                        code_field: concat_and_sha("-", *lvls),
                        parent_field: concat_and_sha("-", *lvls[:-1]),
                    }
                )

        return pl.DataFrame(output)

    @staticmethod
    def relations(data: pl.DataFrame, **kwargs) -> pl.DataFrame:
        """Insert data into iTop

        Args:
            data (pl.DataFrame): The dataFrame to modify
            connection (SourceInterface): The connection to the source
            configuration (SourceInterfaceConfiguration): The configuration for the source

        Returns:
            pl.DataFrame: The resulting DataFrame
        """
        if not kwargs.get("sync", False):
            raise Exception(
                "Error: include_sync option is not set for transformer itop.relations"
            )

        if kwargs["sync"].destination.source_configuration.has_foreign:
            transformed = data.clone()
            for field in kwargs["sync"].destination.source_configuration.columns:
                fk_def = getattr(field, "custom_options", {})
                if fk_def is not None:
                    fk_def = fk_def.get("itop", None)
                    if fk_def is not None:
                        try:
                            query = (
                                f'SELECT {fk_def["target_class"]}'
                                + f' WHERE {fk_def["reconciliation_key"]} = '
                            )

                            transformed = transformed.with_columns(
                                transformed[fk_def["comparison_field"]]
                                .apply(
                                    lambda x: query + f"'{x}'"
                                    if x is not None and x != ""
                                    else None
                                )
                                .alias(field.name)
                            )
                        except KeyError as e:
                            raise Exception(
                                "Error: The key definition is not valid"
                            ) from e

            return transformed

        return data
