from typing import Any
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
            return concat_and_sha("-", toplevel, *st.values())

        new = data.with_columns(
            pl.struct(inFields).map_elements(make_code).alias(outField)
        )

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
    def relations(
        data: pl.DataFrame, field_relations: list[dict[str, Any]]
    ) -> pl.DataFrame:
        """Insert data into iTop

        Args:
            data (pl.DataFrame): The dataFrame to modify
            connection (SourceInterface): The connection to the source
            configuration (SourceInterfaceConfiguration): The configuration for the source

        Returns:
            pl.DataFrame: The resulting DataFrame
        """

        if not field_relations:
            return data

        transformed = data.clone()
        for field in data.get_columns():
            field_relation = next(
                (
                    relation
                    for relation in field_relations
                    if relation["source_field"] == field.name
                ),
                None,
            )
            if not field_relation:
                continue

            source_field = field_relation["source_field"]
            source_comparison_field = field_relation["source_comparison_field"]
            target_class = field_relation["target_class"]
            target_comparison_field = field_relation["target_comparison_field"]
            use_like_operator = field_relation.get("use_like_operator", False)

            if (
                not source_field
                or not source_comparison_field
                or not target_class
                or not target_comparison_field
            ):
                raise ValueError(
                    "One of the field_relations config values for the itop.relations transformer is missing. All fields except use_like_operator are required."
                )

            try:
                comparison_operator = "LIKE" if use_like_operator else "="

                query = (
                    f"SELECT {target_class}"
                    + f" WHERE {target_comparison_field} {comparison_operator} "
                )

                transformed = transformed.with_columns(
                    transformed[source_comparison_field]
                    .map_elements(
                        lambda x: (
                            query + f"'{x}'" if x is not None and x != "" else None
                        )
                    )
                    .alias(source_field)
                )
            except KeyError as e:
                raise Exception("Error: The key definition is not valid") from e

        return transformed
