from copy import deepcopy
from typing import Any
from uuid import uuid4

import polars as pl
from polars import DataFrame, Expr, Schema, any_horizontal, struct, when

from .diff_model import Diff, DiffInsert, DiffRowData, DiffRowIdentifiers, DiffUpdate


def create_diff(
    name: str,
    source: DataFrame,
    destination: DataFrame,
    inserts: DataFrame,
    updates: DataFrame,
    deletes: DataFrame,
    unique_columns: tuple[str, ...],
    comparison_columns: tuple[str, ...],
) -> Diff:
    deletes = deletes.select(unique_columns).to_dicts()
    inserts = list(
        map(
            lambda row: create_insert(row, unique_columns, comparison_columns),
            inserts.to_dicts(),
        )
    )
    updates = create_updates(source, destination, unique_columns, comparison_columns)

    return Diff(name, updates, inserts, deletes)


def remove_nulls_from_struct(struct_col: pl.Expr) -> pl.Expr:
    """Removes null values from a struct by keeping only non-null fields."""
    return struct_col.struct.rename_fields(
        [
            field
            for field in struct_col.struct.fields
            if not struct_col.struct.field(field).is_null().all()
        ]
    )


def create_insert(
    row: dict[str, Any], unique_columns: tuple[str, ...], data_columns: tuple[str, ...]
) -> DiffInsert:
    identifiers = DiffRowIdentifiers({col: row[col] for col in unique_columns})
    data = DiffRowData({col: row[col] for col in data_columns})
    return DiffInsert(identifiers, data)


def create_updates(
    source: DataFrame,
    destination: DataFrame,
    unique_columns: tuple[str, ...],
    comparison_columns: tuple[str, ...],
) -> tuple[DiffUpdate, ...]:
    # TODO: Continue here
    source = deepcopy(source).select(*unique_columns, *comparison_columns)
    destination = deepcopy(destination).select(*unique_columns, *comparison_columns)
    if not source.schema == destination.schema:
        raise ValueError(
            "DataFrames must have the same schema (columns and data types)."
        )

    # Rename destination columns to have `_old` suffix (except key columns)
    destination_renamed = destination.rename(
        {col: f"{col}_old" for col in destination.columns if col not in unique_columns}
    )

    # Perform an outer join on key columns
    merged = source.join(destination_renamed, on=unique_columns, how="outer")

    # Identify changed columns (excluding key columns)
    diff_masks = [
        (merged[col] != merged[f"{col}_old"]).alias(f"diff_{col}")
        for col in comparison_columns
    ]

    # Add diff indicators and filter rows with changes
    merged = merged.with_columns(diff_masks)
    merged = merged.filter(pl.any_horizontal(*[col for col in diff_masks]))

    # Create the "identifiers" column
    merged = merged.with_columns(pl.struct(unique_columns).alias("identifiers"))

    # Function to dynamically create a dictionary-like struct without nulls

    # Create the "new" column containing updated values (only non-null fields)
    new_values = [
        pl.when(pl.col(f"diff_{col}") & pl.col(col).is_not_null())
        .then(pl.col(col))
        .alias(col)
        for col in comparison_columns
    ]
    merged = merged.with_columns(pl.struct(new_values).alias("new"))

    # Create the "old" column containing original values before update (only non-null fields)
    old_values = [
        pl.when(merged[f"diff_{col}"]).then(merged[f"{col}_old"]).alias(col)
        for col in comparison_columns
    ]
    merged = merged.with_columns(pl.struct(old_values).alias("old"))

    # Drop unnecessary diff indicators and duplicate old columns

    merged = merged.select(["identifiers", "old", "new"])

    ## TODO: Continue here trying to remove nulls from the structs

    #    def func(struct_field: Schema, base_expr: Expr, base_name: str):
    #        if base_name == "identifiers":
    #            return []
    #        expr = []
    #        for field in struct_field.fields:
    #            print(field.name)
    #            expr.append(
    #                pl.when(base_expr.struct.field(field.name).is_not_null()).then(
    #                    base_expr.struct.field(field.name).alias(f"{field.name}")
    #                )
    #            )
    #        return expr

    #    exprs = []

    #    for col in merged.columns:
    #        if col in unique_columns:
    #            continue
    #        exprs.extend(func(merged.schema[col], pl.col(col), col))

    #    result = merged.select(*exprs)
    #    def remove_nulls_from_struct(struct_col: pl.Expr, schema: Schema) -> pl.Expr:
    #        """Removes null values from a struct by keeping only non-null fields."""
    #        fields = schema.fields
    #        non_null_fields = [
    #            pl.when(struct_col.struct.field(field.name).is_not_null())
    #            .then(struct_col.struct.field(field.name))
    #            .otherwise(struct_col.struct.drop_nulls())
    #            .alias(field.name)
    #            for field in fields
    #        ]
    #        return pl.struct(non_null_fields)

    #    # Remove null properties from the struct
    #    result = merged.with_columns(
    #        remove_nulls_from_struct(pl.col("old"), merged.schema["old"]).alias(
    #            "filtered_data"
    #        )
    #    )
    print("########################")
    print(merged)

    return ()
