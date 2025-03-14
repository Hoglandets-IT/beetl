from typing import Any

import polars as pl
from polars import DataFrame, any_horizontal, struct, when

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
    if not source.schema == destination.schema:
        raise ValueError(
            "DataFrames must have the same schema (columns and data types)."
        )

    # Rename destination columns to have `_old` suffix (except key columns)
    destination_renamed = destination.rename(
        {
            col: f"{col}_old"
            for col in destination.columns
            if col not in comparison_columns
        }
    )

    # Perform an outer join on key columns
    merged = source.join(destination_renamed, on=comparison_columns, how="outer")

    # Identify changed columns (excluding key columns)
    diff_cols = [col for col in source.columns if col not in comparison_columns]
    diff_masks = [
        (merged[col] != merged[f"{col}_old"]).alias(f"diff_{col}") for col in diff_cols
    ]

    # Add diff indicators and filter rows with changes
    merged = merged.with_columns(diff_masks)
    merged = merged.filter(pl.any_horizontal(*[col for col in diff_masks]))

    # Create the "identifiers" column
    merged = merged.with_columns(pl.struct(comparison_columns).alias("identifiers"))

    # Function to dynamically create a dictionary-like struct without nulls

    # Create the "new" column containing updated values (only non-null fields)
    new_values = [
        pl.when(merged[f"diff_{col}"]).then(merged[col]).alias(col) for col in diff_cols
    ]
    merged = merged.with_columns(new_values.alias("new"))

    # Create the "old" column containing original values before update (only non-null fields)
    old_values = [
        pl.when(merged[f"diff_{col}"]).then(merged[f"{col}_old"]).alias(col)
        for col in diff_cols
    ]
    merged = merged.with_columns(old_values.alias("old"))

    # Drop unnecessary diff indicators and duplicate old columns
    drop_cols = [f"diff_{col}" for col in diff_cols] + [
        f"{col}_old" for col in diff_cols
    ]

    result = merged.drop(drop_cols)

    return ()
