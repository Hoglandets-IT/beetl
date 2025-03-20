from copy import deepcopy
from typing import Any
from uuid import uuid4

import polars as pl
from polars import DataFrame, Expr, Schema, any_horizontal, struct, when

from .diff_model import (
    Diff,
    DiffDelete,
    DiffInsert,
    DiffRowData,
    DiffRowIdentifiers,
    DiffUpdate,
)


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
    deletes = tuple(
        map(lambda row: create_delete(row), deletes.select(unique_columns).to_dicts())
    )
    inserts = list(
        map(
            lambda row: create_insert(row, unique_columns, comparison_columns),
            inserts.to_dicts(),
        )
    )
    updates = create_updates(source, destination, unique_columns, comparison_columns)

    return Diff(name, updates, inserts, deletes)


def create_insert(
    row: dict[str, Any], unique_columns: tuple[str, ...], data_columns: tuple[str, ...]
) -> DiffInsert:
    identifiers = DiffRowIdentifiers({col: row[col] for col in unique_columns})
    data = DiffRowData({col: row[col] for col in data_columns})
    return DiffInsert(identifiers, data)


def create_delete(row: dict[str, Any]) -> DiffDelete:
    return DiffDelete(DiffRowIdentifiers(row))


def create_updates(
    source: DataFrame,
    destination: DataFrame,
    unique_columns: tuple[str, ...],
    comparison_columns: tuple[str, ...],
) -> tuple[DiffUpdate, ...]:
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

    print("########################")
    print(merged)

    dicts = merged.to_dicts()
    updates = []
    for dict in dicts:
        identifiers = DiffRowIdentifiers(dict["identifiers"])
        old = DiffRowData(dict["old"])
        new = DiffRowData(dict["new"])
        updates.append(DiffUpdate(identifiers, old, new))

    return updates
