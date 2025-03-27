from copy import deepcopy
from typing import Any

import polars as pl
from polars import DataFrame

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
    unique_columns: tuple[str, ...],
    comparison_columns: tuple[str, ...],
) -> Diff:
    deletes = create_deletes(source, destination, unique_columns)
    inserts = create_inserts(source, destination, unique_columns, comparison_columns)
    updates = create_updates(source, destination, unique_columns, comparison_columns)

    return Diff(name, updates, inserts, deletes)


def create_inserts(
    source: DataFrame,
    destination: DataFrame,
    unique_columns: tuple[str, ...],
    comparision_columns: tuple[str, ...],
) -> tuple[DiffInsert, ...]:
    inserts = source.join(destination, on=unique_columns, how="anti").select(
        *unique_columns, *comparision_columns
    )
    inserts = inserts.select(
        pl.struct(unique_columns).alias("identifiers"),
        pl.struct(comparision_columns).alias("data"),
    )
    return tuple(map(DiffInsert, inserts.to_dicts()))


def create_deletes(
    source: DataFrame, destination: DataFrame, unique_columns: tuple[str, ...]
) -> tuple[DiffDelete, ...]:
    deletes = destination.join(source, on=unique_columns, how="anti").select(
        *unique_columns
    )
    deletes = deletes.select(pl.struct(unique_columns).alias("identifiers"))
    return tuple(map(DiffDelete, deletes.to_dicts()))


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

    destination_renamed_as_old = destination.rename(
        {col: f"{col}_old" for col in destination.columns if col not in unique_columns}
    )

    merged = source.join(destination_renamed_as_old, on=unique_columns, how="outer")

    diff_masks = [
        (merged[col] != merged[f"{col}_old"]).alias(f"diff_{col}")
        for col in comparison_columns
    ]

    merged = merged.with_columns(diff_masks)
    # TODO: Here it would be possible to split the merged dataframe into two
    #   One with the old, one with the new
    #   New can be passed back as the data to be updated
    #   Then old and new can pass through the transformers and be merged again to become the update diff
    merged = merged.filter(pl.any_horizontal(*[col for col in diff_masks]))

    merged = merged.with_columns(pl.struct(unique_columns).alias("identifiers"))

    new_values = [
        pl.when(pl.col(f"diff_{col}")).then(pl.col(col)).alias(col)
        for col in comparison_columns
    ]
    merged = merged.with_columns(pl.struct(new_values).alias("new"))

    old_values = [
        pl.when(merged[f"diff_{col}"]).then(merged[f"{col}_old"]).alias(col)
        for col in comparison_columns
    ]
    merged = merged.with_columns(pl.struct(old_values).alias("old"))

    merged = merged.select(["identifiers", "old", "new"])

    updates = []
    for updateRow in merged.to_dicts():
        identifiers = DiffRowIdentifiers(updateRow["identifiers"])
        old = DiffRowData(updateRow["old"])
        new = DiffRowData(updateRow["new"])
        updates.append(DiffUpdate(identifiers, old, new))

    return updates
