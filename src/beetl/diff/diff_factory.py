import gc
from typing import Optional

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


class DiffCalculator:
    sync_name: str
    unique_columns: tuple[str, ...]
    comparison_columns: tuple[str, ...]
    source: DataFrame
    destination: DataFrame

    updates_old: Optional[DataFrame] = None
    updates_new: Optional[DataFrame] = None
    updates_diff_mask: Optional[DataFrame] = None

    inserts: Optional[DataFrame] = None
    deletes: Optional[DataFrame] = None

    def __init__(
        self,
        sync_name: str,
        source: DataFrame,
        destination: DataFrame,
        unique_columns: tuple[str, ...],
        comparison_columns: tuple[str, ...],
    ):
        self.sync_name = sync_name
        self.source = source
        self.destination = destination
        self.unique_columns = unique_columns
        self.comparison_columns = comparison_columns

        (
            self.inserts,
            self.updates_old,
            self.updates_new,
            self.updates_diff_mask,
            self.deletes,
        ) = self.calculate_diff()

    def calculate_diff(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        inserts = self.calculate_inserts(
            self.source, self.destination, self.unique_columns, self.comparison_columns
        )
        updates_old, updates_new, updated_diff_mask = self.calculate_updates(
            self.source, self.destination, self.unique_columns, self.comparison_columns
        )
        deletes = self.calculate_deletes(
            self.source, self.destination, self.unique_columns, self.comparison_columns
        )

        return (inserts, updates_old, updates_new, updated_diff_mask, deletes)

    @staticmethod
    def calculate_inserts(
        source: DataFrame,
        destination: DataFrame,
        unique_columns: tuple[str, ...],
        comparision_columns: tuple[str, ...],
    ) -> DataFrame:
        return source.join(destination, on=unique_columns, how="anti").select(
            *unique_columns, *comparision_columns
        )

    @staticmethod
    def calculate_deletes(
        source: DataFrame,
        destination: DataFrame,
        unique_columns: tuple[str, ...],
        comparison_columns: tuple[str, ...],
    ) -> DataFrame:
        return destination.join(source, on=unique_columns, how="anti").select(
            *unique_columns, *comparison_columns
        )

    @staticmethod
    def calculate_updates(
        source: DataFrame,
        destination: DataFrame,
        unique_columns: tuple[str, ...],
        comparison_columns: tuple[str, ...],
    ) -> tuple[DataFrame, DataFrame]:
        source_filtered = source.select(*unique_columns, *comparison_columns)
        destination_filtered = destination.select(*unique_columns, *comparison_columns)
        if not source_filtered.schema == destination_filtered.schema:
            raise ValueError(
                "DataFrames must have the same schema (columns and data types)."
            )

        destination_renamed_as_old = destination_filtered.rename(
            {
                col: f"{col}_old"
                for col in destination_filtered.columns
                if col not in unique_columns
            }
        )

        merged = source_filtered.join(
            destination_renamed_as_old, on=unique_columns, how="outer"
        )

        update_diff_mask_expression = [
            (merged[col] != merged[f"{col}_old"]).alias(f"diff_{col}")
            for col in comparison_columns
        ]

        merged = merged.with_columns(update_diff_mask_expression)
        updated_rows = merged.filter(
            pl.any_horizontal(*[col for col in update_diff_mask_expression])
        )
        diff_mask = updated_rows.select(
            *unique_columns, *[f"diff_{col}" for col in comparison_columns]
        )
        updated_rows_identifiers = updated_rows.select(*unique_columns)

        old = destination_filtered.join(
            updated_rows_identifiers, on=unique_columns, how="inner"
        ).select(*unique_columns, *comparison_columns)
        new = source_filtered.join(
            updated_rows_identifiers, on=unique_columns, how="inner"
        ).select(*unique_columns, *comparison_columns)

        return (old, new, diff_mask)

    def create_diff(
        self,
    ) -> Diff:
        diff_deletes = tuple(
            map(
                DiffDelete,
                self.deletes.select(
                    pl.struct(self.unique_columns).alias("identifiers")
                ).to_dicts(),
            )
        )
        diff_inserts = tuple(
            map(
                DiffInsert,
                self.inserts.select(
                    pl.struct(self.unique_columns).alias("identifiers"),
                    pl.struct(self.comparison_columns).alias("data"),
                ).to_dicts(),
            )
        )

        update_old_columns_renamed_as_old = self.updates_old.rename(
            {
                col: f"{col}_old"
                for col in self.updates_old.columns
                if col not in self.unique_columns
            }
        )

        updates = self.updates_new.join(
            update_old_columns_renamed_as_old, on=self.unique_columns, how="inner"
        )
        updates = updates.join(
            self.updates_diff_mask, on=self.unique_columns, how="inner"
        )
        updates = updates.with_columns(
            pl.struct(self.unique_columns).alias("identifiers")
        )

        new_values_expression = [
            pl.when(pl.col(f"diff_{col}")).then(pl.col(col)).alias(col)
            for col in self.comparison_columns
        ]
        updates = updates.with_columns(pl.struct(new_values_expression).alias("new"))

        old_values_expression = [
            pl.when(updates[f"diff_{col}"]).then(updates[f"{col}_old"]).alias(col)
            for col in self.comparison_columns
        ]
        updates = updates.with_columns(pl.struct(old_values_expression).alias("old"))

        updates = updates.select(["identifiers", "old", "new"])

        diff_updates = []
        for updateRow in updates.to_dicts():
            identifiers = DiffRowIdentifiers(updateRow["identifiers"])
            old = DiffRowData(updateRow["old"])
            new = DiffRowData(updateRow["new"])
            diff_updates.append(DiffUpdate(identifiers, old, new))

        return Diff(self.sync_name, diff_updates, diff_inserts, diff_deletes)

    def get_create_update_delete_for_sync(
        self,
    ) -> tuple[DataFrame, DataFrame, DataFrame]:
        return (self.inserts, self.updates_new, self.deletes)


def create_diff(
    name: str,
    source: DataFrame,
    destination: DataFrame,
    unique_columns: tuple[str, ...],
    comparison_columns: tuple[str, ...],
) -> Diff:
    return Diff(name, DataFrame(), DataFrame(), DataFrame())
