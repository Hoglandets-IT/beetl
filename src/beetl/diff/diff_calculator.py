"""Utilities related to calculating the diff between two datasets."""

from typing import Literal, Optional

import polars as pl
from polars import DataFrame

from ..constants import BEETL_COMPARISON_HASH_COLUMN_IDENTIFIER, RESERVED_IDENTIFIERS
from ..transformers import TransformerConfiguration, run_transformers
from ..typings import CASTABLE, ComparisonColumn
from .diff_model import Diff, DiffRow, DiffUpdate


class DiffCalculator:
    """
    DiffCalculator is a utility class designed to compute the differences
      between two datasets represented as DataFrames.
    It identifies changes based on unique and comparison
      columns specified during initialization.
    """

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
        columns: list[ComparisonColumn],
    ):
        self.sync_name = sync_name
        self.source = self._cast_columns_to_specified_types(
            self._initialize_columns_if_empty(source, columns), columns
        )
        self.destination = self._cast_columns_to_specified_types(
            self._initialize_columns_if_empty(destination, columns), columns
        )
        self.unique_columns = [col.name for col in columns if col.unique]
        self.comparison_columns = [
            col.name for col in columns if col.name not in self.unique_columns
        ]

        (
            self.inserts,
            self.updates_old,
            self.updates_new,
            self.updates_diff_mask,
            self.deletes,
        ) = self._calculate_diff()

    def _calculate_diff(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        columns_contain_list_column = any(
            [
                True
                for name in self.comparison_columns
                if isinstance(self.source.get_column(name).dtype, pl.List)
            ]
        )
        if columns_contain_list_column:
            raise ValueError(
                """Beetl does not support comparing list columns.
                Please remove them from the sync.comparisonColumns field.
                """
            )

        inserts = self._calculate_inserts(
            self.source, self.destination, self.unique_columns
        )
        deletes = self._calculate_deletes(
            self.source, self.destination, self.unique_columns
        )

        if self.diff_cannot_contain_any_updates():
            return (inserts, DataFrame(), DataFrame(), DataFrame(), deletes)

        updates_old, updates_new, updated_diff_mask = self._calculate_updates(
            self.source, self.destination, self.unique_columns, self.comparison_columns
        )

        return (inserts, updates_old, updates_new, updated_diff_mask, deletes)

    def diff_cannot_contain_any_updates(self):
        """
        Nothing can change if there are no comparison columns.
        The only thing that can change is the unique columns.
        That means that rows will be inserted or deleted, but not updated.

        returns:
            bool: True if there are no comparison columns.
        """
        return len(self.comparison_columns) == 0

    @staticmethod
    def _calculate_inserts(
        source: DataFrame,
        destination: DataFrame,
        unique_columns: tuple[str, ...],
    ) -> DataFrame:
        return source.join(destination, on=unique_columns, how="anti")

    @staticmethod
    def _calculate_deletes(
        source: DataFrame,
        destination: DataFrame,
        unique_columns: tuple[str, ...],
    ) -> DataFrame:
        return destination.join(source, on=unique_columns, how="anti")

    @staticmethod
    def _calculate_updates(
        source: DataFrame,
        destination: DataFrame,
        unique_columns: tuple[str, ...],
        comparison_columns: tuple[str, ...],
    ) -> tuple[DataFrame, DataFrame]:
        source_filtered = source.select(*unique_columns, *comparison_columns)
        destination_filtered = destination.select(*unique_columns, *comparison_columns)

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
        updated_rows = merged.filter(pl.any_horizontal(update_diff_mask_expression))
        diff_mask = updated_rows.select(
            *unique_columns, *[f"diff_{col}" for col in comparison_columns]
        )
        updated_rows_identifiers = updated_rows.select(*unique_columns)

        old = destination.join(updated_rows_identifiers, on=unique_columns, how="inner")
        new = source.join(updated_rows_identifiers, on=unique_columns, how="inner")

        return (old, new, diff_mask)

    @staticmethod
    def _initialize_columns_if_empty(
        dataframe: DataFrame, columns: tuple[ComparisonColumn, ...]
    ) -> DataFrame:
        if len(dataframe) == 0 and dataframe.width == 0:
            for col in columns:
                dataframe = dataframe.with_columns(pl.Series(col.name, dtype=col.type))
        return dataframe

    @staticmethod
    def _cast_columns_to_specified_types(
        dataframe: DataFrame, columns: tuple[ComparisonColumn, ...]
    ) -> DataFrame:
        for col in columns:
            if col.name in dataframe.columns and col.type in CASTABLE:
                dataframe = dataframe.with_columns(pl.col(col.name).cast(col.type))
        return dataframe

    def create_diff(
        self, transformers: Optional[list[TransformerConfiguration]] = None
    ) -> Diff:
        """
        Creates a diff object that represents the differences between two datasets.

        This method calculates the differences by comparing the deletes, inserts,
        and updates between the old and new datasets. It applies optional
        transformations to the datasets before performing the comparison.

        Args:
            transformers (Optional[list[TransformerConfiguration]]):
                A list of transformer configurations to apply to the datasets
                before computing the diff. Defaults to None.

        Returns:
            Diff: An object containing the differences, including updates, inserts,
            and deletes, between the old and new datasets.
        """
        diff_deletes = tuple(
            map(
                DiffRow,
                run_transformers(self.deletes, transformers).to_dicts(),
            )
        )
        diff_inserts = tuple(
            map(
                DiffRow,
                run_transformers(self.inserts, transformers).to_dicts(),
            )
        )

        add_hash_expression = (
            pl.struct(self.unique_columns)
            .hash()
            .alias(BEETL_COMPARISON_HASH_COLUMN_IDENTIFIER)
        )
        old_with_hash = self.updates_old.with_columns(add_hash_expression)
        new_with_hash = self.updates_new.with_columns(add_hash_expression)

        old_transformed = run_transformers(old_with_hash, transformers)
        new_transformed = run_transformers(new_with_hash, transformers)

        columns = old_transformed.columns

        old_renamed = old_transformed.rename(
            self._create_rename_mapping("old", columns)
        )
        new_renamed = new_transformed.rename(
            self._create_rename_mapping("new", columns)
        )

        updates = old_renamed.join(
            new_renamed, on=BEETL_COMPARISON_HASH_COLUMN_IDENTIFIER, how="inner"
        )

        updates = updates.with_columns(
            self._create_struct_column_expression("new", columns)
        )
        updates = updates.with_columns(
            self._create_struct_column_expression("old", columns)
        )

        updates = updates.select(["old", "new"])

        diff_updates = []
        for update_row in updates.to_dicts():
            old = DiffRow(update_row["old"])
            new = DiffRow(update_row["new"])
            diff_updates.append(DiffUpdate(old, new))

        return Diff(self.sync_name, diff_updates, diff_inserts, diff_deletes)

    @staticmethod
    def _create_rename_mapping(
        postfix: Literal["old", "new"], column_names: tuple[str, ...]
    ):
        return {
            col: f"{col}_{postfix}"
            for col in column_names
            if col not in RESERVED_IDENTIFIERS
        }

    @staticmethod
    def _create_struct_column_expression(
        postfix: Literal["old", "new"], columns: tuple[str, ...]
    ):
        return pl.struct(
            [
                pl.col(f"{col}_{postfix}").alias(col)
                for col in columns
                if col not in RESERVED_IDENTIFIERS
            ]
        ).alias(postfix)

    def get_create_update_delete_for_sync(
        self,
    ) -> tuple[DataFrame, DataFrame, DataFrame]:
        """
        Retrieves the DataFrames representing the inserts, updates, and deletes
        required for synchronization.

        Returns:
            tuple[DataFrame, DataFrame, DataFrame]: A tuple containing three DataFrames:
                - The first DataFrame represents the rows to be inserted.
                - The second DataFrame represents the rows to be updated.
                - The third DataFrame represents the rows to be deleted.
        """
        return (self.inserts, self.updates_new, self.deletes)
