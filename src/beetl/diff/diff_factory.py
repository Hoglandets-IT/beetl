from typing import Any

from polars import DataFrame

from .diff_model import Diff, DiffInsert, DiffRowData, DiffRowIdentifiers, DiffUpdate


def create_diff(
    name: str,
    inserts: DataFrame,
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
    updates = list(
        map(
            lambda row: create_insert(row, unique_columns, comparison_columns),
            u.to_dicts(),
        )
    )

    return Diff(name, inserts, deletes)


def create_insert(
    row: dict[str, Any], unique_columns: tuple[str, ...], data_columns: tuple[str, ...]
) -> DiffInsert:
    identifiers = DiffRowIdentifiers({col: row[col] for col in unique_columns})
    data = DiffRowData({col: row[col] for col in data_columns})
    return DiffInsert(identifiers, data)


def create_update() -> DiffUpdate:
    pass
