"""Classes related to comparison column definition."""

from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass
class ComparisonColumn:
    """
    A class representing a column used for comparison, with attributes for its name, data type, and uniqueness.

    Attributes:
      name (str): The name of the column.
      type (pl.DataType): The data type of the column, determined dynamically from a string representation.
      unique (bool): Indicates whether the column values are unique. Defaults to False.
    """

    name: str
    type: pl.DataType
    unique: bool = False

    def __init__(self, name: str, type: Any, unique: bool = False) -> None:
        self.name, self.unique = name, unique
        self.type = getattr(pl, type)
