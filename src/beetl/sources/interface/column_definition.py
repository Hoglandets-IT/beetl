from dataclasses import dataclass

import polars as pl
from polars import DataType


@dataclass
class ColumnDefinition:
    """The definition of a column in a dataset.
    Name: Name of column
    Type: Polars data type
    Unique: Whether it is unique
    Skip Update: Whether to skip updating this field when inserting/updating source
    Custom Options: Custom options for some providers
    """

    name: str
    type: DataType
    unique: bool = False
    skip_update: bool = False
    custom_options: dict = None

    def __post_init__(self) -> None:
        """Get the actual data type from Polars"""
        self.type = getattr(pl, self.type)
