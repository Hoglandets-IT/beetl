"""Contains types and helpers for types used in the Polars package."""

from typing import Literal

import polars as pl

CASTABLE = (
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
    pl.Float32,
    pl.Float64,
    pl.Boolean,
    pl.Utf8,
    pl.Object,
    # Does not quite work
    # pl.Binary,
)

PolarTypeLiterals = Literal[
    "Decimal",
    "Float32",
    "Float64",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Date",
    "Datetime",
    "Duration",
    "Time",
    "List",
    "Array",
    "Categorical",
    "Enum",
    "Struct",
    "Object",
    "Utf8",
    "String",
]


def get_polar_type_from_literal(type_as_string: str) -> pl.DataType:
    """
    Retrieve a Polars data type from its string literal representation.

    Args:
        type (str): The string literal representing the Polars data type.

    Returns:
        pl.DataType: The corresponding Polars data type.

    Raises:
        AttributeError: If the provided type string does not match any attribute in the Polars module.
    """
    return getattr(pl, type_as_string)


PolarTypeOverridesParameters = dict[str, PolarTypeLiterals]


class PolarTypeOverrides(dict[str, pl.DataType]):
    def __init__(self, overrides: PolarTypeOverridesParameters):
        super().__init__(overrides)
        for key, value in overrides.items():
            self[key] = get_polar_type_from_literal(value)
