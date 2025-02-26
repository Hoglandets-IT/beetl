from typing import Literal

import polars as pl

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


def get_polar_type_from_literal(type: str) -> pl.DataType:
    return getattr(pl, type)


PolarTypeOverridesParameters = dict[str, PolarTypeLiterals]


class PolarTypeOverrides(dict[str, pl.DataType]):
    def __init__(self, overrides: PolarTypeOverridesParameters):
        super().__init__(overrides)
        for key, value in overrides.items():
            self[key] = get_polar_type_from_literal(value)
