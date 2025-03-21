from typing import Annotated, Literal

from pydantic import Field

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class SqlserverDiffConfigArguments(SourceDiffConfigArguments):
    table: str


class SqlserverDiffArguments(SourceDiffArguments):
    source_type: Literal["Sqlserver"]
    config: SqlserverDiffConfigArguments


class SqlserverDiff(SourceDiff):
    table: str

    def __init__(self, diff_config: SqlserverDiffArguments) -> None:
        self.table = diff_config.config.table
