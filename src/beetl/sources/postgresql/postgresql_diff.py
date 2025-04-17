"""Contains the diff configuration for the Postgresql source."""

from typing import Literal

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class PostgresDiffConfigArguments(SourceDiffConfigArguments):
    """Represents the source specifig config for the diff configuration of the Postgresql source."""

    table: str


class PostgresDiffArguments(SourceDiffArguments):
    """Represents the arguments for the diff configuration for the Postgresql source."""

    type: Literal["Postgresql"]
    config: PostgresDiffConfigArguments


class PostgresDiff(SourceDiff):
    """Represents the parsed diff configuration for the Postgresql source."""

    table: str

    def __init__(self, diff_config: PostgresDiffArguments) -> None:
        super().__init__(diff_config)
        self.table = diff_config.config.table
