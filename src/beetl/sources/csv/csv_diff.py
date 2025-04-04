from typing import Literal

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class CsvDiffConfigArguments(SourceDiffConfigArguments):
    path: str


class CsvDiffArguments(SourceDiffArguments):
    type: Literal["Csv"]
    config: CsvDiffConfigArguments


class CsvDiff(SourceDiff):
    path: str

    def __init__(self, diff_config: CsvDiffArguments) -> None:
        self.path = diff_config.config.path
