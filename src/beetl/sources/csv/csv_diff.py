from typing import Literal

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class CsvDiffConfigArguments(SourceDiffConfigArguments):
    pass


class CsvDiffArguments(SourceDiffArguments):
    type: Literal["Csv"]
    config: CsvDiffConfigArguments


class CsvDiff(SourceDiff):

    def __init__(self, diff_config: CsvDiffArguments) -> None:
        super().__init__(diff_config)
