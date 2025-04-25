from typing import Literal

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class ExcelDiffConfigArguments(SourceDiffConfigArguments):
    pass


class ExcelDiffArguments(SourceDiffArguments):
    type: Literal["Excel"]
    config: ExcelDiffConfigArguments


class ExcelDiff(SourceDiff):

    def __init__(self, diff_config: ExcelDiffArguments) -> None:
        super().__init__(diff_config)
