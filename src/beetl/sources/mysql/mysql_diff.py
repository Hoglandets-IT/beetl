from typing import Literal

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class MysqlDiffConfigArguments(SourceDiffConfigArguments):
    table: str


class MysqlDiffArguments(SourceDiffArguments):
    type: Literal["Mysql"]
    config: MysqlDiffConfigArguments


class MysqlDiff(SourceDiff):
    table: str

    def __init__(self, diff_config: MysqlDiffArguments) -> None:
        super().__init__(diff_config)
        self.table = diff_config.config.table
