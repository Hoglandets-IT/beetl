from typing import Literal

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class SqlserverDiffConfigArguments(SourceDiffConfigArguments):
    pass


class SqlserverDiffArguments(SourceDiffArguments):
    type: Literal["Sqlserver"]
    config: SqlserverDiffConfigArguments


class SqlserverDiff(SourceDiff):
    pass
