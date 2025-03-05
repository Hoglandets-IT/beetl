from typing import Literal

from ..interface.interface_diff import SourceDiffConfigArguments


class SqlserverDiffConfig(SourceDiffConfigArguments):
    pass


class SqlserverDiffArguments(SourceDiffConfigArguments):
    type: Literal["Sqlserver"]
    config: SqlserverDiffConfig
