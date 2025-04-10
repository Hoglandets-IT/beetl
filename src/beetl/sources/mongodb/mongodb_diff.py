from typing import Literal

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class MongodbDiffConfigArguments(SourceDiffConfigArguments):
    collection: str


class MongodbDiffArguments(SourceDiffArguments):
    type: Literal["Mongodb"]
    config: MongodbDiffConfigArguments


class MongodbDiff(SourceDiff):

    def __init__(self, diff_config: MongodbDiffArguments) -> None:
        super().__init__(diff_config)
