from typing import Literal

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class FakerDiffConfigArguments(SourceDiffConfigArguments):
    pass


class FakerDiffArguments(SourceDiffArguments):
    type: Literal["Faker"]
    config: FakerDiffConfigArguments


class FakerDiff(SourceDiff):

    def __init__(self, diff_config: FakerDiffArguments) -> None:
        super().__init__(diff_config)
