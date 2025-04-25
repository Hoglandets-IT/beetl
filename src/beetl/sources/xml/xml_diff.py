from typing import Literal

from ..interface.interface_diff import (
    SourceDiff,
    SourceDiffArguments,
    SourceDiffConfigArguments,
)


class XmlDiffConfigArguments(SourceDiffConfigArguments):
    pass


class XmlDiffArguments(SourceDiffArguments):
    type: Literal["Xml"]
    config: XmlDiffConfigArguments


class XmlDiff(SourceDiff):

    def __init__(self, diff_config: XmlDiffArguments) -> None:
        super().__init__(diff_config)
