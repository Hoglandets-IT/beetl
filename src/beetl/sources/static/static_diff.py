from typing import Annotated, Literal

from pydantic import Field

from ..interface.interface_diff import SourceDiff, SourceDiffArguments


class StaticDiffArguments(SourceDiffArguments):
    type: Annotated[Literal["Static"], Field(default="Static")] = "Static"


class StaticDiff(SourceDiff):
    pass
