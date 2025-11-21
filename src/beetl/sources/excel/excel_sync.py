from typing import Annotated, Literal, Optional

from pydantic import Field

from ...typings import PolarTypeOverrides, PolarTypeOverridesParameters
from ..interface import SourceSync, SourceSyncArguments


class ExcelSyncArguments(SourceSyncArguments):
    types: Annotated[Optional[PolarTypeOverridesParameters], Field(default=None)]
    sheet_name: Annotated[Optional[str], Field(default=None)]
    type: Annotated[Literal["Excel"], Field(default="Excel")] = "Excel"


class ExcelSync(SourceSync):
    """The configuration class used for Excel file sources"""

    types: Optional[PolarTypeOverrides] = None
    sheet_name: Optional[str] = None

    def __init__(self, arguments: ExcelSyncArguments):
        super().__init__(arguments)

        self.sheet_name = arguments.sheet_name

        if arguments.types is not None:
            self.types = PolarTypeOverrides(arguments.types)
