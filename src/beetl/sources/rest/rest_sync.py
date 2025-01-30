from typing import Annotated, Dict, Literal, Optional

from pydantic import ConfigDict, Field

from ...validation import ValidationBaseModel
from ..interface import SourceSync, SourceSyncArguments


class RestResponse:
    length: str
    items: str

    def __init__(self, length: str, items: str):
        self.length = length
        self.items = items


class BodyOptions(ValidationBaseModel):
    accepts_multiple: Annotated[bool, Field(default=False)]
    object_path: Annotated[str, Field(default="")]


class PaginationSettings(ValidationBaseModel):
    enabled: Annotated[bool, Field(default=False)]
    pageSize: Annotated[int, Field(default=10)]
    startPage: Annotated[int, Field(default=0)]
    pageSizeQuery: Annotated[str, Field(default="pageSize")]
    pageQuery: Annotated[str, Field(default="page")]
    totalQuery: Annotated[str, Field(default="page.count")]
    queryIn: Annotated[str, Field(default="query")]
    totalQueryIn: Annotated[str, Field(default="body")]


class RestRequest(ValidationBaseModel):
    # Arbitrary types is needed since the response is placed in the request when made but not present when intializing the request.
    model_config = ConfigDict(
        extra="forbid", ignored_types=(RestResponse,), arbitrary_types_allowed=True
    )

    path: Annotated[str, Field(default=None)]
    query: Annotated[Dict[str, str], Field(default={})]
    method: Annotated[str, Field(default="GET")]
    pagination: Annotated[Optional[PaginationSettings], Field(default=None)]
    body_type: Annotated[str, Field(default="application/json")]
    body: Annotated[Optional[str], Field(default=None)]
    body_options: Annotated[Optional[BodyOptions], Field(default=None)]
    return_type: Annotated[str, Field(default="application/json")]
    response: RestResponse = None


class RestSyncArguments(SourceSyncArguments):
    listRequest: RestRequest
    createRequest: Annotated[Optional[RestRequest], Field(default=None)]
    updateRequest: Annotated[Optional[RestRequest], Field(default=None)]
    deleteRequest: Annotated[Optional[RestRequest], Field(default=None)]
    type: Annotated[Literal["Rest"], Field(defauls="Rest")] = "Rest"


class RestSync(SourceSync):
    """The configuration class used for MySQL sources"""

    listRequest: RestRequest
    createRequest: Optional[RestRequest]
    updateRequest: Optional[RestRequest]
    deleteRequest: Optional[RestRequest]

    def __init__(self, arguments: RestSyncArguments):
        super().__init__(arguments)
        self.listRequest = arguments.listRequest
        self.createRequest = arguments.createRequest
        self.updateRequest = arguments.updateRequest
        self.deleteRequest = arguments.deleteRequest
