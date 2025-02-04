from typing import Annotated, Any, Dict, Literal, Optional, Union

from pydantic import ConfigDict, Field, model_validator

from ...errors import ConfigValueError
from ...validation import ValidationBaseModel
from ..interface import SourceSync, SourceSyncArguments


class RestResponse(ValidationBaseModel):
    length: Annotated[Optional[str], Field(default=None)]
    items: str


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
    path: Annotated[str, Field(default=None)]
    query: Annotated[Dict[str, Union[str, int]], Field(default={})]
    method: Annotated[
        Literal["GET", "POST", "PUT", "HEAD", "DELETE", "OPTIONS", "PATCH"],
        Field(default="GET"),
    ]
    pagination: Annotated[Optional[PaginationSettings], Field(default=None)]
    body_type: Annotated[str, Field(default="application/json")]
    body: Annotated[
        Optional[Union[dict[Union[str, int], Any], str]], Field(default=None)
    ]
    body_options: Annotated[Optional[BodyOptions], Field(default=None)]
    return_type: Annotated[str, Field(default="application/json")]
    response: Annotated[Optional[RestResponse], Field(default=None)]

    @model_validator(mode="after")
    def validate_body_type(cls, instance: "RestRequest"):
        if instance.body_type == "application/json" and isinstance(instance.body, str):
            raise ConfigValueError(
                "body",
                "Body must be of type dictionary when body_type is set to 'application/json'.",
                instance.location,
            )
        return instance


class RestSyncArguments(SourceSyncArguments):
    listRequest: RestRequest
    createRequest: Annotated[Optional[RestRequest], Field(default=None)]
    updateRequest: Annotated[Optional[RestRequest], Field(default=None)]
    deleteRequest: Annotated[Optional[RestRequest], Field(default=None)]
    type: Annotated[Literal["Rest"], Field(defauls="Rest")] = "Rest"

    @model_validator(mode="before")
    def propagate_nested_location(cls, values: dict):
        cls.propagate_location("listRequest", values)
        cls.propagate_location("createRequest", values)
        cls.propagate_location("updateRequest", values)
        cls.propagate_location("deleteRequest", values)

        return values


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
