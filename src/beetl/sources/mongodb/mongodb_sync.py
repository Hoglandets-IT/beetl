from typing import Annotated, Any, Literal, Optional

from pydantic import Field

from ..interface import SourceSync, SourceSyncArguments


class MongodbSyncArguments(SourceSyncArguments):
    collection: str
    filter: Annotated[Optional[dict[str, Any]], Field(default={})]
    projection: Annotated[Optional[dict[str, int]], Field(default={})]
    uniqueFields: Annotated[
        Optional[list[str]],
        Field(default=[], description="Required when used as a destination"),
    ]
    type: Annotated[Literal["Mongodb"], Field(default="Mongodb")] = "Mongodb"


class MongodbSync(SourceSync):
    """The configuration class used for MongoDB sources"""

    collection: Annotated[Optional[str], Field(default=None)]
    filter: Annotated[str, Field(default={})]
    projection: dict = None
    unique_fields: list[str] = None

    def __init__(self, arguments: MongodbSyncArguments):
        super().__init__(arguments)
        self.collection = arguments.collection
        self.filter = arguments.filter
        self.projection = arguments.projection
        self.unique_fields = arguments.uniqueFields
