from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field

from ...typings import PolarTypeOverrides, PolarTypeOverridesParameters
from ..interface import SourceSync, SourceSyncArguments

DATAMODELS_WITHOUT_SOFT_DELETE = (
    "NutanixCluster",
    "NutanixClusterHost",
    "NutanixVM",
    "NutanixVMDisk",
    "NutanixNetwork",
    "NutanixNetworkInterface",
)


class SoftDeleteArguments(BaseModel):
    enabled: Annotated[bool, Field(default=False)]
    field: Annotated[str, Field(min_length=1, default="status")]
    active_value: Annotated[str, Field(min_length=1, default="enabled")]
    inactive_value: Annotated[str, Field(min_length=1, default="inactive")]


class ItopSyncArguments(SourceSyncArguments):
    datamodel: Annotated[str, Field(min_length=1)]
    oql_key: Annotated[str, Field(min_length=1)]
    soft_delete: Annotated[Optional[SoftDeleteArguments], Field(default=None)]
    link_columns: Annotated[list[str], Field(default=[])]
    comparison_columns: Annotated[list[str], Field(min_length=1)]
    unique_columns: Annotated[list[str], Field(min_length=1)]
    skip_columns: Annotated[Optional[list[str]], Field(default=[])]
    foreign_key_columns: Annotated[Optional[list[str]], Field(default=[])]
    type_overrides: Annotated[Optional[PolarTypeOverridesParameters], Field(default={})]

    type: Annotated[Literal["Itop"], Field(default="Itop")] = "Itop"


class ItopSync(SourceSync):
    """The configuration class used for iTop sources"""

    datamodel: str
    oql_key: str
    soft_delete: Optional[SoftDeleteArguments]
    link_columns: list[str]
    comparison_columns: list[str]
    unique_columns: list[str]
    skip_columns: list[str]
    foreign_key_columns: list[str]
    type_overrides: PolarTypeOverrides

    def __init__(self, arguments: ItopSyncArguments):
        super().__init__(arguments)

        self.datamodel = arguments.datamodel
        self.oql_key = arguments.oql_key
        self.comparison_columns = arguments.comparison_columns
        self.unique_columns = arguments.unique_columns
        self.skip_columns = arguments.skip_columns
        self.soft_delete = arguments.soft_delete
        self.link_columns = arguments.link_columns
        self.foreign_key_columns = arguments.foreign_key_columns
        self.type_overrides = PolarTypeOverrides(arguments.type_overrides)

        # TODO: This can be made redundant by adding the model to the soft delete and only allowing the values to be models that support soft delete
        if arguments.soft_delete is not None:
            soft_delete_not_supported = (
                arguments.soft_delete.enabled
                and arguments.datamodel in DATAMODELS_WITHOUT_SOFT_DELETE
            )
            if soft_delete_not_supported:
                raise Exception(
                    f"Soft delete is not supported for {arguments.datamodel} datamodel"
                )
