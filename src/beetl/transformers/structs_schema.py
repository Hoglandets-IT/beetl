from typing import Annotated, Literal, Optional, Union

from pydantic import Field

from .interface import TransformerConfigBase, TransformerSchemaBase


class StructTransformerSchema:
    class StaticField(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            field: Annotated[str, Field(min_length=1)]
            value: str
            only_add_if_missing: Optional[bool] = False

        transformer: Literal["structs.staticfield"]
        config: "StructTransformerSchema.StaticField.Config"


StringTransformerSchemas = Union[StructTransformerSchema.StaticField]
