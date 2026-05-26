from typing import Annotated, Any, Literal, Optional, Union

from pydantic import Field

from .interface import TransformerConfigBase, TransformerSchemaBase


class StructTransformerSchema:
    class StaticField(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            field: Annotated[str, Field(min_length=1)]
            value: Any
            only_add_if_missing: Optional[bool] = False

        transformer: Literal["structs.staticfield"]
        config: "StructTransformerSchema.StaticField.Config"

    class Jsonpath(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[str, Field(min_length=1)]
            jsonPath: Annotated[str, Field(min_length=1)]
            defaultValue: Annotated[str, Field(default="")]

        transformer: Literal["structs.jsonpath"]
        config: "StructTransformerSchema.Jsonpath.Config"

    class ComposeStruct(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            outField: Annotated[str, Field(min_length=1)]
            map: Annotated[dict[str, str], Field(min_length=1)]

        transformer: Literal["structs.compose_struct"]
        config: "StructTransformerSchema.ComposeStruct.Config"

    class ComposeListOfStructs(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            outField: Annotated[str, Field(min_length=1)]
            map: Annotated[dict[str, str], Field(min_length=1)]

        transformer: Literal["structs.compose_list_of_structs"]
        config: "StructTransformerSchema.ComposeListOfStructs.Config"


StructTransformerSchemas = Union[
    StructTransformerSchema.StaticField,
    StructTransformerSchema.Jsonpath,
    StructTransformerSchema.ComposeStruct,
    StructTransformerSchema.ComposeListOfStructs,
]
