from typing import Annotated, Any, Literal, Optional, Union

from pydantic import Field

from .interface import TransformerConfigBase, TransformerSchemaBase


class ItopTransformerSchema:
    class Orgcode(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inFields: Annotated[list[str], Field(min_length=1)]
            outField: Annotated[str, Field(min_length=1)]
            toplevel: Annotated[Optional[str], Field(default=None)]

        transformer: Literal["itop.orgcode"]
        config: Config

    class Orgtree(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            treeFields: Annotated[list[str], Field(min_length=1)]
            toplevel: Annotated[str, Field(min_length=1)]
            name_field: Annotated[str, Field(default="name")]
            path_field: Annotated[str, Field(default="orgpath")]
            code_field: Annotated[str, Field(default="code")]
            parent_field: Annotated[str, Field(default="parent_code")]

        transformer: Literal["itop.orgtree"]
        config: Config

    class Relations(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            class FieldRelation(TransformerConfigBase):
                source_field: Annotated[str, Field(min_length=1)]
                source_comparison_field: Annotated[str, Field(min_length=1)]
                foreign_class_type: Annotated[str, Field(min_length=1)]
                foreign_comparison_field: Annotated[str, Field(min_length=1)]
                use_like_operator: Annotated[bool, Field(default=False)]

            field_relations: Annotated[list[FieldRelation], Field(min_length=1)]

        transformer: Literal["itop.relations"]
        config: Config


ItopTransformerSchemas = Union[
    ItopTransformerSchema.Orgcode,
    ItopTransformerSchema.Orgtree,
    ItopTransformerSchema.Relations,
]
