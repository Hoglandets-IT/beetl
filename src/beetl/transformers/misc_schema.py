from typing import Annotated, Literal, Optional, Union

from pydantic import Field

from .interface import TransformerConfigBase, TransformerSchemaBase


class MiscTransformerSchema:
    class SamFromDn(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]

        transformer: Literal["misc.sam_from_dn"]
        config: "MiscTransformerSchema.SamFromDn.Config"

    class Lookup(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[str, Field(min_length=1)]
            mapping: dict[str, str]
            caseInsensitive: Annotated[bool, Field(default=True)]
    
        transformer: Literal["misc.lookup"]
        config: Config
    
    
    class GuidFromFields(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inFields: Annotated[list[str], Field(min_length=1)]
            outField: Annotated[str, Field(min_length=1)]
            separator: Annotated[str, Field(default="|")]
            namespace: Annotated[str, Field(default="beetl")]
    
        transformer: Literal["misc.guid_from_fields"]
        config: Config


MiscTransformerSchemas = Union[MiscTransformerSchema.SamFromDn, MiscTransformerSchema.Lookup, MiscTransformerSchema.GuidFromFields]
