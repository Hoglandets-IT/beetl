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


MiscTransformerSchemas = Union[MiscTransformerSchema.SamFromDn,]
