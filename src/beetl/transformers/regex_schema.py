from typing import Annotated, Literal, Optional, Union

from pydantic import Field

from .interface import TransformerConfigBase, TransformerSchemaBase


class RegexTransformerSchema:
    class Replace(TransformerSchemaBase):
        class Config(TransformerConfigBase):
            inField: Annotated[str, Field(min_length=1)]
            outField: Annotated[Optional[str], Field(default=None)]
            query: Annotated[str, Field(min_length=1)]
            replace: str
            maxN: Annotated[int, Field(default=-1)]

        transformer: Literal["regex.replace"]
        config: "RegexTransformerSchema.Replace.Config"


RegexTransformerSchemas = Union[RegexTransformerSchema.Replace,]
