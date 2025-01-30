from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field


class ValidationBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    location: Annotated[
        tuple[str, ...],
        Field(
            default=(),
            description="The location path of the model in the configuration file. Used to provide more detailed error messages.",
        ),
    ]

    @staticmethod
    def propagate_location(nested_cls_property, arguments: dict[str, Any]):
        """Propagates the location of the model to nested models. This should be used inside of a model_validator with mode='before' on every validation model that has nested validation models.

        Example:
        ```python
        class NestedModel(ValidationBaseModel):
          pass

        class MainModel(ValidationBaseModel):
          nested: NestedModel

          @model_validator(mode="before")
          def propagate_nested_location(cls, values: dict):
              propagate_location("nested", location)

              return values
        ```
        """
        nested_cls = arguments.get(nested_cls_property, None)
        if nested_cls is None:
            return
        location = arguments.get("location", ())
        nested_cls["location"] = location + (nested_cls_property,)
