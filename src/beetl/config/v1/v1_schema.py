from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from pydantic_core import ErrorDetails

from ...sources import (
    CsvConfigArguments,
    CsvDiffArguments,
    ExcelConfigArguments,
    ExcelDiffArguments,
    ExcelSyncArguments,
    FakerConfigArguments,
    FakerDiffArguments,
    ItopConfigArguments,
    ItopSyncArguments,
    MongodbConfigArguments,
    MongodbDiffArguments,
    MongodbSyncArguments,
    MysqlConfigArguments,
    MysqlDiffArguments,
    MysqlSyncArguments,
    PostgresConfigArguments,
    PostgresDiffArguments,
    PostgresSyncArguments,
    RestConfigArguments,
    RestSyncArguments,
    Sources,
    SqlserverConfigArguments,
    SqlserverDiffArguments,
    SqlserverSyncArguments,
    StaticConfigArguments,
    StaticDiffArguments,
    XmlConfigArguments,
    XmlDiffArguments,
    XmlSyncArguments,
)
from ...transformers import TransformerSchemas

SourceConfigArguments = list[
    Union[
        (
            StaticConfigArguments,
            ItopConfigArguments,
            MongodbConfigArguments,
            ExcelConfigArguments,
            CsvConfigArguments,
            FakerConfigArguments,
            MysqlConfigArguments,
            PostgresConfigArguments,
            RestConfigArguments,
            SqlserverConfigArguments,
            XmlConfigArguments,
        )
    ]
]

SourceSyncArguments = Union[
    (
        ItopSyncArguments,
        MongodbSyncArguments,
        MysqlSyncArguments,
        PostgresSyncArguments,
        RestSyncArguments,
        SqlserverSyncArguments,
        XmlSyncArguments,
        ExcelSyncArguments,
        # Necessary for sources that don't have a sync configuration
        dict,
    )
]


class ComparisonColumnV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: Annotated[str, Field(min_length=1)]
    type: Annotated[str, Field(min_length=1)]
    unique: Annotated[bool, Field(default=False)]


ComparisonColumns = Union[list[ComparisonColumnV1], dict[str, str]]
OptionalTransformers = Annotated[
    list[Annotated[TransformerSchemas, Field(discriminator="transformer")]],
    Field(default=[]),
]

SourceDiffArguments = Union[
    SqlserverDiffArguments,
    StaticDiffArguments,
    CsvDiffArguments,
    ExcelDiffArguments,
    FakerDiffArguments,
    MongodbDiffArguments,
    MysqlDiffArguments,
    PostgresDiffArguments,
    XmlDiffArguments,
]


class DiffArguments(BaseModel):
    model_config = ConfigDict(extra="forbid")
    transformers: OptionalTransformers
    destination: Annotated[SourceDiffArguments, Field(discriminator="type")]


class V1Sync(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_to_type: Annotated[
        dict[str, str],
        Field(
            default={},
            description="Should never be provided by the user. Is automatically populated at validation time.",
        ),
    ]
    location: Annotated[
        tuple[str, ...],
        Field(
            default=(),
            description="Should never be provided by the user. Is automatically populated at validation time.",
        ),
    ]

    name: Annotated[Optional[str], Field(default=None)]
    source: Annotated[str, Field(min_length=1)]
    destination: Annotated[str, Field(min_length=1)]
    sourceConfig: Annotated[SourceSyncArguments, Field()]
    destinationConfig: Annotated[SourceSyncArguments, Field()]
    comparisonColumns: Annotated[
        ComparisonColumns,
        Field(min_length=1),
    ]
    sourceTransformers: OptionalTransformers
    destinationTransformers: OptionalTransformers
    insertionTransformers: OptionalTransformers
    deletionTransformers: OptionalTransformers

    diff: Annotated[Optional[DiffArguments], Field(default=None)]

    @model_validator(mode="before")
    def validate_sources(cls, values):
        """Makes sure that each source configuration is only validated against one source type, the one that matches the 'type' field.
        This is necessary since without it the static jsonschema validation will display errors from ALL source sync config types if the the config isn't fulfilling any of them, which can be confusing for the user.
        """
        source_to_type = values.get("source_to_type", {})
        if not source_to_type:
            raise ValueError(
                "source_to_type is missing, something is wrong with the validation setup."
            )

        errors = []
        for direction in ["source", "destination"]:
            errors.extend(cls.validate_sync_config(direction, values))

        if errors:
            raise ValidationError.from_exception_data(cls.__name__, errors)
        return values

    @classmethod
    def validate_sync_config(
        cls, direction: Literal["source", "destination"], values: dict
    ):
        """Validates the source or destination sync configuration against the correct source. Makes sure that the configuration matches exactly one source type."""
        errors = []
        source_name = values.get(direction, None)
        source_type = values.get("source_to_type", {}).get(source_name, None)
        source_sync_config = values.get(f"{direction}Config", None)
        if source_name and source_type and source_sync_config:
            registrated_source = Sources.sources.get(source_type, None)
            location = (*values["location"], f"{direction}Config")
            try:
                registrated_source.cls.SyncArgumentsClass(
                    direction=direction,
                    name=source_name,
                    location=location,
                    type=source_type,
                    **source_sync_config,
                )
            except ValidationError as e:
                errors.extend(adjust_validation_error_location(e, location))

        return errors


class BeetlConfigSchemaV1(BaseModel):
    """Represents the configuration as supplied by the user. This class is used to validate the configuration against the static jsonschema and the dynamic beetl validation rules."""

    model_config = ConfigDict(extra="forbid")

    version: Literal["V1"]
    sources: Annotated[SourceConfigArguments, Field(min_items=1)]
    sync: Annotated[list[V1Sync], Field(min_items=1)]

    @model_validator(mode="before")
    def populate_validation_values_in_nested_types(cls, values):
        """Makes sure that a map of source names to source types is available in each sync dict. This is neccessary for the validation of the source and destination config to identify what class each config should be validated against. These values are automatically generated and should never be provided by the user."""
        if not values.get("sync", []) or not values.get("sources", []):
            return values
        source_to_type_dictionary = {}
        for source in values["sources"]:
            source_to_type_dictionary[source["name"]] = source["type"]
        for index, sync in enumerate(values["sync"]):
            sync["source_to_type"] = source_to_type_dictionary
            sync["location"] = ("sync", str(index))
        return values

    @model_validator(mode="before")
    def validate_sources(cls, values):
        """Makes sure that each source in the sources list is valid against the correct source config class."""
        sources: list[dict[str, Any]] = values.get("sources", [])
        errors = []
        for i, source in enumerate(sources):
            source_type = source.get("type", None)
            if source_type is None:
                raise ValueError(
                    f"sources.{i}.type is missing, valid sources are {list(Sources.sources.keys())}"
                )

            registrated_source = Sources.sources.get(source_type, None)
            if registrated_source is None:
                raise ValueError(
                    f'sources.{i}.type "{source_type}" is not recognized, valid sources are {list(Sources.sources.keys())}'
                )
            location = ("sources", str(i))
            try:
                registrated_source.cls.ConfigArgumentsClass(location=location, **source)
            except ValidationError as e:
                errors.extend(adjust_validation_error_location(e, location))

        if errors:
            raise ValidationError.from_exception_data(cls.__name__, errors)
        return values


def adjust_validation_error_location(
    e: ValidationError, location: tuple
) -> list[ErrorDetails]:
    errors = []
    for error in e.errors():
        error["loc"] = location + error["loc"]
        errors.append(error)
    return errors
