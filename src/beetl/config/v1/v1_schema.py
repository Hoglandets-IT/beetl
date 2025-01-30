from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, Field, ValidationError, model_validator
from pydantic_core import ErrorDetails

from ...sources import (
    CsvConfigArguments,
    FakerConfigArguments,
    ItopConfigArguments,
    ItopSyncArguments,
    MongodbConfigArguments,
    MongodbSyncArguments,
    MysqlConfigArguments,
    MysqlSyncArguments,
    PostgresConfigArguments,
    PostgresSyncArguments,
    RestConfigArguments,
    RestSyncArguments,
    Sources,
    SqlserverConfigArguments,
    SqlserverSyncArguments,
    StaticConfigArguments,
    XmlConfigArguments,
    XmlSyncArguments,
)

SourceConfigs = list[
    Union[
        (
            StaticConfigArguments,
            ItopConfigArguments,
            MongodbConfigArguments,
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

SourceSyncConfig = Union[
    (
        dict[str, Any],
        ItopSyncArguments,
        MongodbSyncArguments,
        MysqlSyncArguments,
        PostgresSyncArguments,
        RestSyncArguments,
        SqlserverSyncArguments,
        XmlSyncArguments,
    )
]


class Sync(BaseModel):
    source_to_type: Annotated[
        dict[str, str],
        Field(
            default={},
            description="Used at validation time to validate the source and destination config against the correct source type. Should never be provided by the user",
        ),
    ]
    location: Annotated[
        tuple[str, ...],
        Field(
            default=(),
            description="The location path of the model in the configuration file. Used to provide more detailed error messages.",
        ),
    ]

    source: Annotated[str, Field(min_length=1)]
    destination: Annotated[str, Field(min_length=1)]
    sourceConfig: Annotated[SourceSyncConfig, Field()]
    destinationConfig: Annotated[SourceSyncConfig, Field()]
    comparisonColumns: Annotated[Any, Field()]
    sourceTransformers: Annotated[Optional[Any], Field(default=None)]
    destinationTransformers: Annotated[Optional[Any], Field(default=None)]
    insertionTransformers: Annotated[Optional[Any], Field(default=None)]
    deletionTransformers: Annotated[Optional[Any], Field(default=None)]

    @model_validator(mode="before")
    def validate_sources(cls, values):
        """Makes sure that each source configuration is only validated against one source type, the one that matches the 'type' field"""
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
        errors = []
        source_name = values.get(direction, None)
        source_type = values.get("source_to_type", {}).get(source_name, None)
        source_sync_config = values.get(f"{direction}Config", None)
        if source_name and source_type and source_sync_config:
            source_sync_config["type"] = source_type
            registrated_source = Sources.sources.get(source_type, None)
            location = (*values["location"], f"{direction}Config")
            try:
                registrated_source.cls.SyncArgumentsClass(
                    direction=direction,
                    name=source_name,
                    location=location,
                    **source_sync_config,
                )
            except ValidationError as e:
                errors.extend(adjust_validation_error_location(e, location))

        return errors


class BeetlConfigSchemaV1(BaseModel):
    version: Literal["V1"]
    sources: Annotated[SourceConfigs, Field(min_items=1)]
    sync: Annotated[list[Sync], Field(min_items=1)]

    @model_validator(mode="before")
    def populate_validation_values_in_nested_types(cls, values):
        """Makes sure that a map of source names to source types is available in each sync dict. This is neccessary for the validation of the source and destination config. These values are automatically generated and should never be provided by the user."""
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
        """Makes sure that each source configuration is only validated against one source type, the one that matches the 'type' field"""
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
            source["location"] = location
            try:
                registrated_source.cls.ConfigArgumentsClass(**source)
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
