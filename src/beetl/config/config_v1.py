
import copy
import json
import os
from pydantic import BaseModel, Field, model_validator, ValidationError
from typing import Annotated, Any, List, Literal, Union

# TODO: Probably move imports to the __init__.py of sources
from ..sources.mongodb import MongodbSource, MongoDBSourceConnectionSettingsAttributes
from ..sources.interface import SourceInterface
from ..sources.itop import ItopSource, ItopSourceConnectionSettingsArguments
from ..sources.static import StaticSource, StaticSourceConnectionSettingsArguments
from ..transformers.interface import TransformerConfiguration
from .config_base import BeetlConfig, ComparisonColumn, SyncConfiguration


class BeetlConfigSchemaSourceV1(BaseModel):
    name: str
    type: Literal["Csv", "Faker", "Itop", "Mongodb",
                  "Mysql", "Postgres", "Rest", "Sqlserver", "Static"]


# TODO: This can probably be a dict where sources register them selves just like transformers
TYPE_TO_SOURCE: dict[str, SourceInterface] = {
    "Static": StaticSource,
    "Itop": ItopSource,
    "Mongodb": MongodbSource
}

Sources = List[Union[StaticSourceConnectionSettingsArguments,
               ItopSourceConnectionSettingsArguments, MongoDBSourceConnectionSettingsAttributes]]


class BeetlConfigSchemaV1(BaseModel):
    version: Literal["V1"]
    sources: Annotated[Sources, Field(min_items=1)]
    sync: List[Any]

    @ model_validator(mode='before')
    def validate_sources(cls, values):
        """Makes sure that each source configuration is only validated against one source type, the one that matches the 'type' field"""
        sources = values.get('sources', [])
        errors = []
        for i, source in enumerate(sources):
            source_type = source.get('type', None)
            if source_type is None:
                raise ValueError(
                    f"sources.{i}.type is missing, valid sources are {list(TYPE_TO_SOURCE.keys())}")
            try:
                source_class = TYPE_TO_SOURCE.get(
                    source_type, None)
                if source_class is None:
                    raise ValueError(
                        f"sources.{i}.type \"{source_type}\" is not recognized, valid sources are {list(TYPE_TO_SOURCE.keys())}")
                source_class.ConnectionSettingsArguments(**source)
            except ValidationError as e:
                # Mutatate the location so that it is absolute to the sources position in the configuration
                for error in e.errors():
                    error['loc'] = ('sources', i) + error['loc']
                    errors.append(error)
        if errors:
            raise ValidationError.from_exception_data(cls.__name__, errors)
        return values


class BeetlConfigV1(BeetlConfig):
    """Version 1 of the Beetl Configuration Interface"""

    @staticmethod
    def generate_jsonschema() -> str:
        return json.dumps(BeetlConfigSchemaV1.model_json_schema())

    @staticmethod
    def validate_config(config: dict):
        BeetlConfigSchemaV1(**config)

    def __init__(self, config: dict):
        self.sources, self.sync_list = {}, []

        if not config.get("sources", []):
            config["sources"] = self.read_sources_from_env(config)

        BeetlConfigV1.validate_config(config)

        self.sources = self.initialize_sources(config)

        # TODO: Validate sync
        if len(config.get("sync", "")) == 0:
            raise Exception(
                "The configuration file is missing the 'sync' section.")

        for sync in config["sync"]:
            if not self.sources.get(sync["source"], False) or not self.sources.get(
                sync["destination"], False
            ):
                raise Exception(
                    "One of the source/destination names in "
                    "the sync section does not match a source name "
                    "in the sources section."
                    "Please check your configuration.",
                )

            tmpSource = copy.deepcopy(self.sources[sync["source"]])
            tmpSource.set_sourceconfig(sync["sourceConfig"])
            tmpDestination = copy.deepcopy(self.sources[sync["destination"]])
            tmpDestination.set_sourceconfig(sync["destinationConfig"])

            comparisonColumnsConf = sync.get("comparisonColumns", None)
            if not comparisonColumnsConf:
                raise Exception(
                    "The sync configuration is missing the 'comparisonColumns' key."
                )
            if type(comparisonColumnsConf) is list:
                try:
                    comparisonColumns = [
                        ComparisonColumn(**args) for args in sync["comparisonColumns"]
                    ]
                except TypeError as e:
                    raise Exception(
                        "When passing the comparisonColumns as a list it must be a list of dictionaries containing the mandatory keys 'name', 'type', and optionally 'unique'."
                    ) from e
            elif type(comparisonColumnsConf) is dict:
                try:
                    comparisonColumns = []
                    unique_column_name, _ = list(
                        comparisonColumnsConf.items())[0]
                    for key, value in comparisonColumnsConf.items():
                        comparisonColumns.append(ComparisonColumn(
                            key, value, key == unique_column_name))

                except Exception as e:
                    raise Exception(
                        "The comparisonColumns must be a list of dictionaries containing the mandatory keys 'name', 'type', and optionally 'unique'."
                    ) from e
            else:
                raise Exception(
                    "The comparisonColumns must be a list of dictionaries containing the mandatory keys 'name', 'type', and optionally 'unique'. Or a dictionary with the column names as key and types as values, where the first key is treated as the unique column."
                )

            syncConfig = SyncConfiguration(
                name=sync.get("name", ""),
                source=tmpSource,
                sourceConfig=sync["sourceConfig"],
                destination=tmpDestination,
                destinationConfig=sync["destinationConfig"],
                comparisonColumns=comparisonColumns,
            )

            tmpSource = tmpDestination = None

            if sync.get("sourceTransformer", None) is not None:
                syncConfig.sourceTransformer = TransformerConfiguration(
                    "source", sync["sourceTransformer"], {}
                )

            if sync.get("sourceTransformers", None) is not None:
                syncConfig.sourceTransformers = []
                for transformer in sync["sourceTransformers"]:
                    syncConfig.sourceTransformers.append(
                        TransformerConfiguration(
                            transformer.get("transformer"),
                            transformer.get("config", None),
                        )
                    )

            if sync.get("destinationTransformers", None) is not None:
                syncConfig.destinationTransformers = []
                for transformer in sync["destinationTransformers"]:
                    syncConfig.destinationTransformers.append(
                        TransformerConfiguration(
                            transformer.get("transformer"),
                            transformer.get("config", None),
                        )
                    )

            if sync.get("insertionTransformers", None) is not None:
                syncConfig.insertionTransformers = []
                for transformer in sync["insertionTransformers"]:
                    syncConfig.insertionTransformers.append(
                        TransformerConfiguration(
                            transformer.get("transformer"),
                            transformer.get("config", None),
                            transformer.get("include_sync", False),
                        )
                    )

            if sync.get("deletionTransformers", None) is not None:
                syncConfig.deletionTransformers = []
                for transformer in sync["deletionTransformers"]:
                    syncConfig.deletionTransformers.append(
                        TransformerConfiguration(
                            transformer.get("transformer"),
                            transformer.get("config", None),
                            transformer.get("include_sync", False),
                        )
                    )

            self.sync_list.append(syncConfig)

    def initialize_sources(self, config):
        sources = {}
        for source in config["sources"]:
            name = source["name"]
            source_type = source["type"]
            source_module = __import__(
                ".".join(
                    self.__module__.split(".")[0:-2]
                    + [f"sources.{source_type.lower()}"]
                ),
                fromlist=[""],
            )
            try:
                source_class = getattr(source_module, f"{source_type}Source")
            except Exception as e:
                raise Exception(
                    f"The source type '{source_type}' used in source '{name}' is not a valid source type."
                ) from e

            sources[name] = source_class(source)
        return sources

    def read_sources_from_env(self, config):
        not_using_env_config = config.get("sourcesFromEnv", "") in [None, ""]
        if not_using_env_config:
            return

        sourcesFromEnv = config.pop("sourcesFromEnv")
        envJson = os.environ.get(sourcesFromEnv)
        try:
            envConfig = json.loads(envJson)
        except Exception as e:
            raise Exception(
                "The environment variable specified in 'sourcesFromEnv' "
                "is not a valid JSON string or does not exist."
            ) from e

        return envConfig
