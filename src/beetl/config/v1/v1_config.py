import copy
import json
import os

from ...sources import Sources
from ...transformers.interface import TransformerConfiguration
from ...typings import ComparisonColumn
from ..config_base import BeetlConfig, SyncConfiguration
from .v1_schema import BeetlConfigSchemaV1


class BeetlConfigV1(BeetlConfig):
    """Version 1 of the Beetl Configuration Interface"""

    @staticmethod
    def generate_jsonschema() -> str:
        """Generates the jsonschema for the BeetlConfigV1 configuration."""
        return json.dumps(BeetlConfigSchemaV1.model_json_schema())

    @staticmethod
    def validate_config(config: dict):
        """Performs static and dynamic validation of the user supplied configuration.

        Args:
            config (dict): The user supplied configuration

        Raises:
            ValidationError: If the configuration is invalid when compared against the static jsonschema
            ConfigValidationError: If the configuration is invalid when compared against the dynamic beetl validation rules
        """
        BeetlConfigSchemaV1(**config)

    def __init__(self, config: dict):
        self.sources, self.sync_list = {}, []

        if not config.get("sources", []):
            config["sources"] = self.read_sources_from_env(config)

        BeetlConfigV1.validate_config(config)

        self.sources = self.initialize_sources(config)

        if len(config.get("sync", "")) == 0:
            raise ValueError("The configuration file is missing the 'sync' section.")

        for sync_index, sync in enumerate(config["sync"]):
            location = ("sync", str(sync_index))
            if not self.sources.get(sync["source"], False) or not self.sources.get(
                sync["destination"], False
            ):
                raise ValueError(
                    "One of the source/destination names in "
                    "the sync section does not match a source name "
                    "in the sources section."
                    "Please check your configuration.",
                )

            source_name = sync["source"]
            source_instance = copy.deepcopy(self.sources[source_name])
            source_instance.set_sourceconfig(
                sync["sourceConfig"],
                direction="source",
                name=source_name,
                location=(*location, "sourceConfig"),
            )

            destination_name = sync["destination"]
            destination_instance = copy.deepcopy(self.sources[destination_name])
            destination_instance.set_sourceconfig(
                sync["destinationConfig"],
                direction="destination",
                name=destination_name,
                location=(*location, "destinationConfig"),
            )

            raw_diff_section = sync.get("diff", {})
            raw_diff_destination = raw_diff_section.get("destination", None)
            diff_instance = None
            if raw_diff_destination:
                diff_name = raw_diff_destination.get("name", None)
                diff_instance = copy.deepcopy(self.sources.get(diff_name, None))
                if not diff_instance:
                    raise ValueError(
                        "The diff source name in the sync section does not match a source name in the sources section."
                    )
                diff_instance.set_diff_config(raw_diff_destination)

            comparisonColumnsConf = sync.get("comparisonColumns", None)
            if not comparisonColumnsConf:
                raise ValueError(
                    "The sync configuration is missing the 'comparisonColumns' key."
                )
            if type(comparisonColumnsConf) is list:
                try:
                    comparisonColumns = [
                        ComparisonColumn(**args) for args in sync["comparisonColumns"]
                    ]
                except TypeError as e:
                    raise ValueError(
                        "When passing the comparisonColumns as a list it must be a list of dictionaries containing the mandatory keys 'name', 'type', and optionally 'unique'."
                    ) from e
            elif type(comparisonColumnsConf) is dict:
                try:
                    comparisonColumns = []
                    unique_column_name, _ = list(comparisonColumnsConf.items())[0]
                    for key, value in comparisonColumnsConf.items():
                        comparisonColumns.append(
                            ComparisonColumn(key, value, key == unique_column_name)
                        )

                except Exception as e:
                    raise ValueError(
                        "The comparisonColumns must be a list of dictionaries containing the mandatory keys 'name', 'type', and optionally 'unique'."
                    ) from e
            else:
                raise ValueError(
                    "The comparisonColumns must be a list of dictionaries containing the mandatory keys 'name', 'type', and optionally 'unique'. Or a dictionary with the column names as key and types as values, where the first key is treated as the unique column."
                )

            syncConfig = SyncConfiguration(
                name=sync.get("name", ""),
                source=source_instance,
                sourceConfig=sync["sourceConfig"],
                destination=destination_instance,
                destinationConfig=sync["destinationConfig"],
                comparisonColumns=comparisonColumns,
                diff_destination_instance=diff_instance,
            )

            source_instance = destination_instance = None

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

            if raw_diff_section.get("transformers", None) is not None:
                syncConfig.diff_transformers = []
                for transformer in raw_diff_section["transformers"]:
                    syncConfig.diff_transformers.append(
                        TransformerConfiguration(
                            transformer.get("transformer"),
                            transformer.get("config", None),
                            transformer.get("include_sync", False),
                        )
                    )

            self.sync_list.append(syncConfig)

    def initialize_sources(self, config):
        sources = {}
        for source_index, source in enumerate(config["sources"]):
            name = source["name"]
            source_type = source["type"]
            registrated_source = Sources.sources.get(source_type, None)
            if registrated_source is None:
                raise ValueError(
                    f"The source type '{source_type}' used in source '{name}' is not registrated in available sources"
                )

            location = ("sources", str(source_index))
            source_config_with_location = {**source, "location": location}
            sources[name] = registrated_source.cls(source_config_with_location)
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
            raise ValueError(
                "The environment variable specified in 'sourcesFromEnv' "
                "is not a valid JSON string or does not exist."
            ) from e

        return envConfig
