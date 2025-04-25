import json
from dataclasses import dataclass
from typing import Any, Dict, List, Literal

import polars as pl
import yaml

from ..sources.interface import SourceConfig, SourceInterface, SourceSync
from ..transformers.interface import TransformerConfiguration
from ..typings import ComparisonColumn


class SourceSettings:
    """The configuration class used for datasources"""

    name: str
    source_type: str
    connection: SourceConfig
    config: SourceSync

    def __init__(
        self, name: str, source_type: str, connection: dict, config: dict
    ) -> None:
        self.name, self.source_type = name, source_type
        source_class = __import__(f"beetl.sources.{source_type.lower()}")

        self.connection = getattr(
            source_class, f"{source_type}SourceConnectionSettings"
        )(**connection)

        self.config = getattr(source_class, f"{source_type}SourceConfig")(**config)


@dataclass
class ChangeDetectionConfig:
    threshold: int = -1
    stopSync: bool = False
    webhook: str = None
    webhookHeaders: Dict[str, str] = None
    webhookPayload: Dict[str, str] = None


@dataclass
class SyncConfiguration:
    """The configuration for a single sync between two sources"""

    source: SourceInterface
    sourceConfig: SourceSync
    destination: SourceInterface
    destinationConfig: SourceSync
    comparisonColumns: list[ComparisonColumn]
    name: str = ""
    changeDetection: ChangeDetectionConfig = None

    sourceTransformers: TransformerConfiguration = None
    destinationTransformers: TransformerConfiguration = None

    insertionTransformers: list[TransformerConfiguration] = None
    deletionTransformers: list[TransformerConfiguration] = None

    diff_destination_instance: SourceInterface = None
    diff_transformers: list[TransformerConfiguration] = None

    def __post_init__(self) -> None:
        self.source.config = self.sourceConfig
        self.destination.config = self.destinationConfig


class BeetlConfig:
    """The configuration class for BeETL"""

    version: Literal["V1"]
    sources: Dict[str, SourceSettings]
    sync_list: List[SyncConfiguration]

    def __init__(self, config: dict) -> None:
        # Set version to keep track of changes
        # in an effort to keep backwards compatibility

        # Get the class corresponding to the configuration version
        module_root = str.join(".", self.__module__.split(".")[:-1])
        module = __import__(module_root, fromlist=[""])
        config_class = getattr(
            module, f'BeetlConfig{config.get("configVersion", "V1")}'
        )

        # Transform this class into the versioned configuration class
        self.__class__ = config_class
        self.__dict__ = config_class(config).__dict__
        self.version = config.get("configVersion", "V1")

    @classmethod
    def from_yaml(cls, yamlData: str) -> "BeetlConfig":
        """Import configuration from a YAML string

        Args:
            yaml (str): The YAML string containing the configuration

        Returns:
            BeetlConfig: Beetl Configuration
        """
        config = yaml.safe_load(yamlData)
        return cls(config)

    @classmethod
    def from_yaml_file(cls, path: str, encoding: str = "utf-8") -> "BeetlConfig":
        """Import configuration from YAML file

        Args:
            path (str): The path to the configuration file
            encoding (str, optional): The encoding of the file. Defaults to 'utf-8'.

        Raises:
            Exception: File not found

        Returns:
            BeetlConfig: Beetl Configuration
        """
        try:
            with open(path, "r", encoding=encoding) as file:
                config = yaml.safe_load(file)
                return cls(config)
        except FileNotFoundError as e:
            raise Exception(
                f"The configuration file was not found at the path specified ({path})."
            ) from e

    @classmethod
    def from_json(cls, jsonData: str) -> "BeetlConfig":
        """Import configuration from a JSON string

        Args:
            jsonData (str): The JSON string containing the configuration

        Returns:
            BeetlConfig: Beetl Configuration
        """
        config = json.loads(jsonData)
        return cls(config)

    @classmethod
    def from_json_file(cls, path: str, encoding: str = "utf-8") -> "BeetlConfig":
        """Import configuration from YAML file

        Args:
            path (str): The path to the configuration file
            encoding (str, optional): The encoding of the file. Defaults to 'utf-8'.

        Raises:
            Exception: File not found

        Returns:
            BeetlConfig: Beetl Configuration
        """
        try:
            with open(path, "r", encoding=encoding) as file:
                config = json.load(file)
                return cls(config)
        except FileNotFoundError as e:
            raise Exception(
                f"The configuration file was not found at the path specified ({path})."
            ) from e
