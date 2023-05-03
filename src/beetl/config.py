import yaml
import json
from typing import List, Dict
from dataclasses import dataclass
from .sources.interface import (
    SourceInterfaceConfiguration, 
    SourceInterfaceConnectionSettings
)
from .transformers.interface import (
    TransformerConfiguration
)

class SourceConfiguration:
    """ The configuration class used for datasources """
    name: str
    source_type: str
    connection: SourceInterfaceConnectionSettings
    config: SourceInterfaceConfiguration
    
    def __init__(self, name: str, source_type: str, connection: dict, config: dict) -> None:
        self.name, self.source_type = name, source_type
        source_class = __import__(f'beetl.sources.{source_type.lower()}')
        self.connection = getattr(source_class, f'{source_type}SourceConnectionSettings')(
            **connection
        )
        self.config = getattr(source_class, f'{source_type}SourceConfig')(
            **config
        )

@dataclass
class SyncConfiguration:
    """The configuration for a single sync between two sources"""
    source: SourceConfiguration
    destination: SourceConfiguration
    sourceTransformer: TransformerConfiguration = None
    fieldTransformers: List[TransformerConfiguration] = None

class BeetlConfig:
    """The configuration class for BeETL"""
    version: str
    sources: Dict[str, SourceConfiguration]
    sync_list: List[SyncConfiguration]
    
    def __init__(self, config: dict) -> None:
        # Set version to keep track of changes
        # in an effort to keep backwards compatibility
        
        # Get the class corresponding to the configuration version
        module = __import__(self.__module__, fromlist=[''])
        config_class = getattr(module, f'BeetlConfig{config.get("configVersion", "V1")}')
        
        # Transform this class into the versioned configuration class
        self.__class__ = config_class
        self.__dict__ = config_class(config).__dict__
        self.version = config.get('configVersion', 'V1')
        
    
    @classmethod
    def from_yaml_file(cls, path: str, encoding: str = 'utf-8') -> "BeetlConfig":
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
            with open(path, 'r', encoding=encoding) as file:
                config = yaml.safe_load(file)
                return cls(config)
        except FileNotFoundError as e:
            raise Exception(f"The configuration file was not found at the path specified ({path}).") from e

    @classmethod
    def from_json_file(cls, path: str, encoding: str = 'utf-8') -> "BeetlConfig":
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
            with open(path, 'r', encoding=encoding) as file:
                config = json.load(file)
                return cls(config)
        except FileNotFoundError as e:
            raise Exception(f"The configuration file was not found at the path specified ({path}).") from e

class BeetlConfigV1(BeetlConfig):
    """Version 1 of the Beetl Configuration Interface"""
    def __init__(self, config: dict):
        self.sources, self.sync_list = {}, []
        
        if len(config.get('sources', '')) == 0 or len(config.get('sync', '')) == 0:
            raise Exception("The configuration file is missing the 'sources' or 'sync' section.")
        
        for source in config['sources']:
            name = source.pop('name')
            source_type = source.pop('type')
            source_module = __import__('.'.join(self.__module__.split('.')[0:-1] + [f'sources.{source_type.lower()}']), fromlist=[''])          
            source_class = getattr(source_module, f'{source_type}Source')
            self.sources[name] = source_class(**source)
        
        for sync in config['sync']:
            if ( not self.sources.get(sync['source'], False)
                or not self.sources.get(sync['destination'], False)
            ):
                raise Exception("One of the source/destination names in "
                                "the sync section does not match a source name "
                                "in the sources section."
                                "Please check your configuration.",        
                )

            syncConfig = SyncConfiguration(
                    source = self.sources[sync['source']],
                    destination = self.sources[sync['destination']]
            )
            if sync.get('sourceTransformer', None):
                syncConfig.sourceTransformer = TransformerConfiguration("source", sync['sourceTransformer'], {})
            
            if sync.get('fieldTransformers', None) is not None:
                syncConfig.fieldTransformers = []
                for transformer in sync['fieldTransformers']:
                    syncConfig.fieldTransformers.append(
                        TransformerConfiguration("field", transformer.get('transformer'), transformer.get('config', None))
                    )
            
            self.sync_list.append(syncConfig)
        
