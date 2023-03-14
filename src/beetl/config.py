from typing import List, Dict
from dataclasses import dataclass
from .sources.interface import SourceInterfaceConfig, SourceConnectionSettings
import yaml
import json

class SourceConfig:
    name: str
    source_type: str
    connection: SourceConnectionSettings
    config: SourceInterfaceConfig
    
    def __init__(self, name: str, source_type: str, connection: dict, config: dict):
        self.name, self.source_type = name, source_type
        source_class = __import__(f'sources.{source_type.lower()}')
        self.connection = getattr(source_class, f'{source_type}SourceConnectionSettings')(
            **connection
        )
        self.config = getattr(source_class, f'{source_type}SourceConfig')(
            **config
        )   

@dataclass
class SyncConfig:
    source: SourceConfig
    destination: SourceConfig
    mapping: List[dict]

class Config:
    def __init__(self, config: dict):
        version = config.get('configVersion', '1')
        
        # Get the class corresponding to the config version
        module_path = ".".join(self.__module__.split('.')[0:-1])
        config_module = __import__(
            '.'.join((
                    module_path, 
                    'config'
                )
            ), 
            fromlist=['']
        )
        config_class = getattr(config_module, 'ConfigV' + version.upper())
        
        # Init the config
        self.__dict__ = config_class(config).__dict__
    
    @classmethod
    def from_yaml_file(cls, path: str, encoding: str = 'utf-8'):
        with open(path, 'r', encoding=encoding) as f:
            content = yaml.safe_load(f)
        
        return cls(content)

    @classmethod
    def from_json_file(cls, path: str, encoding: str = 'utf-8'):
        with open(path, 'r', encoding=encoding) as f:
            content = json.load(f)
        
        return cls(content)

class ConfigV1(Config):
    sources: Dict[str, SourceConfig]
    sync: List[SyncConfig]

    def __init__(self, config: SyncConfig):
        self.sources = {}
        module_path = ".".join(self.__module__.split('.')[0:-1])
        
        if len(config.get('sources', '')) == 0 or len(config.get('sync', '')) == 0:
            raise Exception('Config must contain at least one source and one sync')
        
        for source in config['sources']:
            name = source.pop('name')
            source_type = source.pop('type')
            source_module = __import__(
                '.'.join((
                        module_path, 
                        'sources', 
                        source_type.lower()
                    )
                ), 
                fromlist=['']
            )
            source_class = getattr(source_module, source_type + 'Source')
            self.sources[name] = source_class(**source)
        
        self.sync = []
        for synchro in config["sync"]:
            self.sync.append(
                SyncConfig(
                    source=self.sources[synchro['source']],
                    destination=self.sources[synchro['destination']],
                    mapping=synchro.get('mapping', [])
                )
            )