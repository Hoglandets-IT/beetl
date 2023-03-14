from .interface import SourceInterface, SourceInterfaceConfig, SourceConnectionSettings
from polars import DataFrame as POLARS_DF
import pandas as pd
from dataclasses import dataclass
import polars as pl
from enum import Enum

class ContentTypes(Enum):
    JSON = 'json'
    CSV = 'csv'
    XML = 'xml'
    EXCEL = 'excel'

@dataclass
class FileSourceConfig(SourceInterfaceConfig):
    skip_rows: int = 0
    filetype: ContentTypes = None
    
    def __post_init__(self):
        super().__post_init__()
        self.filetype = ContentTypes(self.filetype)

@dataclass
class FileSourceConnectionSettings(SourceConnectionSettings):
    path: str = None
    encoding: str = None
    
    def __post_init__(self):
        if hasattr(super(), '__post_init__'):
            super().__post_init__()

class FileSource(SourceInterface):
    ConnectionSettingsClass = FileSourceConnectionSettings
    SourceConfigClass = FileSourceConfig
    cache: POLARS_DF = None
    
    """
        File source implementation
        connection: File handle
        source_config:
            # Data Retrieval
            columns: List[DataColumn]
            skip_rows: int
            filetype: ContentTypes
            
            # Data Insertion
            columns: List[DataColumn]
            
        connection_settings:
            path: str
            encoding: str
    """
    
    def _configure(self):
       pass 
       
    def query(self) -> POLARS_DF:
        if self.source_config.filetype == ContentTypes.EXCEL:
            df = POLARS_DF(pd.read_excel(self.connection_settings.path))
        elif self.source_config.filetype == ContentTypes.JSON:
            df = pl.read_json(self.connection_settings.path)
        elif self.source_config.filetype == ContentTypes.CSV:
            df = pl.read_csv(self.connection_settings.path)
        elif self.source_config.filetype == ContentTypes.XML:
            df = POLARS_DF(pd.read_xml(self.connection_settings.path))
        else:
            raise NotImplementedError(f"File type {self.source_config.filetype} not implemented")
    
        self.connection = df
        
        return df
    
    def save_file(self, data: POLARS_DF):
        with open(self.connection_settings.path, 'w') as f:
            panda = self.connection.to_pandas()
            if self.source_config.filetype == ContentTypes.EXCEL:
                panda.to_excel(f)
            elif self.source_config.filetype == ContentTypes.JSON:
                panda.to_json(f)
            elif self.source_config.filetype == ContentTypes.CSV:
                panda.to_csv(f)
            elif self.source_config.filetype == ContentTypes.XML:
                panda.to_xml(f)
            else:
                raise NotImplementedError(f"File type {self.source_config.filetype} not implemented")
    
    def insert(self, df: POLARS_DF):
        self.connection = pl.concat([self.connection, df])
        self.save_file()

    def update(self, df: POLARS_DF):
        pass

    def delete(self, df: POLARS_DF):
        self.connection = self.connection.join(df, how='anti', on=[x.name for x in self.source_config.columns if x.unique])
        self.save_file()

    
    