from .interface import SourceInterface, SourceInterfaceConfig, SourceConnectionSettings
from polars import DataFrame as POLARS_DF
from dataclasses import dataclass
import polars as pl

@dataclass
class MySQLSourceConfig(SourceInterfaceConfig):
    query: str = None
    table: str = None
    
    def __post_init__(self):
        super().__post_init__()

@dataclass
class MySQLSourceConnectionSettings(SourceConnectionSettings):
    connection_string: str = None
    host: str = None
    username: str = None
    password: str = None
    port: str = None
    database: str = None
    
    def __post_init__(self):
        if hasattr(super(), '__post_init__'):
            super().__post_init__()

        if self.connection_string in [None, "", " ", False]:
            self.connection_string = f'mysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'

class MySQLSource(SourceInterface):
    ConnectionSettingsClass = MySQLSourceConnectionSettings
    SourceConfigClass = MySQLSourceConfig
    
    """
        MySQL source implementation
        connection: SQLAlchemy Connection
        source_config:
            # Data Retrieval
            query: str
            columns: List[DataColumn]
                        
            # Data Insertion
            table: str
            columns: List[DataColumn]
                    
        connection_settings:
            # Either connection string
            connection_string: str
            
            # Or host/username/password
            host: str
            username: str
            password: str
            port: int
            database: str

    """
    def _configure(self):
        if getattr(self.connection_settings, 'connection_string', None) in [None, "", " ", False]:
            self.connection_settings.connection_string = f'mysql://{self.connection_settings.username}:{self.connection_settings.password}@{self.connection_settings.host}:{self.connection_settings.port}/{self.connection_settings.database}'

    def query(self, *args, **kwargs) -> POLARS_DF:        
        df = pl.read_sql(
            sql=self.source_config.query,
            connection_uri=self.connection_settings.connection_string
        )
        
        df.with_columns([col.name for col in self.source_config.columns])
        
        for col in self.source_config.columns:
            df = df.with_column(pl.col(col.name).cast(col.type, strict=False))
        
        return df
    
    def insert(self, data: POLARS_DF):
        pass
    
    def update(self, data: POLARS_DF):
        pass
    
    def delete(self, data: POLARS_DF):
        pass