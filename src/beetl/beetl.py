from typing import List
from polars import DataFrame as POLARS_DF
from .config import Config

class Beetl:
    config: Config = None
    """
    The main class for Beetl
    """
    def __init__(self, config: Config):
        self.config = config
    
    @classmethod
    def from_yaml(cls, path: str, encoding: str = 'utf-8'):
        return cls(Config.from_yaml_file(path, encoding))
    
    @classmethod
    def from_json(cls, path: str, encoding: str = 'utf-8'):
        return cls(Config.from_json_file(path, encoding))
    
    def get_iud(self, source: POLARS_DF, destination: POLARS_DF, keys = ["id"], columns = []) -> List[POLARS_DF]: 
        # If source is empty, delete all in destination
        if len(source) == 0:
            return source, source, destination
        
        # If destination is empty, create all from source
        if len(destination) == 0:
            return source, destination, destination
        
        # Get all columns from source if none are specified
        columns = source.columns if len(columns) == 0 else columns
        
        # Get rows that only exist in source (Creates)
        create = source.join(destination, on=keys, how="anti")
        
        # Get rows that exist in both and have differing values (Updates)
        update = source.join(destination, on=keys, how="semi").join(destination, on=columns, how="anti")
        
        # Get rows that only exist in destination (Deletes)
        delete = destination.join(source, on=keys, how="anti")
        
        return create, update, delete
    
    def sync(self):
        # 1: Get data from source
        # 2: Format source data
        # 3: Get data from destination
        # 4: Compare datasets and generate IUD
        # 5: Run IUDs
        for sync in self.config.sync:
            source_data = sync.source.query()
            destination_data = sync.destination.query()
        
            create, update, delete = self.get_iud(source_data, destination_data, )
            print(create)
            print(update)
            print(delete)