from typing import List
from .config import BeetlConfig
from polars import DataFrame as POLARS_DF

class Beetl:
    config: BeetlConfig = None
    """The main class for BeETL. This class is responsible for orchestrating the ETL process."""
    def __init__(self, config: BeetlConfig):
        self.config = config
    
    @classmethod
    def from_yaml(cls, path: str, encoding: str = 'utf-8') -> "Beetl":
        """Creates a Beetl instance from a YAML-formatted configuration file

        Args:
            path (str): The path to the file
            encoding (str, optional): The charset for the file. Defaults to 'utf-8'.

        Returns:
            Beetl: An instance of Beetl
        """
        return cls(BeetlConfig.from_yaml_file(path, encoding))
    
    @classmethod
    def from_json(cls, path: str, encoding: str = 'utf-8') -> "Beetl":
        """Creates a Beetl instance from a JSON-formatted configuration file

        Args:
            path (str): The path to the file
            encoding (str, optional): The charset for the file. Defaults to 'utf-8'.

        Returns:
            Beetl: _description_
        """
        return cls(BeetlConfig.from_json_file(path, encoding))
    
    def compare_datasets(self, source: POLARS_DF, destination: POLARS_DF, keys: List[str] = ["id"], columns: List[str] = []) -> List[POLARS_DF]:
        """Compares two datasets and returns the differences

        Args:
            source (POLARS_DF): The source dataset
            destination (POLARS_DF): The destination dataset
            keys (List[str], optional): Unique keys in the dataset. Defaults to ["id"].
            columns (List[str], optional): List of columns to compare for differences. Defaults to [].

        Returns:
            List[POLARS_DF]: A list of Polars Dataframes (Insert, Update, Delete) containing the differences between source and destination
        """
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
    
    def sync(self) -> None:
        """Executes the ETL process. The following steps will be performed:
        1. Load source and destination data
        2. Format source data to be compatible with destination through field and source transformers
        3. Compare source and destination data with compare_datasets
        4. Execute the respective insert, update and delete queries
        """
        for sync in self.config.sync_list:
            source_data = sync.source.query()
            destination_data = sync.destination.query()
            
            transformedSource = source_data
            if sync.sourceTransformer:
                transformedSource = sync.sourceTransformer.transform(source_data)

            if sync.fieldTransformers is not None:
                for transformer in sync.fieldTransformers:
                    transformedSource = transformer.transform(transformedSource)
                       
            unique = [x.name for x in sync.destination.source_configuration.columns if x.unique == True]
            columns = [x.name for x in sync.destination.source_configuration.columns if x.skip_update == False and x.unique == False]
                        
            create, update, delete = self.compare_datasets(transformedSource, destination_data, unique, columns)
            
            sync.destination.insert(create)
            sync.destination.update(update)
            sync.destination.delete(delete)
