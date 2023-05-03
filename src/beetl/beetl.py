from typing import List, Union
from .config import BeetlConfig
from polars import DataFrame as POLARS_DF
from time import perf_counter

BENCHMARK = []

class Beetl:
    """The main class for BeETL. This class is responsible for orchestrating the ETL process."""
    config: BeetlConfig = None
    """Holds the BeETL Configuration"""
    
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
    
    @staticmethod
    def compare_datasets(source: POLARS_DF, destination: POLARS_DF, keys: List[str] = ["id"], columns: List[str] = []) -> List[POLARS_DF]:
        """
        This function uses polars DataFrames to quickly compare two datasets and return the differences.
        
        Polars is roughly 10-50x faster than Pandas for this task, but this can vary depending on the dataset.

        Args:
            source (POLARS_DF): The source dataset
            destination (POLARS_DF): The destination dataset
            keys (List[str], optional): Unique keys in the dataset. Defaults to ["id"].
            columns (List[str], optional): List of columns to compare for differences. Defaults to [].

        Returns:
            List[POLARS_DF]: A list of Polars Dataframes (Insert, Update, Delete) containing the differences between source and destination
        """
        
        if isinstance(source, Union[list, set, tuple]):
            source = POLARS_DF(source)
        
        if isinstance(destination, Union[list, set, tuple]):
            destination = POLARS_DF(destination)
        
        # If source is empty, delete all in destination
        if len(source) == 0:
            return source, source, destination
        
        # If destination is empty, create all from source
        if len(destination) == 0:
            return source, destination, destination
        
        # Get all columns from destination if none are specified
        columns = destination.columns if len(columns) == 0 else columns
        
        # Get rows that only exist in source (Creates)
        create = source.join(destination, on=keys, how="anti")
        
        # Get rows that exist in both and have differing values (Updates)
        update = source.join(destination, on=keys, how="semi").join(destination, on=columns, how="anti")
        
        # Get rows that only exist in destination (Deletes)
        delete = destination.join(source, on=keys, how="anti")
        
        return (
            create.select(keys + columns), 
            update.select(keys + columns), 
            delete.select(keys)
        )
    
    def benchmark(self, text: str) -> None:
        """Inserts a benchmark into the log"""
        BENCHMARK.append({
            "text": text,
            "perf": perf_counter()
        })

        if len(BENCHMARK) > 1:
            print(
                BENCHMARK[-1]['text'] + 
                ": " + 
                str(round(BENCHMARK[-1]['perf'] - BENCHMARK[-2]['perf'], 5)))
    
    def sync(self) -> None:
        """Executes the ETL process. The following steps will be performed:
        
        1. Load source and destination data
        
        2. Format source data to be compatible with destination through field and source transformers
        
        3. Compare source and destination data with compare_datasets
        
        4. Execute the respective insert, update and delete queries
        
        """
        self.benchmark("Starting sync and retrieving source data")
        for sync in self.config.sync_list:
            source_data = sync.source.query()
            self.benchmark("Finished data retrieval from source")
            destination_data = sync.destination.query()
            self.benchmark("Finished data retrieval from destination")
            
            self.benchmark("Starting data transformation")
            transformedSource = source_data
            if sync.sourceTransformer:
                transformedSource = sync.sourceTransformer.transform(source_data)
            self.benchmark("Finished source data transformation")
            
            if sync.fieldTransformers is not None:
                for transformer in sync.fieldTransformers:
                    transformedSource = transformer.transform(transformedSource)
            self.benchmark("Finished field data transformation")
                       
            unique = [
                x.name for x in sync.destination.source_configuration.columns 
                if x.unique is True
            ]
            columns = [
                x.name for x in sync.destination.source_configuration.columns 
                if (x.skip_update or x.unique) is False 
            ]
            
            self.benchmark("Starting comparison")
            create, update, delete = self.compare_datasets(
                transformedSource, 
                destination_data, 
                unique, 
                columns
            )
            self.benchmark("Successfully extracted operations from dataset")
            
            amount = {}
            
            self.benchmark("Starting database operations")
            amount['inserts'] = sync.destination.insert(create)
            self.benchmark("Finished inserts, starting updates")
            amount['updates'] = sync.destination.update(update)
            self.benchmark("Finished updates, starting deletes")
            amount['deletes'] = sync.destination.delete(delete)
            self.benchmark("Finished deletes, sync finished")
            
            print("Inserted: " + str(amount['inserts']))
            print("Updated: " + str(amount['updates']))
            print("Deleted: " + str(amount['deletes']))
            
            return amount
