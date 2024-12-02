from typing import List, Union

from src.beetl.result import Result, SyncResult
from .config import BeetlConfig, SyncConfiguration
from .transformers.interface import TransformerConfiguration
import polars as pl
from time import perf_counter
from tabulate import tabulate

BENCHMARK = []


class Beetl:
    """The main class for BeETL. This class is responsible for orchestrating the ETL process."""

    config: BeetlConfig = None
    """Holds the BeETL Configuration"""

    def __init__(self, config: BeetlConfig):
        self.config = config

    @classmethod
    def from_yaml(cls, path: str, encoding: str = "utf-8") -> "Beetl":
        """Creates a Beetl instance from a YAML-formatted configuration file

        Args:
            path (str): The path to the file
            encoding (str, optional): The charset for the file. Defaults to 'utf-8'.

        Returns:
            Beetl: An instance of Beetl
        """
        return cls(BeetlConfig.from_yaml_file(path, encoding))

    @classmethod
    def from_json(cls, path: str, encoding: str = "utf-8") -> "Beetl":
        """Creates a Beetl instance from a JSON-formatted configuration file

        Args:
            path (str): The path to the file
            encoding (str, optional): The charset for the file. Defaults to 'utf-8'.

        Returns:
            Beetl: _description_
        """
        return cls(BeetlConfig.from_json_file(path, encoding))

    @staticmethod
    def compare_datasets(
        source: pl.DataFrame,
        destination: pl.DataFrame,
        keys: List[str] = ["id"],
        columns: List[str] = [],
    ) -> List[pl.DataFrame]:
        """
        This function uses polars DataFrames to quickly compare two datasets and return the differences.

        Polars is roughly 10-50x faster than Pandas for this task, but this can vary depending on the dataset.

        Args:
            source (pl.DataFrame): The source dataset
            destination (pl.DataFrame): The destination dataset
            keys (List[str], optional): Unique keys in the dataset. Defaults to ["id"].
            columns (List[str], optional): List of columns to compare for differences. Defaults to [].

        Returns:
            List[pl.DataFrame]: A list of Polars Dataframes (Insert, Update, Delete) containing the differences between source and destination
        """

        if isinstance(source, Union[list, set, tuple]):
            source = pl.DataFrame(source)

        if isinstance(destination, Union[list, set, tuple]):
            destination = pl.DataFrame(destination)

        # Get all columns from destination if none are specified
        columns = destination.columns if len(columns) == 0 else columns

        for column in columns:

            if column not in source.columns and column not in destination.columns:
                raise Exception(
                    f"Column {column} does not exist in any of the datasets")

            if column not in source.columns and len(source) > 0:
                source = source.with_columns(pl.lit(None).alias(
                    column).cast(destination[column].dtype))

            if column not in destination.columns and len(destination) > 0:
                destination = destination.with_columns(
                    pl.lit(None).alias(column).cast(source[column].dtype))

        # If source is empty, delete all in destination
        if len(source) == 0:
            return source, source, destination

        # If destination is empty, create all from source
        if len(destination) == 0:
            return (
                source,
                destination,
                destination
            )
        try:
            # Get rows that only exist in source (Creates)
            create = source.join(destination, on=keys, how="anti")

            # Get rows that exist in both and have differing values (Updates)
            update = source.join(destination, on=keys, how="semi").join(
                destination, on=columns, how="anti"
            )

            # Get rows that only exist in destination (Deletes)
            delete = destination.join(source, on=keys, how="anti")

        except pl.ColumnNotFoundError as e:
            raise ValueError(
                "One or more comparison columns do not exist \n"
                f"Source columns: {','.join(source.columns)} \n"
                f"Destination columns: {','.join(destination.columns)} \n"
                f"Comparison columns: {','.join(columns)} \n"
            ) from e

        try:
            comparison_results = (
                create.select(source.columns),
                update.select(source.columns),
                delete.select(source.columns)
            )
        except Exception:
            raise Exception(
                "Could not create comparison results. Most likely due to a mismatch in column names between source and destination.")

        return comparison_results

    def benchmark(self, text: str) -> None:
        """Inserts a benchmark into the log"""
        BENCHMARK.append({"text": text, "perf": perf_counter()})

        if len(BENCHMARK) > 1:
            print(
                BENCHMARK[-1]["text"]
                + ": "
                + str(round(BENCHMARK[-1]["perf"] - BENCHMARK[-2]["perf"], 5))
            )

    def runTransformers(
        self,
        source: pl.DataFrame,
        transformers: List[TransformerConfiguration],
        sync: SyncConfiguration,
    ) -> pl.DataFrame:
        transformed = source.clone()

        if transformers is not None and len(transformers) > 0:
            for transformer in transformers:
                if transformer.include_sync:
                    transformed = transformer.transform(transformed, sync=sync)
                    continue

                transformed = transformer.transform(transformed)

        return transformed

    def sync(self) -> Result:
        """Executes the ETL process. The following steps will be performed:

        1. Load source and destination data

        2. Format source data to be compatible with destination through field and source transformers

        3. Compare source and destination data with compare_datasets

        4. Execute the respective insert, update and delete queries

        """
        self.benchmark("Starting sync and retrieving source data")
        allAmounts = []
        for i, sync in enumerate(self.config.sync_list, 1):
            start = perf_counter()
            if sync.name != "":
                print(f"Starting sync: {sync.name}")
            else:
                print(f"Starting sync {i}")
            source_data = sync.source.query(sync.sourceConfig)
            self.benchmark("Finished data retrieval from source")

            destination_data = sync.destination.query(sync.destinationConfig)
            self.benchmark("Finished data retrieval from destination")

            self.benchmark("Starting source data transformation")
            transformedSource = self.runTransformers(
                source_data, sync.sourceTransformers, sync
            )
            self.benchmark(
                "Finished source data transformation, starting destination transformation")
            transformedDestination = self.runTransformers(
                destination_data, sync.destinationTransformers, sync
            )

            self.benchmark("Finished data transformation before comparison")

            self.benchmark("Starting comparison")
            create, update, delete = self.compare_datasets(
                transformedSource,
                transformedDestination,
                sync.destination.source_configuration.unique_columns,
                sync.destination.source_configuration.comparison_columns,
            )
            self.benchmark("Successfully extracted operations from dataset")

            load_and_compare = perf_counter() - start
            print(f"Load and compare took {load_and_compare} seconds")

            amount = {}

            print(
                f"Insert: {len(create)}, Update: {len(update)}, Delete: {len(delete)}"
            )

            self.benchmark("Starting database operations")
            amount["inserts"] = 0
            if len(create):
                amount["inserts"] = sync.destination.insert(
                    self.runTransformers(
                        create, sync.insertionTransformers, sync)
                )

            self.benchmark("Finished inserts, starting updates")
            amount["updates"] = 0
            if len(update):
                amount["updates"] = sync.destination.update(
                    self.runTransformers(
                        update, sync.insertionTransformers, sync)
                )

            self.benchmark("Finished updates, starting deletes")
            amount["deletes"] = 0
            if len(delete):

                amount["deletes"] = sync.destination.delete(
                    self.runTransformers(
                        delete, sync.deletionTransformers, sync)
                )

            self.benchmark("Finished deletes, sync finished")

            print("Inserted: " + str(amount["inserts"]))
            print("Updated: " + str(amount["updates"]))
            print("Deleted: " + str(amount["deletes"]))

            allAmounts.append([sync.name, *amount.values()])

        print("\r\n\r\n" + tabulate(allAmounts,
              headers=["Sync", "Inserts", "Updates", "Deletes"]))

        return SyncResult(allAmounts)
