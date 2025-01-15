from typing import List, Union

from .comparison_result import ComparisonResult
from .sources.interface import CASTABLE
from .result import Result, SyncResult
from .config import BeetlConfig, ComparisonColumn, SyncConfiguration
from .transformers.interface import TransformerConfiguration
import polars as pl
from time import perf_counter
from tabulate import tabulate

BENCHMARK = []


class Beetl:
    """The main class for BeETL. This class is responsible for orchestrating the ETL process."""

    config: Union[BeetlConfig, None] = None
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
        columns: List[ComparisonColumn] = [],
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
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

        source = Beetl._initialize_columns_if_empty(source, columns)
        source = Beetl._cast_columns_to_types(source, columns)
        destination = Beetl._initialize_columns_if_empty(destination, columns)
        destination = Beetl._cast_columns_to_types(destination, columns)
        column_names = [col.name for col in columns]

        for column in column_names:

            if column not in source.columns and column not in destination.columns:
                raise Exception(
                    f"Column {column} does not exist in any of the datasets"
                )

            if column not in source.columns and len(source) > 0:
                source = source.with_columns(
                    pl.lit(None).alias(column).cast(destination[column].dtype)
                )

            if column not in destination.columns and len(destination) > 0:
                destination = destination.with_columns(
                    pl.lit(None).alias(column).cast(source[column].dtype)
                )

        columns_contain_list_column = any(
            [
                True
                for name in column_names
                if type(source.get_column(name).dtype) == pl.List
            ]
        )
        if columns_contain_list_column:
            raise ValueError(
                "Beetl does not support comparing list columns, please remove them from the sync.comparisonColumns field. If you didn't specify any non unique columns, Beetl will compare all columns by default. Please specify the columns you want to compare in the sync.comparisonColumns field."
            )

        # If source is empty, delete all in destination
        if len(source) == 0:
            return source, source, destination

        # If destination is empty, create all from source
        if len(destination) == 0:
            return (source, destination, destination)
        try:
            # Get rows that only exist in source (Creates)
            create = source.join(destination, on=keys, how="anti")

            # Get rows that exist in both and have differing values (Updates)
            update = source.join(destination, on=keys, how="semi").join(
                destination, on=column_names, how="anti", join_nulls=True
            )

            # Get rows that only exist in destination (Deletes)
            delete = destination.join(source, on=keys, how="anti")

        except pl.ColumnNotFoundError as e:
            raise ValueError(
                "One or more comparison columns do not exist \n"
                f"Source columns: {','.join(source.columns)} \n"
                f"Destination columns: {','.join(destination.columns)} \n"
                f"Comparison columns: {','.join(column_names)} \n"
            ) from e

        try:
            comparison_results = (
                create.select(source.columns),
                update.select(source.columns),
                delete.select(destination.columns),
            )
        except Exception:
            raise Exception(
                "Could not create comparison results. Most likely due to a mismatch in column names between source and destination."
            )

        return comparison_results

    @staticmethod
    def _initialize_columns_if_empty(source, columns):
        if len(source) == 0 and source.width == 0:
            for col in columns:
                source = source.with_columns(
                    pl.Series(col.name, dtype=col.type))
        return source

    @staticmethod
    def _cast_columns_to_types(source, columns):
        for col in columns:
            if col.name in source.columns and col.type in CASTABLE:
                source = source.with_columns(pl.col(col.name).cast(col.type))
        return source

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

    def sync(self, dry_run: bool = False) -> Union[Result, List[ComparisonResult]]:
        """Executes the ETL process.

        Args:
            dry_run (bool, optional): If set to true, the function will not execute any database operations and will return the dataframes that would have been used in the operations. Defaults to False.

        Returns:
            ComparisonResult: If the argument dry_run was passed as true, returns a list of ComparisionResult objects that contain the create, update and delete dataframes that would have been used by the destination if the dry_run argument was false.
            Result: A Result object containing the amount of inserts, updates and deletes for each sync

        The following steps will be performed:

        1. Load source and destination data

        2. Format source data to be compatible with destination through field and source transformers

        3. Compare source and destination data with compare_datasets

        4. Execute the respective insert, update and delete queries

        """
        dry_run_results = []
        self.benchmark("Starting sync and retrieving source data")
        allAmounts = []
        for i, sync in enumerate(self.config.sync_list, 1):
            start = perf_counter()
            if sync.name != "":
                print(f"Starting sync: {sync.name}")
            else:
                print(f"Starting sync {i}")
            sync.source.connect()
            source_data = sync.source.query(sync.sourceConfig)
            sync.source.disconnect()
            self.benchmark("Finished data retrieval from source")

            sync.destination.connect()
            destination_data = sync.destination.query(sync.destinationConfig)
            self.benchmark("Finished data retrieval from destination")

            self.benchmark("Starting source data transformation")
            transformedSource = self.runTransformers(
                source_data, sync.sourceTransformers, sync
            )
            self.benchmark(
                "Finished source data transformation, starting destination transformation"
            )
            transformedDestination = self.runTransformers(
                destination_data, sync.destinationTransformers, sync
            )

            self.benchmark("Finished data transformation before comparison")

            self.benchmark("Starting comparison")
            unique_columns = [
                column.name for column in sync.comparisonColumns if column.unique
            ]
            if len(unique_columns) == 0:
                raise ValueError(
                    "You need to specify at least one unique column in the sync.comparisonColumns field"
                )
            create, update, delete = self.compare_datasets(
                transformedSource,
                transformedDestination,
                unique_columns,
                sync.comparisonColumns,
            )
            self.benchmark("Successfully extracted operations from dataset")

            load_and_compare = perf_counter() - start
            print(f"Load and compare took {load_and_compare} seconds")

            amount = {}

            print(
                f"Insert: {len(create)}, Update: {len(update)}, Delete: {len(delete)}"
            )

            if dry_run:
                dry_run_results.append(
                    ComparisonResult(
                        self.runTransformers(
                            create, sync.insertionTransformers, sync),
                        self.runTransformers(
                            update, sync.insertionTransformers, sync),
                        self.runTransformers(
                            delete, sync.deletionTransformers, sync),
                    )
                )
                sync.destination.disconnect()
                continue

            self.benchmark("Starting database operations")
            self.benchmark("Starting deletes")
            amount["deletes"] = 0
            if len(delete):
                amount["deletes"] = sync.destination.delete(
                    self.runTransformers(
                        delete, sync.deletionTransformers, sync)
                )

            self.benchmark("Finished deletes, starting inserts")
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

            self.benchmark("Finished updates, sync finished")

            print("Inserted: " + str(amount["inserts"]))
            print("Updated: " + str(amount["updates"]))
            print("Deleted: " + str(amount["deletes"]))

            allAmounts.append(
                [sync.name, *[amount["inserts"], amount["updates"], amount["deletes"]]])

            sync.destination.disconnect()

        if dry_run:
            return dry_run_results

        print(
            "\r\n\r\n"
            + tabulate(allAmounts,
                       headers=["Sync", "Inserts", "Updates", "Deletes"])
        )

        return SyncResult(allAmounts)
