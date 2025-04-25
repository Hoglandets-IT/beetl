from time import perf_counter
from typing import List, Union

from tabulate import tabulate

from .compare.compare import Difftool
from .comparison_result import ComparisonResult
from .config import BeetlConfig
from .diff import DiffCalculator
from .result import Result, SyncResult
from .transformers import run_transformers

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

    def benchmark(self, text: str) -> None:
        """Inserts a benchmark into the log"""
        BENCHMARK.append({"text": text, "perf": perf_counter()})

        if len(BENCHMARK) > 1:
            print(
                BENCHMARK[-1]["text"]
                + ": "
                + str(round(BENCHMARK[-1]["perf"] - BENCHMARK[-2]["perf"], 5))
            )

    def sync(
        self, dry_run: bool = False, generate_update_diff: bool = False
    ) -> Union[Result, List[ComparisonResult]]:
        """Executes the ETL process.

        Args:
            dry_run (bool, optional): If set to true, the function will not execute any database operations and will return the dataframes that would have been used in the operations. Defaults to False.
            generate_update_diff (bool, optional): If set to true, the function will return the differences between the source and destination update dataframes. Defaults to False.

        Returns:
            ComparisonResult: If the argument dry_run was passed as true, returns a list of ComparisionResult objects that contain the create, update and delete dataframes that would have been used by the destination if the dry_run argument was false.
            Result: A Result object containing the amount of inserts, updates and deletes for each sync
            list[tuple[list[DataFrame], set[str]]]: If generate_update_diff is set to true, returns a list of tuples containing the differences between the source and destination update dataframes and a set with the keys that have differences across the whole dataframe.

        The following steps will be performed:

        1. Load source and destination data

        2. Format source data to be compatible with destination through field and source transformers

        3. Compare source and destination data with compare_datasets

        4. Execute the respective insert, update and delete queries

        """
        dry_run_results = []
        generate_update_diff_results = []
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
            transformedSource = run_transformers(source_data, sync.sourceTransformers)
            self.benchmark(
                "Finished source data transformation, starting destination transformation"
            )
            transformedDestination = run_transformers(
                destination_data, sync.destinationTransformers
            )

            self.benchmark("Finished data transformation before comparison")

            self.benchmark("Starting comparison")
            unique_columns = tuple(
                column.name for column in sync.comparisonColumns if column.unique
            )
            comparison_columns = tuple(
                column.name
                for column in sync.comparisonColumns
                if column.name not in unique_columns
            )

            if len(unique_columns) == 0:
                raise ValueError(
                    "You need to specify at least one unique column in the sync.comparisonColumns field"
                )

            diff_calculator = DiffCalculator(
                sync.name,
                transformedSource,
                transformedDestination,
                sync.comparisonColumns,
            )
            create, update, delete = diff_calculator.get_create_update_delete_for_sync()
            self.benchmark("Successfully extracted operations from dataset")

            load_and_compare = perf_counter() - start
            print(f"Load and compare took {load_and_compare} seconds")

            amount = {}

            print(
                f"Insert: {len(create)}, Update: {len(update)}, Delete: {len(delete)}"
            )

            if generate_update_diff:
                generate_update_diff_results.append(
                    Difftool.diff_update(
                        update,
                        transformedDestination,
                        comparison_columns,
                        unique_columns,
                    )
                )
                continue

            if dry_run:
                dry_run_results.append(
                    ComparisonResult(
                        run_transformers(create, sync.insertionTransformers),
                        run_transformers(update, sync.insertionTransformers),
                        run_transformers(delete, sync.deletionTransformers),
                    )
                )
                sync.destination.disconnect()
                continue

            if sync.diff_destination_instance is not None:
                diff = diff_calculator.create_diff(sync.diff_transformers)
                sync.diff_destination_instance.connect()
                sync.diff_destination_instance.store_diff(diff)

            self.benchmark("Starting database operations")
            self.benchmark("Starting deletes")
            amount["deletes"] = 0
            if len(delete):
                amount["deletes"] = sync.destination.delete(
                    run_transformers(delete, sync.deletionTransformers)
                )

            self.benchmark("Finished deletes, starting inserts")
            amount["inserts"] = 0
            if len(create):
                amount["inserts"] = sync.destination.insert(
                    run_transformers(create, sync.insertionTransformers)
                )

            self.benchmark("Finished inserts, starting updates")

            amount["updates"] = 0
            if len(update):
                amount["updates"] = sync.destination.update(
                    run_transformers(update, sync.insertionTransformers)
                )

            self.benchmark("Finished updates, sync finished")

            print("Inserted: " + str(amount["inserts"]))
            print("Updated: " + str(amount["updates"]))
            print("Deleted: " + str(amount["deletes"]))

            allAmounts.append(
                [sync.name, *[amount["inserts"], amount["updates"], amount["deletes"]]]
            )

            sync.destination.disconnect()
            if sync.diff_destination_instance:
                sync.diff_destination_instance.disconnect()

        if generate_update_diff:
            return generate_update_diff_results

        if dry_run:
            return dry_run_results

        print(
            "\r\n\r\n"
            + tabulate(allAmounts, headers=["Sync", "Inserts", "Updates", "Deletes"])
        )

        return SyncResult(allAmounts)
