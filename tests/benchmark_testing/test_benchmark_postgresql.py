from time import perf_counter
import unittest

from src.beetl import beetl
from tests.benchmark_testing.tools.data import BenchmarkData
from testcontainers.postgres import PostgresContainer

from tests.configurations import generate_from_postgres_to_postgres


class BenchmarkPostgresqlTest(unittest.TestCase):
    amount = 40000

    def test_benchmark(self):
        with PostgresContainer(driver=None) as postgresql:
            config = generate_from_postgres_to_postgres(
                postgresql.get_connection_url())
            beetl_instance = beetl.Beetl(beetl.BeetlConfig(config))
            print("Starting benchmark of PostgreSQL")
            print(f"Running test with {self.amount} rows")
            start_gen = perf_counter()
            src, dst = BenchmarkData.generateData(self.amount)

            src.to_sql(
                "srctable",
                postgresql.get_connection_url(),
                if_exists="replace"
            )

            dst.to_sql(
                "dsttable",
                postgresql.get_connection_url(),
                if_exists="replace"
            )
            fin = perf_counter() - start_gen
            print(f"Finished generation and insertion in {fin}s")

            start = perf_counter()
            result = beetl_instance.sync()
            tim = perf_counter() - start

            amounts = result[0]
            inserts = amounts[1]
            updates = amounts[2]
            deletes = amounts[3]

            print(f'Finished {inserts} inserts,'
                  f' {updates} updates'
                  f' and {deletes} deletes in {tim}s')

            return tim
