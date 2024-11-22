from time import perf_counter
import unittest

from src.beetl import beetl
from tests.benchmark_testing.tools.data import BenchmarkData
from testcontainers.postgres import PostgresContainer

from tests.benchmark_testing.tools.settings import AMOUNT_OF_ROWS_PER_BENCHMARK
from tests.benchmark_testing.tools.thresholds import BENCHMARK_THRESHOLDS_IN_SECONDS
from tests.configurations import generate_from_postgres_to_postgres


class BenchmarkPostgresqlTest(unittest.TestCase):

    def test_benchmark(self):
        with PostgresContainer(driver=None) as postgresql:
            config = generate_from_postgres_to_postgres(
                postgresql.get_connection_url())
            beetl_instance = beetl.Beetl(beetl.BeetlConfig(config))
            print("Starting benchmark of PostgreSQL")
            print(f"Running test with {AMOUNT_OF_ROWS_PER_BENCHMARK} rows")
            start_gen = perf_counter()
            src, dst = BenchmarkData.generateData(AMOUNT_OF_ROWS_PER_BENCHMARK)

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

            print(f'Finished {result.inserts} inserts,'
                  f' {result.updates} updates'
                  f' and {result.deletes} deletes in {tim}s')

            self.assertLessEqual(
                tim, BENCHMARK_THRESHOLDS_IN_SECONDS[AMOUNT_OF_ROWS_PER_BENCHMARK])

            return tim
