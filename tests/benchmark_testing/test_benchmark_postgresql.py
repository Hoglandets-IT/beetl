from time import perf_counter

from src.beetl import beetl
from tests.benchmark_testing.tools.benchmark_base import BenchmarkBase
from tests.benchmark_testing.tools.data import BenchmarkData
from testcontainers.postgres import PostgresContainer

from tests.configurations import generate_from_postgres_to_postgres


class BenchmarkPostgresqlTest(BenchmarkBase):
    name = "PostgreSQL"

    def run_for_amount(self, amount) -> float:
        """ This method is run through the base class"""
        with PostgresContainer(driver=None) as postgresql:
            config = generate_from_postgres_to_postgres(
                postgresql.get_connection_url())
            beetl_instance = beetl.Beetl(beetl.BeetlConfig(config))

            src, dst = BenchmarkData.generateData(amount)

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

            start = perf_counter()
            beetl_instance.sync()
            tim = perf_counter() - start

            return tim
