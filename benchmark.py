import statistics
import pandas as pd
from src.beetl import beetl
from tests.benchmark_testing.tools.mysql import MySQLBenchmark
from tests.benchmark_testing.tools.mssql import MsSQLBenchmark


def runBenchmark(kind):
    amounts = [
        100,
        1000,
        10000,
        20000,
        40000,
        # 60000,
        # 100000,
        # 150000,
        # 200000,
        # 300000
    ]
    betl = beetl.Beetl(beetl.BeetlConfig(kind.BASIC_CONFIG))
    times = []

    for amount in amounts:
        avv = []
        # for tr in range(0, 10):
        amn = kind.runTestnum(amount, betl)
        avv.append(amn)

        times.append({
            "amount": amount,
            "meanTime": round(statistics.mean(avv), 4),
            "maxTime": round(max(avv), 4),
            "minTime": round(min(avv), 4)
        })
        print(pd.DataFrame(times))

    print(pd.DataFrame(times))


if __name__ == '__main__':
    """
    podman run --rm -it -e MARIADB_ROOT_PASSWORD=password \
    -e MARIADB_DATABASE=database -p 3333:3306 mariadb:latest

    docker run --rm -it -e MARIADB_ROOT_PASSWORD=password \
    -e MARIADB_DATABASE=database -p 3333:3306 mariadb:latest
    """
    # runBenchmark(MySQLBenchmark)
    """
    podman run --rm -it -e ACCEPT_EULA=Y -e SA_PASSWORD=Password123 \
    -e DBNAME=database -p 3334:1433 mssql-server-test:latest
    """
    runBenchmark(MsSQLBenchmark)
