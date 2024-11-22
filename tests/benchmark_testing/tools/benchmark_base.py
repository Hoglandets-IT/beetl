import statistics
import unittest

from tabulate import tabulate

AMOUNT = 10000
TIMES = 5


@unittest.skip("Don't run as part of test suite since it's a benchmark and does not test any functionality")
class BenchmarkBase(unittest.TestCase):

    name = "Replace this in subclass"

    def test_benchmark(self):
        print(f"Starting benchmark of {self.name}")
        amounts = [AMOUNT]*TIMES
        durations = []
        for amount in amounts:
            durations.append(self.run_for_amount(amount))

        print(f"\r\n\r\nResults: {self.name}\r\n")
        print(tabulate([[
            f"{AMOUNT}x{TIMES}",
            round(statistics.mean(durations), 4),
            round(max(durations), 4),
            round(min(durations), 4)]], headers=["amount", "meanTime", "maxTime", "minTime"]))

    def run_for_amount(self, amount) -> float:
        """Method to run the benchmark for a given amount of rows.
        Must return the time it took to run the benchmark.
        This is only implemented in the subclasses."""
        raise NotImplementedError
