from polars import DataFrame
from .result import Result


class ComparisonResult:
    create: DataFrame
    update: DataFrame
    delete: DataFrame

    def __init__(self, create: DataFrame, update: DataFrame, delete: DataFrame):
        self.create = create
        self.update = update
        self.delete = delete

    def __str__(self):
        return f"Create: {self.create.height}, Update: {self.update.height}, Delete: {self.delete.height}"

    def __repr__(self):
        return str(self)

    def to_beetl_result(self):
        result = Result()
        result.inserts = self.create.height
        result.updates = self.update.height
        result.deletes = self.delete.height
        result.names = ("dry run",)
        return result
