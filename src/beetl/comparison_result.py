from polars import DataFrame


class ComparisonResult:
    create: DataFrame
    update: DataFrame
    delete: DataFrame

    def __init__(self, create: DataFrame, update: DataFrame, delete: DataFrame):
        self.create = create
        self.update = update
        self.delete = delete
