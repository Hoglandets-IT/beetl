from polars import DataFrame


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
