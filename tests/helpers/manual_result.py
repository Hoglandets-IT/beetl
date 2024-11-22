from src.beetl.result import Result


class ManualResult(Result):
    def __init__(self, inserts: int, updates: int, deletes: int):
        super().__init__()
        self.inserts = inserts
        self.updates = updates
        self.deletes = deletes
