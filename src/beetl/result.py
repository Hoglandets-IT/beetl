from typing import List, Tuple


class Result:
    updates = 0
    inserts = 0
    deletes = 0
    names = []

    def __str__(self) -> str:
        return f"Result: {self.inserts} inserts, {self.updates} updates, {self.deletes} deletes across {len(self.names)} syncs"

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other) -> bool:
        if not issubclass(type(other), Result):
            return NotImplemented

        return (self.inserts, self.updates, self.deletes) == (
            other.inserts,
            other.updates,
            other.deletes,
        )


class SyncResult(Result):
    def __init__(self, results: List[Tuple[str, int, int, int]] = []):
        super().__init__()
        for result in results:
            self.names.append(result[0])
            self.inserts += result[1] or 0
            self.updates += result[2] or 0
            self.deletes += result[3] or 0
