from typing import List, Tuple


class Result:
    updates: int = 0
    inserts: int = 0
    deletes: int = 0
    names: Tuple[str, ...] = ()

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
        self.names = tuple([result[0] for result in results])
        for result in results:
            self.inserts += result[1] or 0
            self.updates += result[2] or 0
            self.deletes += result[3] or 0
