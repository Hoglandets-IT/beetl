import json
from datetime import datetime
from json import JSONEncoder
from typing import Any, Literal
from uuid import uuid4

DiffRow = dict[str, Any]


class DiffUpdate:
    old: DiffRow
    new: DiffRow

    def __init__(self, old: DiffRow, new: DiffRow):
        self.old = old
        self.new = new

    class JsonEncoder(JSONEncoder):
        def default(self, o: Any):
            if isinstance(o, DiffUpdate):
                return {"old": o.old, "new": o.new}

            return super().default(o)

    def dump_json(self):
        return json.dumps(self, cls=JSONEncoder)

    def to_dict(self) -> dict[str, DiffRow]:
        return {
            "old": self.old,
            "new": self.new,
        }


class DiffStats:
    updates: int
    inserts: int
    deletes: int

    def __init__(self, updates: int, inserts: int, deletes: int):
        self.updates, self.inserts, self.deletes = (
            len(updates),
            len(inserts),
            len(deletes),
        )

    class JsonEncoder(JSONEncoder):
        def default(self, o: Any):
            if isinstance(o, DiffStats):
                return {
                    "updates": o.updates,
                    "inserts": o.inserts,
                    "deletes": o.deletes,
                }
            return super().default(o)

    def dump_json(self):
        return json.dumps(self, cls=JSONEncoder)

    def to_dict(self) -> dict[str, int]:
        return {
            "updates": self.updates,
            "inserts": self.inserts,
            "deletes": self.deletes,
        }


class Diff:
    name: str
    date: datetime
    uuid: str
    version: Literal["1.0.0"] = "1.0.0"
    updates: tuple[DiffUpdate, ...]
    inserts: tuple[DiffRow, ...]
    deletes: tuple[DiffRow, ...]
    stats: DiffStats

    def __init__(
        self,
        name: str,
        updates: tuple[DiffUpdate, ...],
        inserts: tuple[DiffRow, ...],
        deletes: tuple[DiffRow, ...],
    ):
        self.name = name
        self.date = datetime.now()
        self.uuid = uuid4()
        self.updates = updates
        self.inserts = inserts
        self.deletes = deletes
        self.stats = DiffStats(updates, inserts, deletes)

    def dump_json(self):
        return json.dumps(self, cls=DiffJsonEncoder)

    def date_as_string(self) -> str:
        return self.date.isoformat()


class DiffJsonEncoder(JSONEncoder):
    def default(self, o: Any):
        if isinstance(o, Diff):
            return {
                "name": o.name,
                "date": o.date.isoformat(),
                "uuid": str(o.uuid),
                "version": o.version,
                "updates": [
                    DiffUpdate.JsonEncoder().default(update) for update in o.updates
                ],
                "inserts": o.inserts,
                "deletes": o.deletes,
                "stats": DiffStats.JsonEncoder().default(o.stats),
            }
        return super().default(o)
