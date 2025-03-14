import json
from datetime import datetime
from json import JSONEncoder
from typing import Any, Literal, Union
from uuid import uuid4


class DiffRowIdendifiers(dict[str, Union[str, int, float]]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DiffRowData(dict[str, Any]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DiffUpdate:
    identifiers: DiffRowIdendifiers
    old: DiffRowData
    new: DiffRowData


class DiffInsert:
    identifiers: DiffRowIdendifiers
    data: DiffRowData


class DiffDelete:
    identifiers: DiffRowIdendifiers


class DiffStats:
    updates: int
    inserts: int
    deletes: int

    def __init__(self, updates: int, inserts: int, deletes: int):
        self.updates, self.inserts, self.deletes = updates, inserts, deletes

    class JsonEncoder(JSONEncoder):
        def default(self, o: Any):
            if isinstance(o, DiffStats):
                return {
                    "updates": o.updates,
                    "inserts": o.inserts,
                    "deletes": o.deletes,
                }
            return super().default(o)


class Diff:
    name: str
    date: datetime
    uuid: str
    version: Literal["1.0.0"] = "1.0.0"
    updates: tuple[DiffUpdate, ...]
    inserts: tuple[DiffInsert, ...]
    deletes: tuple[DiffDelete, ...]
    stats: DiffStats

    def __init__(self, name: str):
        self.name = name
        self.date = datetime.now()
        self.uuid = uuid4()
        self.updates = ()
        self.inserts = ()
        self.deletes = ()
        self.stats = DiffStats(0, 0, 0)

    def dump_json(self):
        return json.dumps(self, cls=DiffJsonEncoder)


class DiffJsonEncoder(JSONEncoder):
    def default(self, o: Any):
        if isinstance(o, Diff):
            return {
                "name": o.name,
                "date": o.date.isoformat(),
                "uuid": str(o.uuid),
                "version": o.version,
                "updates": o.updates,
                "inserts": o.inserts,
                "deletes": o.deletes,
                "stats": json.dumps(o.stats, cls=DiffStats.JsonEncoder),
            }
        return super().default(o)
