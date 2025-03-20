import json
from datetime import datetime
from json import JSONEncoder
from typing import Any, Literal, Union
from uuid import uuid4

from polars import DataFrame


class DiffRowIdentifiers(dict[str, Union[str, int, float]]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DiffRowData(dict[str, Any]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DiffUpdate:
    identifiers: DiffRowIdentifiers
    old: DiffRowData
    new: DiffRowData

    def __init__(
        self, identifiers: DiffRowIdentifiers, old: DiffRowData, new: DiffRowData
    ):
        self.identifiers, self.old, self.new = identifiers, old, new

    class JsonEncoder(JSONEncoder):
        def default(self, o: Any):
            if isinstance(o, DiffUpdate):
                return {"identifiers": o.identifiers, "old": o.old, "new": o.new}

            return super().default(o)


class DiffInsert:
    identifiers: DiffRowIdentifiers
    data: DiffRowData

    def __init__(self, identifiers: DiffRowIdentifiers, data: DiffRowData):
        self.identifiers, self.data = identifiers, data

    class JsonEncoder(JSONEncoder):
        def default(self, o: Any):
            if isinstance(o, DiffInsert):
                return {
                    "identifiers": o.identifiers,
                    "data": o.data,
                }

            return super().default(o)


class DiffDelete:
    identifiers: DiffRowIdentifiers

    def __init__(self, identifiers: DiffRowIdentifiers):
        self.identifiers = identifiers

    class JsonEncoder(JSONEncoder):
        def default(self, o: Any):
            if isinstance(o, DiffDelete):
                return {
                    "identifiers": o.identifiers,
                }
            return super().default(o)


class DiffStats:
    updates: int
    updated_fields: tuple[str, ...] = ()
    inserts: int
    deletes: int

    def __init__(self, updates: int, inserts: int, deletes: int):
        self.updates, self.inserts, self.deletes = (
            len(updates),
            len(inserts),
            len(deletes),
        )
        if len(updates):
            self.updated_fields = tuple(updates[0].new.keys())

    class JsonEncoder(JSONEncoder):
        def default(self, o: Any):
            if isinstance(o, DiffStats):
                return {
                    "updates": o.updates,
                    "inserts": o.inserts,
                    "deletes": o.deletes,
                    "updated_fields": o.updated_fields,
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

    def __init__(
        self,
        name: str,
        updates: tuple[DiffInsert, ...],
        inserts: tuple[DiffInsert, ...],
        deletes: tuple[DiffDelete, ...],
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
                "inserts": [
                    DiffInsert.JsonEncoder().default(insert) for insert in o.inserts
                ],
                "deletes": [
                    DiffDelete.JsonEncoder().default(delete) for delete in o.deletes
                ],
                "stats": DiffStats.JsonEncoder().default(o.stats),
            }
        return super().default(o)
