import json
from datetime import datetime
from unittest import TestCase
from uuid import uuid4

from src.beetl.diff.diff_model import (
    Diff,
    DiffDelete,
    DiffInsert,
    DiffRowData,
    DiffRowIdentifiers,
    DiffUpdate,
)


class DiffModelUnitTests(TestCase):
    def test_dump_json__when_all_fields_are_populated__returns_serialised_model(self):
        # arrange
        insert = DiffInsert({"identifiers": {"id": 1}, "data": {"name": "test1"}})
        update = DiffUpdate(
            DiffRowIdentifiers({"id": 2}),
            DiffRowData({"name": "test2"}),
            DiffRowData({"name": "test2-updated"}),
        )
        delete = DiffDelete({"identifiers": {"id": 2}})
        diff = Diff(
            "name",
            (update,),
            (insert,),
            (delete,),
        )

        # act
        result_json = diff.dump_json()

        # assert
        expected_json = """{"name": "name", "date": "2025-03-21T09:14:39.329739", "uuid": "354e0687-4995-42c7-a908-530e14e1622c", "version": "1.0.0", "updates": [{"identifiers": {"id": 2}, "old": {"name": "test2"}, "new": {"name": "test2-updated"}}], "inserts": [{"identifiers": {"id": 1}, "data": {"name": "test1"}}], "deletes": [{"identifiers": {"id": 2}}], "stats": {"updates": 1, "inserts": 1, "deletes": 1, "updated_fields": ["name"]}}"""

        ## Result uuid and date is unique per diff instance and need to be aligned before comparison.
        known_uuid = str(uuid4())
        known_date = str(datetime.now())

        expected_dict = json.loads(expected_json)
        result_dict = json.loads(result_json)

        expected_dict["uuid"] = known_uuid
        result_dict["uuid"] = known_uuid

        expected_dict["date"] = known_date
        result_dict["date"] = known_date

        self.maxDiff = None

        self.assertEqual(expected_dict, result_dict)
