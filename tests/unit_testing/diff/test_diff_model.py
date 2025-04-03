import json
from datetime import datetime
from unittest import TestCase
from uuid import uuid4

from src.beetl.diff.diff_model import Diff, DiffRow, DiffUpdate


class DiffModelUnitTests(TestCase):
    def test_dump_json__when_all_fields_are_populated__returns_serialised_model(self):
        # arrange
        insert = DiffRow({"id": 1, "name": "test1"})
        update = DiffUpdate(
            DiffRow({"id": 2, "name": "test2"}),
            DiffRow({"id": 2, "name": "test2-updated"}),
        )
        delete = DiffRow({"id": 2})
        diff = Diff(
            "name",
            (update,),
            (insert,),
            (delete,),
        )

        # act
        result_json = diff.dump_json()

        # assert
        expected_json = """{"name": "name", "date": "2025-04-03T13:43:42.372858", "uuid": "53d684c1-5639-4efe-a455-760ff7494f1f", "version": "1.0.0", "updates": [{"old": {"id": 2, "name": "test2"}, "new": {"id": 2, "name": "test2-updated"}}], "inserts": [{"id": 1, "name": "test1"}], "deletes": [{"id": 2}], "stats": {"updates": 1, "inserts": 1, "deletes": 1}}"""

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
