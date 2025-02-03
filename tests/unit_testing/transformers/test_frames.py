import json
from unittest import TestCase

from polars import DataFrame

from src.beetl.transformers.frames import FrameTransformer


class UnitTestFramesTransformers(TestCase):
    def test_extract_nested_rows__with_valid_input__nested_rows_are_extracted(
        self,
    ):
        data = DataFrame(
            [
                {
                    "id": 1,
                    "name": "A",
                    "items": [
                        {
                            "id": 3,
                            "name": "a",
                            "age": 30,
                            "favorite_color": "blue",
                        }
                    ],
                },
                {
                    "id": 2,
                    "name": "B",
                    "items": [
                        {
                            "id": 4,
                            "name": "b",
                            "age": 31,
                            "favorite_color": "green",
                        }
                    ],
                },
            ]
        )
        config = {
            "iterField": "items",
            "fieldMap": {
                "Name": "name",
                "Age": "age",
                "FavoriteColor": "favorite_color",
            },
            "colMap": {"Id": "id"},
        }

        result = FrameTransformer.extract_nested_rows(data, **config)

        expected = DataFrame(
            [
                {"Name": "a", "Age": 30, "FavoriteColor": "blue", "Id": 1},
                {"Name": "b", "Age": 31, "FavoriteColor": "green", "Id": 2},
            ]
        )

        self.assertEquals(
            json.dumps(result.to_dicts()), json.dumps(expected.to_dicts())
        )
