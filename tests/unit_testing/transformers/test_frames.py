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

    def test_coalesce__with_valid_input__out_field_has_first_non_null_value(
        self,
    ):
        data = DataFrame(
            [
                {"id": 1, "field1": None, "field2": 3, "field3": 4},
            ]
        )
        config = {
            "fields": ("field1", "field2", "field3"),
            "outField": "out",
        }

        result = FrameTransformer.coalesce(data, **config)

        expected = DataFrame(
            [
                {"id": 1, "field1": None, "field2": 3, "field3": 4, "out": 3},
            ]
        )

        self.assertEquals(
            json.dumps(result.to_dicts()), json.dumps(expected.to_dicts())
        )

    def test_coalesce_if__with_valid_input__out_field_is_selected_by_condition(
        self,
    ):
        data = DataFrame(
            [
                {"isActive": True, "primaryValue": "A", "fallbackValue": "B"},
                {"isActive": False, "primaryValue": "C", "fallbackValue": "D"},
            ]
        )
        config = {
            "conditionField": "isActive",
            "conditionValue": True,
            "trueField": "primaryValue",
            "falseField": "fallbackValue",
            "outField": "selectedValue",
        }
        result = FrameTransformer.coalesce_if(data, **config)
        expected = DataFrame(
            [
                {
                    "isActive": True,
                    "primaryValue": "A",
                    "fallbackValue": "B",
                    "selectedValue": "A",
                },
                {
                    "isActive": False,
                    "primaryValue": "C",
                    "fallbackValue": "D",
                    "selectedValue": "D",
                },
            ]
        )
        self.assertEqual(
            json.dumps(result.to_dicts()), json.dumps(expected.to_dicts())
        )
