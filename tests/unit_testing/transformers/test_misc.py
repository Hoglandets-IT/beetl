import json
from unittest import TestCase

from polars import DataFrame

from src.beetl.transformers.misc import MiscTransformer

class UnitTestMiscTransformers(TestCase):
    def test_sam_from_dn__with_valid_input__sam_is_extracted(self):
        data = DataFrame(
            [
                {
                    "dn": "CN=test.user,OU=Users,DC=example,DC=local",
                }
            ]
        )

        result = MiscTransformer.sam_from_dn(
            data,
            inField="dn",
            outField="sam",
        )

        expected = DataFrame(
            [
                {
                    "dn": "CN=test.user,OU=Users,DC=example,DC=local",
                    "sam": "test.user",
                }
            ]
        )

        self.assertEqual(
            json.dumps(result.to_dicts()), json.dumps(expected.to_dicts())
        )


    def test_lookup__with_valid_input__out_field_has_mapped_value(self):
        data = DataFrame(
            [
                {"municipality": "stockholm"},
                {"municipality": "TRANÅS"},
                {"municipality": "GöTEBORG"},
                {"municipality": "MALMÖ"},
            ]
        )
        config = {
            "inField": "municipality",
            "outField": "municipalityCode",
            "caseInsensitive": True,
            "mapping": {
                "TRANÅS": "0564",
                "STOCKHOLM": "0180",
                "GöTEBORG": "1480",
                "MALMÖ": "1280",
            },
        }

        result = MiscTransformer.lookup(data, **config)

        expected = DataFrame(
            [
                {"municipality": "stockholm", "municipalityCode": "0180"},
                {"municipality": "TRANÅS", "municipalityCode": "0564"},
                {"municipality": "GöTEBORG", "municipalityCode": "1480"},
                {"municipality": "MALMÖ", "municipalityCode": "1280"},
            ]
        )

        self.assertEqual(
            json.dumps(result.to_dicts()), json.dumps(expected.to_dicts())
        )


    def test_guid_from_fields__with_valid_input__guid_is_created(self):
        data = DataFrame(
            [
                {"field1": "abc", "field2": "123"},
                {"field1": "abc", "field2": "123"},
            ]
        )

        config = {
            "inFields": ["field1", "field2"],
            "outField": "externalId",
        }

        result = MiscTransformer.guid_from_fields(data, **config)

        values = result["externalId"].to_list()

        self.assertEqual(values[0], values[1])
        self.assertEqual(len(values[0]), 36)