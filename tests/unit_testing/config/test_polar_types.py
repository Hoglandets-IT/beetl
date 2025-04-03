from typing import get_args
from unittest import TestCase

from src.beetl.typings import PolarTypeLiterals, get_polar_type_from_literal


class UnitTestPolarTypes(TestCase):
    def test_resolving_all_polar_types__resolves_all_types(self):
        types = get_args(PolarTypeLiterals)
        for type_string in types:
            polar_datatype = get_polar_type_from_literal(type_string)
            self.assertIsNotNone(polar_datatype)
