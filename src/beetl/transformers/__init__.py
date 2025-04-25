import os
from typing import Union

from .frames_schema import FramesTransformerSchemas
from .integer_schema import IntegerTransformerSchemas
from .interface import TransformerConfiguration
from .itop_schema import ItopTransformerSchemas
from .misc_schema import MiscTransformerSchemas
from .regex_schema import RegexTransformerSchemas
from .run_transformers import run_transformers
from .strings_schema import StringTransformerSchemas
from .structs_schema import StructTransformerSchemas

TransformerSchemas = Union[
    StringTransformerSchemas,
    StructTransformerSchemas,
    RegexTransformerSchemas,
    MiscTransformerSchemas,
    ItopTransformerSchemas,
    IntegerTransformerSchemas,
    FramesTransformerSchemas,
]

# Automatic import of all transformers
cDir = os.path.dirname(__file__)
for file in os.listdir(cDir):
    if file.endswith(".py") and not file.startswith("__") and file != "interface.py":
        __import__(f"{__name__}.{file[:-3]}", fromlist=["*"])
