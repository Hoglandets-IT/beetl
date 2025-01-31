import os
from typing import Union

# re-exports
from .strings_schema import StringTransformerSchemas

TransformerSchemas = Union[StringTransformerSchemas]

# Automatic import of all transformers
cDir = os.path.dirname(__file__)
for file in os.listdir(cDir):
    if file.endswith(".py") and not file.startswith("__") and file != "interface.py":
        __import__(f"{__name__}.{file[:-3]}", fromlist=["*"])
