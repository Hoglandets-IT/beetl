"""Module information."""

import os

FALLBACK_VERSION = "1.0.0-rc.4"

__title__ = "beetl"
__description__ = """
    BeETL is a Python package for extracting data from one datasource, 
    transforming it and loading it into another datasource
"""

__version__ = os.getenv("GHRELEASE", FALLBACK_VERSION)
if os.getenv("GHRUN", False) and os.getenv("GHBRANCH", "develop") == "develop":
    __version__ += f".{os.getenv('RUN_ID')}"

if __version__ == "":
    __version__ = FALLBACK_VERSION

__author__ = "Lars Scheibling"
__author_email__ = "lars.scheibling@hoglandet.se"
__license__ = "GnuPG 3.0"

__url__ = f"https://github.com/Hoglandets-IT/{__title__}"
