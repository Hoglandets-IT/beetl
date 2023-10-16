"""Module information."""
import os

__title__ = "beetl"
__description__ = """
    BeETL is a Python package for extracting data from one datasource, 
    transforming it and loading it into another datasource
"""

__version__ = os.getenv("GHRELEASE", "0.3.2")
if os.getenv('GHRUN', False) and os.getenv('GHBRANCH', 'develop') == 'develop':
    __version__ += f".{os.getenv('RUN_ID')}"

__author__ = "Lars Scheibling"
__author_email__ = "lars.scheibling@hoglandet.se"
__license__ = "GnuPG 3.0"

__url__ = f"https://github.com/Hoglandets-IT/{__title__}"
