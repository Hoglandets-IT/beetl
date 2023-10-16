"""Module information."""
import os

# The title and description of the package
__title__ = "beetl"
__description__ = """
    BeETL is a Python package for extracting data from one source, transforming it and loading it into another
"""

# The version and build number
# Without specifying a unique number, you cannot overwrite packages in the PyPi repo
__version__ = os.getenv("github.env.release.name", "0.3.2")

# Author and license information
__author__ = "Lars Scheibling"
__author_email__ = "lars.scheibling@hoglandet.se"
__license__ = "GnuPG 3.0"

# URL to the project
__url__ = f"https://github.com/Hoglandets-IT/{__title__}"
