#!/usr/bin/env python3
import os
from setuptools import setup

repository_name = "beetl"
module_name = "beetl"

# Get key package details
version = {}  # type: ignore

# Load the __version__.py file from the plugin directory
with open(os.path.join(os.getcwd(), "src", module_name, "__version__.py")) as f:
    exec(f.read(), version)

# Load the README file and use it as the long_description for PyPI
with open("README.md", "r") as f:
    readme = f.read()

# Package configuration - for reference see:
# https://setuptools.readthedocs.io/en/latest/setuptools.html#id9
setup(
    long_description=readme,
    long_description_content_type="text/markdown",
    version=version["__version__"],
)
