#!/usr/bin/env python3
import os
from setuptools import setup, find_packages

repository_name = "beetl"
module_name = "beetl"

with open("requirements.txt", "r") as f:
    required_packages = f.read().splitlines()

# Get key package details
version = {}  # type: ignore
here = os.path.abspath(os.path.dirname(__file__))

# Load the __version__.py file from the plugin directory
with open(os.path.join(here, "src", module_name, "__version__.py")) as f:
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
    packages=find_packages(where="src"),
    include_package_data=True,
    install_requires=required_packages,
    zip_safe=True,
    package_dir={"": "src"},
)
