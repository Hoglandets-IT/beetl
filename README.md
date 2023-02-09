# pytemplate
Python application/plugin template repository

[toc]: # Table of Contents

# Usage
## requirements.txt
Requirements for compiling/installing the application itself

## requirements-ci.txt
Additional requirements for the CI/CD flows

## setup.py
Configuration file for the application, installation and CLI entry point

## pyproject.toml
Additional build instructions

## .gitpod.yml
Instructions for building development container @gitpod.io

## tpl.github
Rename this dir to .github to activate the Github Actions

### Linting
Scans the code for errors/formatting, outputs a HTML report uploaded to Github

### Matrix_testing
Testing on several OSes with several python versions

### Testing
Regular testing on a single OS/version

### Publish
Publish the package to pypi and the documentation to github pages (/docs)

## src/pytemplate
The source code of the application

### __init__.py
The entry point, can be empty but needs to be present

### __version__.py
The version information for the package/module

### cli.py
The CLI entry point, if usage via the command line is desired

### main.py
The main entry point, example class. No need for using the name main.py

## tests
The tests for the application. With inclusion like src.pytemplate.xyz, the tests need to be run from the project root folder (python3 -m unittest discover tests/)