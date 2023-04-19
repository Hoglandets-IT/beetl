import os
import sys
sys.path.insert(0, os.path.abspath('../..'))
import src.beetl


project = 'BeETL'
copyright = '2023, Lars Scheibling'
author = 'Lars Scheibling'

version = '0.1.0'
release = '2023'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'm2r2',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon'
]

# autodoc_default_flags = ['members']
# autosummary_generate = True

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', '.venv']

language = 'en'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['../_static']
