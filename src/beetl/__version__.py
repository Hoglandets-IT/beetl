"""Module version information."""

import os
import re

REGULAR_RELEASE = "(\d.\d.\d)"
PRE_RELEASE = "(\d.\d.\d-rc.\d)"

gh_tag = os.getenv("GHTAG")
gh_branch = os.getenv("GHBRANCH")
gh_run = os.getenv("GHRUN")

__version__ = "0.0.1"

if gh_tag is not None:
    matches = re.findall(PRE_RELEASE, gh_tag)
    if matches and len(matches) == 1:
        __version__ = matches[0]
    else:
        matches = re.findall(REGULAR_RELEASE, gh_tag)
        if matches and len(matches) == 1:
            __version__ = matches[0]
