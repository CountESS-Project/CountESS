---
layout: default
---

# Contributing to CountESS

CountESS is an open source project and we welcome contributions.

[CountESS source code](https://github.com/CountESS-Project/CountESS/)
is maintained on github.
CountESS is Copyright (C) 2022- CountESS Developers.
It is released under [a BSD 3-clause license](https://github.com/CountESS-Project/CountESS/blob/main/LICENSE.txt).

## Writing your own plugins

You don't need to change anything in CountESS to write your own 
plugins, or to coordinate with or ask permission from the core development
team!

See [Writing CountESS Plugins](../writing-plugins/) for details.

## Raising an issue

If you've found a problem or have an enhancement you're interested in working on,
It's a good idea to check the open issues and raise a new issue if
your concerns aren't already addressed.  This opens discussion with the core
development team and lets other people who might be interested know what you're
working on.

## Installing for development

First make your own fork in github.

Then clone your fork locally and install for development:

    git clone $YOUR_GIT_REPOSITORY countess
    cd countess
    pip install -e .[dev]

Edit files locally and test using:

    script/code-check
    script/run-tests

Please use code formatting tools to tidy your code up as this makes merging
changes easier:

    script/code-clean

Upload your changes and raise a pull request through github so that
the CountESS core developers can test and review it.

## Contributors Agreement

TBA


