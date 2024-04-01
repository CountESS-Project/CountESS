---
layout: default
---

# Contributing to CountESS

CountESS is an open source project and we welcome contributions.

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

To contribute to the CountESS code, you'll need to "fork" your
own version of CountESS and create a local development enviroment.

### Forking

First, [create your own fork](https://github.com/CountESS-Project/CountESS/fork) at github.

Then you can clone your fork locally:

    git clone $YOUR_GIT_REPOSITORY countess
    cd countess

### Virtual Environments

It's generally best to use [venv](https://docs.python.org/3/library/venv.html)
or [direnv](https://direnv.net/)
to keep your development environment(s) separate from your main
python environment.

#### Using venv

    python -m venv .venv
    source .venv/bin/activate

#### Using direnv

    cd countess
    echo "layout python3" > .envrc
    direnv allow
    
## Testing Locally

Tests are run automatically at github, but you can also run
tests locally.

    pytest --run-doctests countess/ tests/

Some tests will open and close GUI windows which may be annoying
so you can either disable these tests:

    pytest --run-doctests -m "not gui" countess/ tests/

or install [xvfb](https://en.wikipedia.org/wiki/Xvfb) and run
tests with:    

    script/run-tests

or run tests in a docker container with:

    scripts/run-tests-in-docker

## Code Quality
 
We use `pylint` and `mypy` to catch potential code quality issues:

    script/code-check

Before submitting a pull request, please use code formatting
tools to tidy your code up as this makes merging changes easier:

    script/code-clean

## Creating a Pull Request

Push your changes to a branch on your own fork and raise
a pull request through github so that
the CountESS core developers can test and review it.

## Contributors Agreement

TBA

## CountESS Documentation

The [CountESS Documentation](https://countess-project.github.io/CountESS/)
site is automatically generated from
[markdown source](https://github.com/CountESS-Project/CountESS/tree/main/docs)
by Github Pages.

To contribute to the documentation,
[raise a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).

### Building Documentation Locally

Documentation is built automatically by Github Pages when changes are merged,
but you can preview changes locally using 'jekyll'.

#### Installing Jekyll

See:

* [About Github Pages & Jekyll](https://docs.github.com/en/pages/setting-up-a-github-pages-site-with-jekyll/about-github-pages-and-jekyll)
* [Jekyll Install Instructions](https://jekyllrb.com/docs/installation/)

Under Ubuntu, `apt install jekyll` should be enough.

#### Running Jekyll

Jekyll will compile the site and write it into the `_site` subdirectory. 
Please do not add this directory to the repository.  The actual site
is built automatically by Github Pages.

To build the documentation once, run:

        jekyll build

or to run a local HTTP server, run:

        jekyll serve --incremental

### Themes, CSS, etc.

The theming is minimal, using [vanilla.css](https://vanillacss.com/) and
very little else.  Page tables of contents aren't really necessary for 
navigation but for convenience they are generated on page load by
[a very small piece of javascript](https://countess-project.github.io/js/toc.js).

For issues with these pages, especially accessibility issues, please
[raise a github issue](https://github.com/CountESS-Project/CountESS/issues).
