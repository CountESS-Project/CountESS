---
layout: default
---

# CountESS Documentation

This site is generated
[from markdown source](https://github.com/CountESS-Project/CountESS/tree/main/docs)
by Github Pages.

To contribute to documentation, [raise a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).

## Building Documentation Locally

Documentation is built automatically by Github Pages, but you can build it locally 
as well using 'jekyll' to check it is all working properly.

### Installing Jekyll

See:

* [About Github Pages & Jekyll](https://docs.github.com/en/pages/setting-up-a-github-pages-site-with-jekyll/about-github-pages-and-jekyll)
* [Jekyll Install Instructions](https://jekyllrb.com/docs/installation/)

Under Ubuntu, `apt install jekyll` should be enough.

### Running Jekyll

Jekyll will compile the site and write it into the `_site` subdirectory. 
Please do not add this directory to the repository.

To build the documentation once, run:

        jekyll build

or to run a local HTTP server run:

        jekyll serve --incremental

## Themes, CSS, etc.

The theming is minimal, using [vanilla.css](https://vanillacss.com/) and
very little else.  Page tables of contents aren't really necessary for 
navigation but for convenience they are generated on page load by
[a very small piece of javascript](https://github.com/CountESS-Project/CountESS/tree/main/docs/js/toc.js).
