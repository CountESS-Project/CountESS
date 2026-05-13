from unittest.mock import patch

import pytest

from countess.gui.main import make_root
from countess.gui.mini_browser import MiniBrowserFrame

example_url = "http://example.com/"


@pytest.mark.gui
def test_mini_browser():
    # this package is optional so just skip this test
    # if it isn't installed.

    try:
        import tkinterweb
    except ImportError:
        return

    with patch.object(tkinterweb.HtmlFrame, "load_url") as p:
        root = make_root()
        MiniBrowserFrame(root, example_url)

        p.assert_called_once()
