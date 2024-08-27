from unittest.mock import patch

import pytest
import tkinterweb

from countess.gui.main import make_root
from countess.gui.mini_browser import MiniBrowserFrame

example_url = "http://example.com/"


@pytest.mark.gui
def test_mini_browser():
    with patch.object(tkinterweb.HtmlFrame, "load_url") as p:
        root = make_root()
        MiniBrowserFrame(root, example_url)

        p.assert_called_once()
