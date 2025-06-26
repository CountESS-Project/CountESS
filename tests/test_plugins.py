import importlib.metadata
from unittest.mock import patch

from countess.core.plugins import get_plugin_classes

empty_entry_points = importlib.metadata.EntryPoints()

invalid_entry_points = importlib.metadata.EntryPoints(
    (importlib.metadata.EntryPoint(name="test", value="mockplugin", group="countess_plugins"),)
)


class NoParentPlugin:
    pass


noparent_entry_points = importlib.metadata.EntryPoints(
    (importlib.metadata.EntryPoint(name="test", value="NoParentPlugin", group="countess_plugins"),)
)


def test_get_plugin_classes_invalid(caplog):
    with patch("importlib.metadata.entry_points", lambda: invalid_entry_points):
        get_plugin_classes()
        assert "could not be loaded" in caplog.text


def test_get_plugin_classes_wrongparent(caplog):
    with patch("importlib.metadata.entry_points", lambda: noparent_entry_points):
        with patch("importlib.metadata.EntryPoint.load", lambda x: NoParentPlugin):
            get_plugin_classes()
            assert "not a valid CountESS plugin" in caplog.text
