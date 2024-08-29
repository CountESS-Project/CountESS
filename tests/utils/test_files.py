from countess.utils.files import clean_filename


def test_clean_filename():
    assert clean_filename("baz.qux.quux") == "baz"
    assert clean_filename("foo/bar/baz.qux.quux") == "baz"
    assert clean_filename("foo/bar/baz") == "baz"
    assert clean_filename("fnord") == "fnord"
    assert clean_filename("") == ""
