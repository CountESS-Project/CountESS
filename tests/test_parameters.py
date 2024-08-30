import io
from unittest.mock import patch

import pandas as pd
import pytest

from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ChoiceParam,
    ColumnChoiceParam,
    ColumnGroupOrNoneChoiceParam,
    ColumnOrIndexChoiceParam,
    ColumnOrIntegerParam,
    ColumnOrNoneChoiceParam,
    DataTypeChoiceParam,
    DataTypeOrNoneChoiceParam,
    FileArrayParam,
    FileParam,
    FloatParam,
    IntegerParam,
    MultiParam,
    PerColumnArrayParam,
    ScalarParam,
    StringCharacterSetParam,
    StringParam,
    make_prefix_groups,
)


def test_make_prefix_groups():
    x = make_prefix_groups(["one_two", "one_three", "two_one", "two_two", "two_three", "three_four_five"])

    assert x == {"one_": ["two", "three"], "two_": ["one", "two", "three"]}


def test_scalarparm():
    sp1 = ScalarParam("x")
    sp1.value = "hello"
    sp2 = sp1.copy_and_set_value("goodbye")
    assert sp1.value == "hello"
    assert sp2.value == "goodbye"


def test_stringparam():
    sp = StringParam("i'm a frayed knot")

    sp.set_value("hello")

    assert sp == "hello"
    assert sp != "goodbye"
    assert sp > "hell"
    assert sp >= "hell"
    assert sp >= "hello"
    assert sp < "help"
    assert sp <= "hello"
    assert sp <= "help"
    assert sp + "world" == "helloworld"
    assert "why" + sp == "whyhello"
    assert "ell" in sp
    assert hash(sp) == hash("hello")


def test_floatparam():
    fp = FloatParam("whatever")

    for v in (0, 1, 106.7, -45):
        fp.set_value(v)

        assert fp == v
        assert fp != v + 1
        assert fp > v - 1
        assert fp < v + 1
        assert fp + 1 == v + 1
        assert 1 + fp == 1 + v
        assert fp * 2 == v * 2
        assert 3 * fp == 3 * v
        assert -fp == -v
        assert fp - 1 == v - 1
        assert 2 - fp == 2 - v
        assert float(fp) == v
        assert abs(fp) == abs(v)
        assert +fp == +v


def test_booleanparam():
    bp = BooleanParam("dude")

    with pytest.raises(ValueError):
        bp.set_value("Yeah, Nah")

    bp.set_value("T")

    assert bool(bp)
    assert str(bp) == "True"

    bp.set_value("F")

    assert not bool(bp)
    assert str(bp) == "False"


def test_multiparam():
    mp = MultiParam(
        "x",
        {
            "foo": StringParam("Foo"),
            "bar": StringParam("Bar"),
        },
    )

    assert "foo" in mp
    assert sorted(mp.keys()) == ["bar", "foo"]

    mp["foo"] = "hello"
    assert mp.foo == "hello"
    assert mp["foo"] == "hello"
    assert "bar" in mp

    for key in mp:
        assert isinstance(mp[key], StringParam)

    for key, param in mp.items():
        assert isinstance(param, StringParam)

    mp.set_parameter("foo._label", "fnord")
    assert mp["foo"].label == "fnord"

    with pytest.raises(AttributeError):
        assert mp.fnord


def test_scsp():
    pp = StringCharacterSetParam("x", "hello", character_set=set("HelO"))
    pp.value = "helicopter"
    assert pp.value == "HelOe"


def test_choiceparam():
    cp = ChoiceParam("x", value="a", choices=["a", "b", "c", "d"])

    cp.value = None
    assert cp.value == ""

    cp.choice = 2
    assert cp.choice == 2
    assert cp.value == "c"

    cp.choice = 5
    assert cp.choice is None
    assert cp.value == ""

    cp.value = "b"
    cp.set_choices(["a", "b", "c"])
    assert cp.choice == 1
    assert cp.value == "b"

    cp.set_choices(["x", "y"])
    assert cp.choice == 0
    assert cp.value == "x"

    cp.set_choices([])
    assert cp.choice is None
    assert cp.value == ""


def test_dtcp1():
    cp = DataTypeChoiceParam("x")
    assert cp.get_selected_type() is None


def test_dtcp2():
    cp = DataTypeOrNoneChoiceParam("x")

    assert cp.get_selected_type() is None
    assert cp.cast_value("whatever") is None
    assert cp.is_none()

    cp.value = "integer"
    assert cp.get_selected_type() == int
    assert cp.cast_value(7.3) == 7
    assert cp.cast_value("whatever") == 0
    assert not cp.is_none()


def test_ccp1():
    cp = ColumnChoiceParam("x", "a")
    df = pd.DataFrame([])
    with pytest.raises(ValueError):
        cp.get_column(df)


def test_ccp2():
    df = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"])
    cp = ColumnOrNoneChoiceParam("x")
    cp.set_choices(["a", "b"])
    assert cp.is_none()
    assert cp.get_column(df) is None

    cp.value = "a"
    assert cp.is_not_none()
    assert isinstance(cp.get_column(df), pd.Series)

    df = df.set_index("a")
    assert isinstance(cp.get_column(df), pd.Series)

    df = df.reset_index().set_index(["a", "b"])
    assert isinstance(cp.get_column(df), pd.Series)

    df = pd.DataFrame([], columns=["x", "y"])
    with pytest.raises(ValueError):
        cp.get_column(df)


def test_coindex():
    cp = ColumnOrIndexChoiceParam("x", choices=["a", "b"])
    df = pd.DataFrame(columns=["a", "b"]).set_index("a")
    assert cp.is_index()
    assert isinstance(cp.get_column(df), pd.Series)

    cp.choice = 1
    assert cp.is_not_index()
    assert isinstance(cp.get_column(df), pd.Series)


def test_columnorintegerparam():
    df = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"])
    cp = ColumnOrIntegerParam("x")
    cp.set_column_choices(["a", "b"])

    assert cp.get_column_name() is None

    cp.value = "7"
    assert cp.choice is None
    assert cp.get_column_name() is None
    assert cp.get_column_or_value(df, False) == "7"
    assert cp.get_column_or_value(df, True) == 7

    cp.choice = 0
    assert cp.get_column_name() == "a"
    assert isinstance(cp.get_column_or_value(df, False), pd.Series)

    cp.set_column_choices(["c", "d"])
    assert cp.choice is None

    cp.value = "hello"
    assert cp.value == 0


def test_columngroup():
    df = pd.DataFrame([], columns=["one_two", "one_three", "two_one", "two_two", "two_three", "three_four_five"])
    cp = ColumnGroupOrNoneChoiceParam("x")
    cp.set_column_choices(df.columns)
    assert cp.is_none()
    assert "one_*" in cp.choices
    assert "two_*" in cp.choices
    assert cp.get_column_prefix() is None

    cp.choice = 2
    assert cp.is_not_none()
    assert cp.get_column_prefix() == "two_"
    assert cp.get_column_suffixes(df) == ["one", "two", "three"]
    assert cp.get_column_names(df) == ["two_one", "two_two", "two_three"]


def test_fileparam():
    fp = FileParam("x")
    assert fp.get_file_hash() == "0"

    fp.value = "filename"
    buf = io.BytesIO(b"hello")

    with patch("builtins.open", lambda *_, **__: buf):
        h = fp.get_file_hash()
        assert h == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def dummy(*_, **__):
        raise IOError("x")

    with patch("builtins.open", dummy):
        assert fp.get_file_hash() == "0"

    fp.value = "/foo/bar/baz"
    assert fp.get_parameters("fnord", "/foo") == [("fnord", "bar/baz")]

    assert fp.get_parameters("fnord", "") == [("fnord", "/foo/bar/baz")]

    fp.value = ""
    assert fp.get_parameters("fnord", "") == [("fnord", None)]


def test_filearrayparam():
    ap1 = FileArrayParam("x", param=FileParam("Y"))
    assert ap1.find_fileparam().label == "Y"

    ap2 = FileArrayParam("x", param=MultiParam("Y", params={"z": FileParam("Z")}))
    assert ap2.find_fileparam().label == "Z"

    with pytest.raises(TypeError):
        ap0 = FileArrayParam("x", param=IntegerParam("Y"))
        ap0.find_fileparam()


def test_arrayparam_minmax():
    pp = IntegerParam("x")
    ap = ArrayParam("y", param=pp, min_size=2, max_size=3)
    assert len(ap) == 2

    assert isinstance(ap.add_row(), IntegerParam)
    assert len(ap) == 3

    assert ap.add_row() is None
    assert len(ap) == 3

    ap.del_row(0)
    assert len(ap) == 2

    ap.del_row(1)
    assert len(ap) == 2

    assert ap[1] in ap
    ap.del_subparam(ap[1])
    assert len(ap) == 2

    ap[0] = 7
    assert ap[0].value == 7


def test_pcap():
    pp = IntegerParam("x")
    ap = PerColumnArrayParam("y", param=pp)

    ap.set_column_choices(["a", "b", "c"])
    assert len(ap) == 3
    apa, apb, apc = list(ap)

    ap.set_column_choices(["c", "d", "b", "a"])
    assert len(ap) == 4
    assert ap[0] is apc
    assert ap[2] is apb
    assert ap[3] is apa
    assert ap[1] is not apb
