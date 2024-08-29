import pytest

from countess.core.parameters import BooleanParam, FloatParam, MultiParam, StringParam, make_prefix_groups


def test_make_prefix_groups():
    x = make_prefix_groups(["one_two", "one_three", "two_one", "two_two", "two_three", "three_four_five"])

    assert x == {"one_": ["two", "three"], "two_": ["one", "two", "three"]}


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

    mp["foo"] = "hello"
    assert mp.foo == "hello"

    for key in mp:
        assert isinstance(mp[key], StringParam)

    for key, param in mp.items():
        assert isinstance(param, StringParam)
