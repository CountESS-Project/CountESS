import pytest

from countess.core.pipeline import SentinelQueue


def test_sentinelqueue():
    sq = SentinelQueue()

    sq.put("hello")
    sq.put("world")

    sq.finish()

    # can't add more messages once finished
    with pytest.raises(ValueError):
        sq.put("oh, no!")

    sqr = iter(sq)
    assert next(sqr) == "hello"
    assert next(sqr) == "world"

    # when the iterator hits the sentinel it
    # raises StopIteration ...
    with pytest.raises(StopIteration):
        next(sqr)

    # ... and keeps doing so if asked again
    with pytest.raises(StopIteration):
        next(sqr)
