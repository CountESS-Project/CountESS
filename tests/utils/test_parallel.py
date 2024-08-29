import pytest

from countess.utils.parallel import multiprocess_map, IterableMultiprocessQueue


def test_multiprocess_map():
    def function(value):
        for n in range(0, value):
            yield (n + 1) * value

    values = [1, 2, 3, 4, 5, 6, 7]

    output = multiprocess_map(function, values)

    assert sorted(output) == [
        1,
        2,
        3,
        4,
        4,
        5,
        6,
        6,
        7,
        8,
        9,
        10,
        12,
        12,
        14,
        15,
        16,
        18,
        20,
        21,
        24,
        25,
        28,
        30,
        35,
        36,
        42,
        49,
    ]

def test_multiprocess_map_stopped():

    impq = IterableMultiprocessQueue()

    impq.put('1')
    impq.put('2')
    impq.put('3')
    impq.finish()

    with pytest.raises(ValueError):
        impq.put('4')

    assert sorted(list(impq)) == ['1', '2', '3']
