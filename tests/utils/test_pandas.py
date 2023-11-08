import pytest

import pandas as pd

from countess.utils.pandas import *

df0 = pd.DataFrame([])
df1 = pd.DataFrame([ {'a': n, 'b': n*100 } for n in range(0,100) ]).set_index('a')
df2 = pd.DataFrame([ {'a': n, 'b': n*100 } for n in range(100,1000) ]).set_index('a')
df3 = pd.DataFrame([ {'a': n, 'b': n*100 } for n in range(1000,10000) ]).set_index('a')
df4 = pd.DataFrame([ {'a': n, 'b': n*100 } for n in range(10000,10100) ]).set_index('a')
df5 = pd.DataFrame([ {'a': n, 'b': n*100 } for n in range(10100,10200) ]).set_index('a')
df6 = pd.DataFrame([ {'a': n, 'b': n*100 } for n in range(10200,10300) ]).set_index('a')

def assert_iterable_of_dataframes_equal(a,b):

    assert pd.concat(a).sort_index().equals(
        pd.concat(b).sort_index()
    )

def test_collect_dataframes_0():

    x = list(collect_dataframes([df0,df0,None,df0].__iter__()))
    assert len(x) == 0

def test_collect_dataframes_1():
    x = list(collect_dataframes([df1,df4,df5].__iter__(), 300))
    assert_iterable_of_dataframes_equal([df1,df4,df5], x)
    assert min(len(y) for y in x) > 100

def test_collect_dataframes_2():
    x = list(collect_dataframes([df1,df2,df4,df5].__iter__(), 300))
    assert_iterable_of_dataframes_equal([df1,df2,df4,df5], x)
    assert min(len(y) for y in x) > 100

def test_collect_dataframes_3():
    x = list(collect_dataframes([df1,df2,df4,df5,df6].__iter__(), 300))
    assert_iterable_of_dataframes_equal([df1,df2,df4,df5,df6], x)
    assert len(x) < 5
