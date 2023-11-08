import pytest

import pandas as pd
import numpy as np

from countess.plugins.join import JoinPlugin
from countess.core.logger import MultiprocessLogger

df1 = pd.DataFrame([
    { 'foo': 1, 'bar': 2, 'baz': 3 },
    { 'foo': 4, 'bar': 5, 'baz': 6 },
    { 'foo': 7, 'bar': 8, 'baz': 9 },
])

df2 = pd.DataFrame([
    { 'foo': 1, 'qux': 'a' },
    { 'foo': 4, 'qux': 'b' },
    { 'foo': 8, 'qux': 'c' },
])

logger = MultiprocessLogger()

def test_join_inner():

    plugin = JoinPlugin()

    plugin.set_parameter('inputs.0.join_on', 'foo')
    plugin.set_parameter('inputs.0.required', True)
    plugin.set_parameter('inputs.1.join_on', 'foo')
    plugin.set_parameter('inputs.1.required', True)

    plugin.prepare(['df1', 'df2'])

    df3 = plugin.process_dataframes(df1, df2, logger)

    df4 = pd.DataFrame([
        { 'foo': 1, 'bar': 2, 'baz': 3, 'qux': 'a' },
        { 'foo': 4, 'bar': 5, 'baz': 6, 'qux': 'b' },
    ])

    assert df3.equals(df4)


def test_join_left():

    plugin = JoinPlugin()

    plugin.set_parameter('inputs.0.join_on', 'foo')
    plugin.set_parameter('inputs.0.required', True)
    plugin.set_parameter('inputs.1.join_on', 'foo')
    plugin.set_parameter('inputs.1.required', False)

    plugin.prepare(['df1', 'df2'])

    df3 = plugin.process_dataframes(df1, df2, logger)

    df4 = pd.DataFrame([
        { 'foo': 1, 'bar': 2, 'baz': 3, 'qux': 'a' },
        { 'foo': 4, 'bar': 5, 'baz': 6, 'qux': 'b' },
        { 'foo': 7, 'bar': 8, 'baz': 9, 'qux': None },
    ])

    assert df3.equals(df4)

def test_join_right():

    plugin = JoinPlugin()

    plugin.set_parameter('inputs.0.join_on', 'foo')
    plugin.set_parameter('inputs.0.required', False)
    plugin.set_parameter('inputs.1.join_on', 'foo')
    plugin.set_parameter('inputs.1.required', True)

    plugin.prepare(['df1', 'df2'])

    df3 = plugin.process_dataframes(df1, df2, logger)

    df4 = pd.DataFrame([
        { 'foo': 1, 'bar': 2, 'baz': 3, 'qux': 'a' },
        { 'foo': 4, 'bar': 5, 'baz': 6, 'qux': 'b' },
        { 'foo': 8, 'bar': None, 'baz': None, 'qux': 'c' },
    ])

    assert df3.equals(df4)


def test_join_full():

    plugin = JoinPlugin()

    plugin.set_parameter('inputs.0.join_on', 'foo')
    plugin.set_parameter('inputs.0.required', False)
    plugin.set_parameter('inputs.1.join_on', 'foo')
    plugin.set_parameter('inputs.1.required', False)

    plugin.prepare(['df1', 'df2'])

    df3 = plugin.process_dataframes(df1, df2, logger)

    df4 = pd.DataFrame([
        { 'foo': 1, 'bar': 2, 'baz': 3, 'qux': 'a' },
        { 'foo': 4, 'bar': 5, 'baz': 6, 'qux': 'b' },
        { 'foo': 7, 'bar': 8, 'baz': 9, 'qux': None },
        { 'foo': 8, 'bar': None, 'baz': None, 'qux': 'c' },
    ])

    assert df3.equals(df4)



def test_join_drop1():

    plugin = JoinPlugin()

    plugin.set_parameter('inputs.0.join_on', 'foo')
    plugin.set_parameter('inputs.0.drop', True)
    plugin.set_parameter('inputs.1.join_on', 'foo')

    plugin.prepare(['df1', 'df2'])

    df3 = plugin.process_dataframes(df1, df2, logger)

    df4 = pd.DataFrame([
        { 'bar': 2, 'baz': 3, 'qux': 'a' },
        { 'bar': 5, 'baz': 6, 'qux': 'b' },
    ])

    assert df3.equals(df4)


def test_join_drop2():

    plugin = JoinPlugin()

    plugin.set_parameter('inputs.0.join_on', 'foo')
    plugin.set_parameter('inputs.1.join_on', 'foo')
    plugin.set_parameter('inputs.1.drop', True)

    plugin.prepare(['df1', 'df2'])

    df3 = plugin.process_dataframes(df1, df2, logger)

    df4 = pd.DataFrame([
        { 'bar': 2, 'baz': 3, 'qux': 'a' },
        { 'bar': 5, 'baz': 6, 'qux': 'b' },
    ])

    assert df3.equals(df4)

def test_join_drop3():

    plugin = JoinPlugin()

    plugin.set_parameter('inputs.0.join_on', 'foo')
    plugin.set_parameter('inputs.0.drop', True)
    plugin.set_parameter('inputs.1.join_on', 'foo')
    plugin.set_parameter('inputs.1.drop', True)

    plugin.prepare(['df1', 'df2'])

    df3 = plugin.process_dataframes(df1, df2, logger)

    df4 = pd.DataFrame([
        { 'bar': 2, 'baz': 3, 'qux': 'a' },
        { 'bar': 5, 'baz': 6, 'qux': 'b' },
    ])

    assert df3.equals(df4)



def test_join_broken():

    plugin = JoinPlugin()

    plugin.set_parameter('inputs.0.join_on', 'foo')
    plugin.set_parameter('inputs.0.drop', True)
    plugin.set_parameter('inputs.1.join_on', 'qux')
    plugin.set_parameter('inputs.1.drop', True)

    plugin.prepare(['df1', 'df2'])

    df3 = plugin.process_dataframes(df1, df2, logger)

    assert len(df3) == 0



































































































































































































































































