import time

import duckdb
import pytest

from countess.gui.main import make_root
from countess.gui.tabular import TabularDataFrame

ddbc = duckdb.connect()

ddbc.sql("create table n_0 as (select unnest(range(0,1000000)) as x, hash(x) as y, hash(y) as z )")
source = ddbc.table("n_0")


@pytest.mark.gui
def test_tabular_1():
    root = make_root()
    tt = TabularDataFrame(root)

    tt.set_table(ddbc, source)


@pytest.mark.gui
def test_tabular_scroll():
    root = make_root()
    tt = TabularDataFrame(root)
    tt.set_table(ddbc, source)

    for offset in (10, 0, 6666, 6660, 3333):
        time.sleep(0.1)
        tt.refresh(offset)
        root.update()


@pytest.mark.gui
def test_tabular_copy():
    root = make_root()
    tt = TabularDataFrame(root)
    tt.set_table(ddbc, source)

    tt.select_rows = (30, 32)
    tt._column_copy(None)

    x = root.selection_get(selection="CLIPBOARD")
    assert (
        x == """x\ty\tz
29\t687214525942812148\t13438185942843147596
30\t6255274016445075936\t1382406602978853548
31\t13285076694688170502\t14198994021025867918\n\n"""
    )
