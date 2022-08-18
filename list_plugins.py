from countess.core.plugins import PluginManager
import inspect

pm = PluginManager()
for pc in pm.plugins:
    n = pc.__name__
    m = inspect.getmodule(pc).__name__
    f = inspect.getfile(pc)
    print(f"{m}:{n} {f}")
