from importlib.metadata import entry_points
import logging
import dask.dataframe as dd
from dask.callbacks import Callback

class ProgressCallback(Callback):

    def __init__(self, progress_callback):
        self.progress_callback = progress_callback

    def _start_state(self, dsk, state):
        self.total_tasks = len(state["ready"]) + len(state["waiting"])

    def _posttask(self, key, result, dsk, state, worker_id):
        self.progress_callback(len(state["finished"]), self.total_tasks)

    def _finish(self, dsk, state, failed):
        # XXX do something with "failed"
        self.progress_callback(self.total_tasks, self.total_tasks)


class BasePlugin:

    def run_with_progress_callback(self, ddf, callback):
        with ProgressCallback(callback):
            return self.run(ddf)

    def run(self, ddf):
        return ddf.compute()


class TransformPlugin(BasePlugin):

    input_class = dd.DataFrame
    output_class = dd.DataFrame


class InputPlugin(BasePlugin):

    input_class = None
    output_class = dd.DataFrame


class OutputPlugin(BasePlugin):

    input_class = dd.DataFrame
    output_class = None


class PluginManager:

    
    def __init__(self):

        self.plugins = []
        for ep in entry_points(group='countess.plugins'):
            plugin = ep.load()
            if issubclass(plugin, BasePlugin):
                self.plugins.append(plugin)
            else:
                logging.warning(f"{plugin} is not a valid CountESS plugin")


    def get_input_plugins(self):
        return [ pp for pp in self.plugins if pp.input_class is None ]

    def get_transform_plugins(self):
        return [ pp for pp in self.plugins if pp.input_class is not None and pp.output_class is not None ]

    def get_output_plugins(self):
        return [ pp for pp in self.plugins if pp.output_class is None ]


    def get_plugin_chain(self, prev_plugin=None, next_plugin=None):

        for pp in self.plugins:
            if (
                (pp.input_class is None if prev_plugin is None else issubclass(pp.input_class, prev_plugin.output_class)) and
                (pp.output_class is None if next_plugin is None else issubclass(next_plugin.input_class, pp.output_class))
                ): yield pp
