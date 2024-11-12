import getopt
import logging
import logging.handlers
import multiprocessing
import os
import re
import sys
import threading
import tkinter as tk
from collections.abc import Callable
from tkinter import messagebox, ttk
from typing import Optional, Union

import psutil

from countess import VERSION
from countess.core.config import export_config_graphviz, read_config, write_config
from countess.core.pipeline import PipelineGraph
from countess.core.plugins import get_plugin_classes
from countess.gui.config import PluginConfigurator
from countess.gui.mini_browser import MiniBrowserFrame
from countess.gui.tabular import TabularDataFrame
from countess.gui.tree import FlippyCanvas, GraphWrapper
from countess.gui.widgets import (
    LabeledProgressbar,
    ResizingFrame,
    ask_open_filename,
    ask_saveas_filename,
    get_icon,
    info_button,
)
from countess.utils.pandas import concat_dataframes

usage = """usage: countess_gui [--log LEVEL] [INIFILE]

Start the CountESS GUI.

options:
    --help                         show this message & exit.
    --version                      show version
    --log LEVEL                    set log level to LEVEL
    INIFILE                        load configuration file
"""


# import faulthandler
# faulthandler.enable(all_threads=True)

logger = logging.getLogger(__name__)

logging_queue: multiprocessing.Queue = multiprocessing.Queue()

plugin_classes = sorted(get_plugin_classes(), key=lambda x: x.name)


class PluginChooserFrame(tk.Frame):
    def __init__(self, master, title, callback, has_parents, has_children, *a, **k):
        super().__init__(master, *a, **k)

        self.columnconfigure(0, weight=1)

        label_frame = tk.LabelFrame(self, text=title, padx=10, pady=10)
        label_frame.grid(row=1, column=0, sticky=tk.EW, padx=10, pady=10)

        for n, plugin_class in enumerate(plugin_classes):
            if (
                (has_parents and plugin_class.num_inputs == 0)
                or (not has_parents and plugin_class.num_inputs > 0)
                or (has_children and plugin_class.num_outputs == 0)
            ):
                continue

            label_text = plugin_class.description
            tk.Button(
                label_frame,
                text=plugin_class.name,
                command=lambda plugin_class=plugin_class: callback(plugin_class),
            ).grid(row=n + 1, column=0, sticky=tk.EW)
            tk.Label(label_frame, text=label_text).grid(row=n + 1, column=1, sticky=tk.W, padx=10)


class ConfiguratorWrapper:
    """Wraps up the PluginConfigurator or PluginChooserFrame, plus a
    DataFramePreview, to make up the larger part of the main window"""

    config_canvas = None
    config_subframe = None
    config_subframe_id = None
    preview_subframe = None
    config_change_task = None
    notes_widget = None
    node_update_thread = None
    info_toplevel = None
    info_frame = None

    def __init__(self, tk_parent, node, change_callback):
        self.node = node
        self.change_callback = change_callback

        self.frame = ResizingFrame(tk_parent, orientation=ResizingFrame.Orientation.VERTICAL, bg="darkgrey")
        self.subframe = self.frame.add_frame()
        self.subframe.columnconfigure(0, weight=1)
        self.subframe.rowconfigure(3, weight=1)

        self.name_var = tk.StringVar(self.subframe, value=node.name)
        tk.Entry(self.subframe, textvariable=self.name_var, font=("TkHeadingFont", 14, "bold")).grid(
            row=0, columnspan=2, sticky=tk.EW, padx=10, pady=5
        )
        self.name_var.trace("w", self.name_changed_callback)

        self.label = tk.Label(self.subframe, justify=tk.LEFT, wraplength=500)
        self.label.grid(sticky=tk.EW, row=1, padx=10, pady=5)
        self.label.bind("<Configure>", self.on_label_configure)

        self.show_config_subframe()

        if self.node.plugin:
            if self.node.is_dirty:
                self.config_change_callback()
            else:
                self.show_preview_subframe()

    def show_config_subframe(self):
        if self.config_subframe:
            self.config_subframe.destroy()
        if self.config_canvas:
            self.config_canvas.destroy()
        self.config_canvas = tk.Canvas(self.subframe)
        self.config_scrollbar = ttk.Scrollbar(self.subframe, orient=tk.VERTICAL, command=self.config_canvas.yview)
        self.config_canvas.configure(yscrollcommand=self.config_scrollbar.set, bd=0)
        self.config_canvas.grid(row=3, column=0, sticky=tk.NSEW)
        self.config_scrollbar.grid(row=3, column=1, sticky=tk.NS)

        if self.node.plugin:
            self.node.load_config()
            if self.node.notes:
                self.show_notes_widget(self.node.notes)
            else:
                self.notes_widget = tk.Button(self.subframe, text="add notes", command=self.on_add_notes)
                self.notes_widget.grid(row=2, columnspan=2, padx=10, pady=5)

            descr = re.sub(r"\s+", " ", self.node.plugin.description)
            if self.node.plugin.additional:
                descr += "\n" + re.sub(r"\s+", " ", self.node.plugin.additional)

            self.label["text"] = "%s %s — %s" % (
                self.node.plugin.name,
                self.node.plugin.version,
                descr,
            )
            if self.node.plugin.link:
                info_button(self.frame, command=self.on_info_button_press).place(anchor=tk.NE, relx=1, y=50)
            self.configurator = PluginConfigurator(self.config_canvas, self.node.plugin, self.config_change_callback)
            self.config_subframe = self.configurator.frame

        else:
            has_parents = len(self.node.parent_nodes) > 0
            has_children = len(self.node.child_nodes) > 0
            self.config_subframe = PluginChooserFrame(
                self.config_canvas, "Choose Plugin", self.choose_plugin, has_parents, has_children
            )

        self.config_subframe_id = self.config_canvas.create_window((0, 0), window=self.config_subframe, anchor=tk.NW)
        self.config_subframe.bind(
            "<Configure>",
            lambda e: self.config_canvas.configure(scrollregion=self.config_canvas.bbox("all")),
        )
        self.config_canvas.bind("<Configure>", self.on_config_canvas_configure)

    def on_config_canvas_configure(self, *_):
        self.config_canvas.itemconfigure(self.config_subframe_id, width=self.config_canvas.winfo_width())

    def on_label_configure(self, *_):
        self.label["wraplength"] = self.label.winfo_width() - 20

    def on_info_button_press(self, *_):
        if self.info_toplevel is None:
            self.info_toplevel = tk.Toplevel()
            self.info_toplevel.protocol("WM_DELETE_WINDOW", self.on_info_toplevel_close)
            self.info_frame = MiniBrowserFrame(self.info_toplevel, self.node.plugin.link)
            self.info_frame.pack(fill="both", expand=True)
        else:
            self.info_frame.load_url(self.node.plugin.link)

    def on_info_toplevel_close(self):
        self.info_toplevel.destroy()
        self.info_toplevel = None

    def on_add_notes(self, *_):
        self.notes_widget.destroy()
        self.show_notes_widget()

    def show_notes_widget(self, notes=""):
        self.notes_widget = tk.Text(self.subframe, height=5)
        self.notes_widget.insert("1.0", notes)
        self.notes_widget.bind("<<Modified>>", self.notes_modified_callback)
        self.notes_widget.grid(row=2, columnspan=2, sticky=tk.EW, padx=10, pady=5)

    def show_preview_subframe(self):
        if self.preview_subframe:
            self.frame.remove_child(self.preview_subframe).destroy()

        if not self.node.plugin.show_preview:
            return
        elif not self.node.result:
            self.preview_subframe = tk.Frame(self.frame)
            self.preview_subframe.columnconfigure(0, weight=1)
            tk.Label(self.preview_subframe, text="no result").grid(sticky=tk.EW)
        elif all(isinstance(r, (str, bytes)) for r in self.node.result):
            text_result = "".join(self.node.result)
            self.preview_subframe = tk.Frame(self.frame)
            self.preview_subframe.columnconfigure(0, weight=1)
            self.preview_subframe.rowconfigure(1, weight=1)
            n_lines = len(text_result.splitlines())
            tk.Label(self.preview_subframe, text=f"Text Preview {n_lines} Lines").grid(sticky=tk.NSEW)
            text = tk.Text(self.preview_subframe)
            text.insert("1.0", text_result)
            text["state"] = "disabled"
            text.grid(sticky=tk.NSEW)
        else:
            try:
                df = concat_dataframes(self.node.result)
                self.preview_subframe = TabularDataFrame(self.frame, cursor="arrow")
                self.preview_subframe.set_dataframe(df)
                self.preview_subframe.set_sort_order(self.node.sort_column or 0, self.node.sort_descending)
                self.preview_subframe.set_callback(self.preview_changed_callback)
            except (TypeError, ValueError):
                self.preview_subframe = tk.Frame(self.frame)
                self.preview_subframe.columnconfigure(0, weight=1)
                tk.Label(self.preview_subframe, text="no result").grid(sticky=tk.EW)

        self.frame.add_child(self.preview_subframe)

    def preview_changed_callback(self, offset: int, sort_col: int, sort_desc: bool) -> None:
        self.node.sort_column = sort_col
        self.node.sort_descending = sort_desc

    def name_changed_callback(self, *_):
        name = self.name_var.get()
        self.node.name = name
        self.change_callback(self.node)

    def notes_modified_callback(self, *_):
        self.node.notes = self.notes_widget.get("1.0", tk.END).strip()
        self.notes_widget.edit_modified(False)

    def config_change_callback(self, *_):
        """Called immediately if a change to config has occurred."""
        logger.debug("config_change_callback")
        self.node.mark_dirty()

        # Leave it a bit (2500ms) to see if the user is still typing, if so cancel
        # the previous task and make a new task for another 2.5 seconds away ...
        if self.config_change_task:
            logger.debug("config_change_callback: cancelling task")
            self.frame.after_cancel(self.config_change_task)
        self.config_change_task = self.frame.after(2500, self.config_change_task_callback)

    def config_change_task_callback(self):
        """Called when the user makes a change and had paused for a bit"""
        self.config_change_task = None
        logger.debug("config_change_task_callback")

        # if there's already an update running, request to stop it and then
        # wait for it to stop.
        if self.node_update_thread and self.node_update_thread.is_alive():
            logger.debug("config_change_task_callback: stopping thread %s", self.node_update_thread)
            self.node.prerun_stop()
            self.node_update_thread.join()

        self.node_update_thread = threading.Thread(target=self.node.prerun)
        logger.debug("config_change_task_callback: starting thread %s", self.node_update_thread)
        self.node_update_thread.start()
        self.frame.after(500, self.config_change_task_callback_2)

    def config_change_task_callback_2(self):
        logger.debug("config_change_task_callback_2")
        if not self.node_update_thread:
            return

        if self.node_update_thread.is_alive():
            logger.debug("config_change_task_callback_2: waiting for thread %s", self.node_update_thread)
            self.frame.after(500, self.config_change_task_callback_2)
            return

        logger.debug("config_change_task_callback_2: joining thread %s", self.node_update_thread)
        self.node_update_thread.join()
        self.node_update_thread = None

        # XXX stop the form scrolling away when it is refreshed, by putting
        # it back where it belongs.
        pos1, pos2 = self.config_scrollbar.get()
        self.show_preview_subframe()
        self.configurator.update()
        self.frame.update()
        self.config_canvas.yview_moveto(pos1)
        self.config_scrollbar.set(pos1, pos2)

    def choose_plugin(self, plugin_class):
        self.node.plugin = plugin_class()
        self.node.prerun()
        self.node.is_dirty = True
        self.show_config_subframe()
        self.change_callback(self.node)

    def destroy(self):
        if self.config_change_task:
            self.frame.after_cancel(self.config_change_task)
        self.frame.destroy()


class ButtonMenu:  # pylint: disable=R0903
    def __init__(self, tk_parent, buttons):
        self.frame = tk.Frame(tk_parent)
        for button_number, (button_label, button_command) in enumerate(buttons):
            tk.Button(self.frame, text=button_label, command=button_command).grid(
                row=0, column=button_number, sticky=tk.EW
            )
        self.frame.grid(sticky=tk.NSEW)


class RunWindow:
    """Opens a separate window to run the pipeline in.  The actual pipeline is then run
    in a separate process as well, so that it can be stopped."""

    def __init__(self, graph):
        self.graph = graph

        self.toplevel = tk.Toplevel()
        self.toplevel.columnconfigure(0, weight=1)
        self.toplevel.rowconfigure(1, weight=1)

        tk.Label(self.toplevel, text="Run").grid(row=0, column=0, stick=tk.EW)

        self.logger_frame = LoggerFrame(self.toplevel)
        self.logger_frame.grid(row=1, column=0, stick=tk.NSEW)

        self.button = tk.Button(self.toplevel, text="Stop", command=self.on_button)
        self.button.grid(row=2, column=0, sticky=tk.EW)

        self.process = multiprocessing.Process(target=self.subproc)
        self.process.start()

        self.poll()

    def subproc(self):
        self.graph.run()

    def poll(self):
        if self.process:
            if self.process.is_alive():
                self.logger_frame.after(1000, self.poll)
            else:
                logger.info("Finished")
                self.process = None
                self.button["text"] = "Close"

    def on_button(self):
        if self.process:
            for p in psutil.Process(self.process.pid).children(recursive=True):
                p.terminate()
            self.process.terminate()
            self.button["text"] = "Close"
            self.process = None

            logger.info("Stopped")

        elif self.toplevel:
            self.toplevel.destroy()
            self.toplevel = None


class _CallbackLoggingHandler(logging.Handler):
    """This is a tiny stub of a logging.Handler which just lets you
    direct LogRecords to a callback instead of a handler, eg:
    a method in some other class."""

    def __init__(self, callback: Callable[[logging.LogRecord], None], *a, **k):
        super().__init__(*a, **k)
        self.__callback = callback

    def emit(self, record: logging.LogRecord) -> None:
        self.__callback(record)


class LoggerFrame(tk.Frame):
    def __init__(self, *a, **k) -> None:
        super().__init__(*a, **k)
        self.count = 0
        self.text = tk.Text(self)
        self.text.grid(sticky=tk.NSEW)
        self.progress_bars: dict[str, LabeledProgressbar] = {}

        # Start a QueueListener in its own thread,
        # logging_callback gets called for each record
        # received.
        logging.handlers.QueueListener(logging_queue, _CallbackLoggingHandler(self.logging_callback)).start()

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        # self.rowconfigure(1, weight=0)

        self.pbars_done: list[LabeledProgressbar] = []
        self.hide_event()

    def logging_callback(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if m := re.match(r"(.*): (\d+)(%|/\d+)", message):
            name, n1, n2 = m.groups()
            try:
                pbar = self.progress_bars[name]
            except KeyError:
                pbar = LabeledProgressbar(self, value=0)
                self.progress_bars[name] = pbar
                pbar.grid(sticky=tk.EW, row=len(self.progress_bars), column=0)
            if n2 == "%":
                pbar.progress_update(message, int(n1))
            else:
                pbar.progress_update(message)
        else:
            self.text.insert(tk.END, message + "\n")
            self.count += 1

    def hide_event(self):
        for pbar in self.pbars_done:
            pbar.grid_forget()
        self.pbars_done = []
        for pbar in self.progress_bars.values():
            if pbar.done:
                self.pbars_done.append(pbar)
        self.after(5000, self.hide_event)


class MainWindow:
    """Arrange the parts of the main window, with the FrameWrapper on the top
    left and the ConfiguratorWrapper either beside it or under it depending on
    the window dimenstions"""

    graph_wrapper = None
    preview_frame = None
    config_wrapper = None
    config_changed = False
    config_filename = None

    def __init__(self, tk_parent: tk.Widget, config_filename: Optional[str] = None):
        self.tk_parent = tk_parent

        ButtonMenu(
            tk_parent,
            [
                ("New Config", self.config_new),
                ("Load Config", self.config_load),
                ("Save Config", self.config_save),
                ("Export Config", self.config_export),
                ("Tidy Graph", self.graph_tidy),
                ("Run", self.program_run),
                ("Exit", self.program_exit),
            ],
        )

        # +--------------------------------------------+
        # | ButtonMenu                                 |
        # +--------------+-----------------------------+
        # | tree_canvas  | config_wrapper.subframe     |
        # |              |                             |
        # |              +-----------------------------+
        # |              | configw_wrapper.preview     |
        # |              |                             |
        # +--------------+-----------------------------+
        # | logger_subframe                            |
        # +--------------------------------------------+

        # top level is a vertical layout with logs along the bottom
        self.frame = ResizingFrame(tk_parent, orientation=ResizingFrame.Orientation.VERTICAL, bg="darkgrey")
        self.frame.grid(sticky=tk.NSEW)

        # next is a division between the tree view and the configuration
        self.main_subframe = ResizingFrame(self.frame, orientation=ResizingFrame.Orientation.AUTOMATIC, bg="darkgrey")
        self.frame.add_child(self.main_subframe, weight=4)

        self.logger_subframe = LoggerFrame(self.frame)
        # self.logger = self.logger_subframe.get_logger("")
        # self.logger_subframe_show_task()
        self.frame.add_child(self.logger_subframe)

        self.tree_canvas = FlippyCanvas(self.main_subframe, bg="skyblue")
        self.main_subframe.add_child(self.tree_canvas)

        if config_filename:
            self.config_load(config_filename)
        else:
            self.config_new()

    def logger_subframe_show_task(self):
        if self.logger_subframe.count > 0:
            self.frame.add_child(self.logger_subframe)
            self.frame.after(100, self.logger_subframe_hide_task)
        else:
            self.frame.after(100, self.logger_subframe_show_task)

    def logger_subframe_hide_task(self):
        if self.logger_subframe.count == 0:
            self.frame.remove_child(self.logger_subframe)
            self.frame.after(100, self.logger_subframe_show_task)
        else:
            self.frame.after(100, self.logger_subframe_hide_task)

    def config_new(self):
        if self.config_changed:
            if not messagebox.askokcancel("Clear Config", "Clear unsaved config?"):
                return
        self.config_changed = False
        self.config_filename = None
        self.update_title()
        if self.graph_wrapper:
            self.graph_wrapper.destroy()
        self.graph = PipelineGraph()
        self.graph_wrapper = GraphWrapper(self.tree_canvas, self.graph, self.node_select)
        self.graph_wrapper.add_new_node()

    def config_load(self, filename=None):
        if not filename:
            filename = ask_open_filename(file_types=[(".INI Config File", "*.ini")])
        if not filename:
            return
        self.config_filename = filename
        self.config_changed = False
        self.update_title()

        if self.graph_wrapper:
            self.graph_wrapper.destroy()
        self.graph = read_config([filename])
        self.graph_wrapper = GraphWrapper(self.tree_canvas, self.graph, self.node_select)
        self.node_select(None)

    def config_save(self, filename=None):
        if not filename:
            filename = ask_saveas_filename(
                initial_file=self.config_filename,
                file_types=[(".INI Config File", "*.ini")],
            )
        if not filename:
            return
        write_config(self.graph, filename)
        self.config_filename = filename
        self.config_changed = False
        self.update_title()
        # Names may have changed on save so redraw the tree
        self.graph_wrapper.destroy()
        self.graph_wrapper = GraphWrapper(self.tree_canvas, self.graph, self.node_select)

    def config_export(self, filename=None):
        if not filename:
            if self.config_filename:
                initialfile = self.config_filename.removesuffix(".ini") + ".dot"
            else:
                initialfile = None
            filename = ask_saveas_filename(initial_file=initialfile, file_types=[("Graphviz File", "*.dot")])
        if not filename:
            return
        export_config_graphviz(self.graph, filename)

    def graph_tidy(self):
        self.graph.tidy()
        self.graph_wrapper.destroy()
        self.graph_wrapper = GraphWrapper(self.tree_canvas, self.graph, self.node_select)

    def program_run(self):
        RunWindow(self.graph)

    def program_exit(self):
        if not self.config_changed or messagebox.askokcancel("Exit", "Exit CountESS without saving config?"):
            self.tk_parent.quit()

    def node_select(self, node):
        if node:
            new_config_wrapper = ConfiguratorWrapper(self.main_subframe, node, self.node_changed)
            if self.config_wrapper:
                self.main_subframe.replace_child(self.config_wrapper.frame, new_config_wrapper.frame)
                self.config_wrapper.destroy()
            else:
                self.main_subframe.add_child(new_config_wrapper.frame, weight=4)

            self.config_wrapper = new_config_wrapper

        elif self.config_wrapper:
            self.main_subframe.remove_child(self.config_wrapper.frame).destroy()
            self.config_wrapper = None

    def node_changed(self, node):
        self.graph_wrapper.node_changed(node)
        self.config_changed = True
        self.update_title()

    def update_title(self):
        if self.config_filename:
            title = "CountESS: " + os.path.relpath(self.config_filename)
        else:
            title = "CountESS " + VERSION

        if self.config_changed:
            title += " *"

        self.tk_parent.title(title)


class SplashScreen:
    def __init__(self, tk_root):
        bg = "skyblue"
        self.splash = tk.Toplevel(tk_root, bg=bg)
        self.splash.attributes("-type", "dialog")

        font = ("TkHeadingFont", 16, "bold")
        tk.Label(self.splash, text=f"CountESS {VERSION}", font=font, bg=bg).grid(padx=10, pady=10)
        tk.Label(self.splash, image=get_icon(tk_root, "countess"), bg=bg).grid(padx=10)

        self.splash.after(2500, self.destroy)

    def destroy(self):
        self.splash.destroy()


def make_root():
    try:
        import ttkthemes  # pylint: disable=C0415

        root = ttkthemes.ThemedTk()
        themes = set(root.get_themes())
        for t in ["clam", "aqua", "winnative"]:
            if t in themes:
                root.set_theme(t)
                break
    except ImportError:
        root = tk.Tk()
        # XXX some kind of ttk style setup goes here as a fallback

    root.rowconfigure(0, weight=0)
    root.rowconfigure(1, weight=1)
    root.columnconfigure(0, weight=1)

    # Try to start off maximized, but this option doesn't exist
    # in all Tks or all platforms.
    try:
        root.state("zoomed")
    except tk.TclError:
        pass

    return root


def main() -> None:
    args = sys.argv[1:]

    try:
        options, args = getopt.getopt(args, "", ["help", "version", "log="])
    except getopt.GetoptError as exc:
        logger.error(str(exc))
        sys.exit(1)

    for opt_key, opt_val in options:
        if opt_key == "--help":
            print(usage)  # pylint: disable=bad-builtin
            sys.exit(0)
        elif opt_key == "--version":
            print(f"CountESS {VERSION}")  # pylint: disable=bad-builtin
            sys.exit(0)
        elif opt_key == "--log":
            try:
                log_level: Union[int, str]
                if re.match(r"\d+$", opt_val):
                    log_level = int(opt_val)
                else:
                    log_level = str(opt_val).upper()
                logging.getLogger().setLevel(log_level)
                logger.warning("Log level set to %s", log_level)
            except ValueError:
                logger.error("Bad --log level: %s", opt_val)

    # set up a multiprocessing-compatible logging queue to bring all logging
    # messages back to the main process.
    logging.getLogger().addHandler(logging.handlers.QueueHandler(logging_queue))
    logging.getLogger().addHandler(logging.StreamHandler())

    root = make_root()
    SplashScreen(root)
    MainWindow(root, args[0] if args else None)

    root.mainloop()


if __name__ == "__main__":
    main()  # pragma: no cover
