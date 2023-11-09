import multiprocessing
import re
import sys
import threading
import tkinter as tk
import webbrowser
from tkinter import filedialog, messagebox, ttk
from typing import Optional

import psutil

from countess import VERSION
from countess.core.config import export_config_graphviz, read_config, write_config
from countess.core.pipeline import PipelineGraph
from countess.core.plugins import get_plugin_classes
from countess.gui.config import PluginConfigurator
from countess.gui.logger import LoggerFrame
from countess.gui.tabular import TabularDataFrame
from countess.gui.tree import FlippyCanvas, GraphWrapper
from countess.utils.pandas import concat_dataframes

# import faulthandler
# faulthandler.enable(all_threads=True)


UNICODE_INFO = "\u2139"

plugin_classes = sorted(get_plugin_classes(), key=lambda x: x.name)


class PluginChooserFrame(tk.Frame):
    def __init__(self, master, title, callback, *a, **k):
        super().__init__(master, *a, **k)

        self.columnconfigure(0, weight=1)

        label_frame = tk.LabelFrame(self, text=title, padx=10, pady=10)
        label_frame.grid(row=1, column=0, sticky=tk.EW, padx=10, pady=10)

        for n, plugin_class in enumerate(plugin_classes):
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

    def __init__(self, frame, node, change_callback):
        self.frame = frame
        self.node = node
        self.change_callback = change_callback

        self.name_var = tk.StringVar(self.frame, value=node.name)
        tk.Entry(self.frame, textvariable=self.name_var, font=("TkHeadingFont", 14, "bold")).grid(
            row=0, columnspan=2, sticky=tk.EW, padx=10, pady=5
        )
        self.name_var.trace("w", self.name_changed_callback)

        self.label = tk.Label(self.frame, justify=tk.LEFT, wraplength=500)
        self.label.grid(sticky=tk.EW, row=1, padx=10, pady=5)
        self.label.bind("<Configure>", self.on_label_configure)

        self.show_config_subframe()

        if self.node.plugin:
            if self.node.is_dirty:
                self.config_change_callback()
            else:
                self.show_preview_subframe()

    def show_config_subframe(self):
        if self.config_canvas:
            self.config_canvas.destroy()
        self.config_canvas = tk.Canvas(self.frame)
        self.config_scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.config_canvas.yview)
        self.config_canvas.configure(yscrollcommand=self.config_scrollbar.set, bd=0)
        self.config_canvas.grid(row=3, column=0, sticky=tk.NSEW)
        self.config_scrollbar.grid(row=3, column=1, sticky=tk.NS)

        self.logger_subframe = LoggerFrame(self.frame)
        self.logger_subframe.grid(row=5, columnspan=2, sticky=tk.NSEW)
        self.logger = self.logger_subframe.get_logger(self.node.name)

        if self.node.plugin:
            if self.node.notes:
                self.show_notes_widget(self.node.notes)
            else:
                self.notes_widget = tk.Button(self.frame, text="add notes", command=self.on_add_notes)
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
                tk.Button(self.frame, text=UNICODE_INFO, fg="blue", command=self.on_info_button_press).place(
                    anchor=tk.NE, relx=1, y=50
                )
            # self.node.prepare(self.logger)
            # self.node.plugin.update()
            self.configurator = PluginConfigurator(self.config_canvas, self.node.plugin, self.config_change_callback)
            self.config_subframe = self.configurator.frame

        else:
            self.config_subframe = PluginChooserFrame(self.config_canvas, "Choose Plugin", self.choose_plugin)

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
        webbrowser.open_new_tab(self.node.plugin.link)

    def on_add_notes(self, *_):
        self.notes_widget.destroy()
        self.show_notes_widget()

    def show_notes_widget(self, notes=""):
        self.notes_widget = tk.Text(self.frame, height=5)
        self.notes_widget.insert("1.0", notes)
        self.notes_widget.bind("<<Modified>>", self.notes_modified_callback)
        self.notes_widget.grid(row=2, columnspan=2, sticky=tk.EW, padx=10, pady=5)

    def show_preview_subframe(self):
        if self.preview_subframe:
            self.preview_subframe.destroy()

        if not self.node.plugin.show_preview:
            self.frame.rowconfigure(4, weight=0)
            return
        elif self.node.result is None:
            self.preview_subframe = tk.Frame(self.frame)
            self.preview_subframe.columnconfigure(0, weight=1)
            tk.Label(self.preview_subframe, text="no result").grid(sticky=tk.EW)
        elif all(isinstance(r, (str, bytes)) for r in self.node.result):
            text_result = "".join(self.node.result)
            self.preview_subframe = tk.Frame(self.frame, highlightbackground="black", highlightthickness=3)
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
                self.preview_subframe = TabularDataFrame(self.frame, highlightbackground="black", highlightthickness=3)
                self.preview_subframe.set_dataframe(df)
            except (TypeError, ValueError):
                self.preview_subframe = tk.Frame(self.frame)
                self.preview_subframe.columnconfigure(0, weight=1)
                tk.Label(self.preview_subframe, text="no result").grid(sticky=tk.EW)

        self.preview_subframe.grid(row=4, columnspan=2, sticky=tk.NSEW)

    def name_changed_callback(self, *_):
        name = self.name_var.get()
        self.node.name = name
        self.change_callback(self.node)

    def notes_modified_callback(self, *_):
        self.node.notes = self.notes_widget.get("1.0", tk.END).strip()
        self.notes_widget.edit_modified(False)

    def config_change_callback(self, *_):
        self.node.mark_dirty()
        if self.config_change_task:
            self.frame.after_cancel(self.config_change_task)
        self.config_change_task = self.frame.after(500, self.config_change_task_callback)

    def config_change_task_callback(self):
        self.config_change_task = None

        self.node_update_thread = threading.Thread(target=self.node.prerun, args=(self.logger,))
        self.node_update_thread.start()

        self.logger_subframe.after(100, self.config_change_task_callback_2)
        self.change_callback(self.node)

    def config_change_task_callback_2(self):
        self.logger.poll()

        if self.node_update_thread.is_alive():
            self.logger_subframe.after(100, self.config_change_task_callback_2)
            return

        self.node_update_thread.join()

        # XXX stop the form scrolling away when it is refreshed, by putting
        # it back where it belongs.
        pos1, pos2 = self.config_scrollbar.get()
        self.show_preview_subframe()
        self.configurator.update()
        self.frame.update()
        self.config_canvas.yview_moveto(pos1)
        self.config_scrollbar.set(pos1, pos2)

        if self.logger_subframe.count == 0:
            self.logger_subframe.grid_forget()

    #        else:
    #            self.logger_subframe.progress_hide()

    def choose_plugin(self, plugin_class):
        self.node.plugin = plugin_class()
        self.node.prerun(self.logger)
        self.node.is_dirty = True
        self.show_config_subframe()
        if self.node.name.startswith("NEW "):
            self.node.name = self.node.plugin.name + self.node.name.removeprefix("NEW")
            self.name_var.set(self.node.name)
        self.change_callback(self.node)

    def destroy(self):
        if self.config_change_task:
            self.frame.after_cancel(self.config_change_task)
        if self.config_subframe:
            self.config_subframe.destroy()
        if self.preview_subframe:
            self.preview_subframe.destroy()
        if self.logger_subframe:
            self.logger_subframe.destroy()


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
        self.logger = self.logger_frame.get_logger("")
        self.logger.info("Started")

        self.button = tk.Button(self.toplevel, text="Stop", command=self.on_button)
        self.button.grid(row=2, column=0, sticky=tk.EW)

        self.process = multiprocessing.Process(target=self.subproc)
        self.process.start()

        self.poll()

    def subproc(self):
        self.graph.run(self.logger)

    def poll(self):
        if self.process:
            if self.process.is_alive():
                self.logger_frame.after(1000, self.poll)
            else:
                self.logger.info("Finished")
                self.process = None
                self.button["text"] = "Close"
            self.logger_frame.poll()

    def on_button(self):
        if self.process:
            for p in psutil.Process(self.process.pid).children(recursive=True):
                p.terminate()
            self.process.terminate()
            self.button["text"] = "Close"
            self.process = None

            self.logger.info("Stopped")
            self.logger_frame.poll()

        else:
            self.toplevel.destroy()
            self.toplevel = None


class MainWindow:
    """Arrange the parts of the main window, with the FrameWrapper on the top
    left and the ConfiguratorWrapper either beside it or under it depending on
    the window dimenstions"""

    graph_wrapper = None
    preview_frame = None
    config_wrapper = None
    config_changed = False

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

        self.frame = tk.Frame(tk_parent)
        self.frame.grid(sticky=tk.NSEW)

        # The left (or top) pane, which contains the pipeline graph
        self.canvas = FlippyCanvas(self.frame, bg="skyblue")

        # The right (or bottom) pane, which contains everything else.
        # 0: The node label
        # 1: The plugin description
        # 2: Node nodes / add notes button
        # 3: Configuration
        # 4: Preview pane
        # 5: Log output

        self.subframe = tk.Frame(self.frame)
        self.subframe.columnconfigure(0, weight=1)
        self.subframe.columnconfigure(1, weight=0)
        self.subframe.rowconfigure(0, weight=0)
        self.subframe.rowconfigure(1, weight=0)
        self.subframe.rowconfigure(2, weight=0)
        self.subframe.rowconfigure(3, weight=1)
        self.subframe.rowconfigure(4, weight=1)
        self.subframe.rowconfigure(5, weight=0)

        self.frame.bind("<Configure>", self.on_frame_configure, add=True)

        self.logger_subframe = LoggerFrame(self.subframe)
        self.logger_subframe.grid(row=5, columnspan=2, sticky=tk.NSEW)

        if config_filename:
            self.config_load(config_filename)
        else:
            self.config_new()

    def config_new(self):
        if self.config_changed:
            if not messagebox.askokcancel("Clear Config", "Clear unsaved config?"):
                return
        self.config_changed = False
        self.config_filename = None
        if self.graph_wrapper:
            self.graph_wrapper.destroy()
        self.graph = PipelineGraph()
        self.graph_wrapper = GraphWrapper(self.canvas, self.graph, self.node_select)
        self.graph_wrapper.add_new_node(select=True)

    def config_load(self, filename=None):
        if not filename:
            filename = filedialog.askopenfilename(filetypes=[(".INI Config File", "*.ini")])
        if not filename:
            return
        self.config_filename = filename
        if self.graph_wrapper:
            self.graph_wrapper.destroy()
        self.graph = read_config(filename)
        self.graph_wrapper = GraphWrapper(self.canvas, self.graph, self.node_select)
        self.node_select(None)

    def config_save(self, filename=None):
        if not filename:
            filename = filedialog.asksaveasfilename(
                initialfile=self.config_filename,
                filetypes=[(".INI Config File", "*.ini")],
            )
        if not filename:
            return
        write_config(self.graph, filename)
        self.config_changed = False
        # XXX there should be a self.graph_wrapper.refresh()
        # Names may have changed on save
        self.graph_wrapper.destroy()
        self.graph_wrapper = GraphWrapper(self.canvas, self.graph, self.node_select)

    def config_export(self, filename=None):
        if not filename:
            if self.config_filename:
                initialfile = self.config_filename.removesuffix(".ini") + ".dot"
            else:
                initialfile = None
            filename = filedialog.asksaveasfilename(initialfile=initialfile, filetypes=[("Graphviz File", "*.dot")])
        if not filename:
            return
        export_config_graphviz(self.graph, filename)

    def graph_tidy(self):
        self.graph.tidy()
        # XXX there should be a self.graph_wrapper.refresh()
        self.graph_wrapper.destroy()
        self.graph_wrapper = GraphWrapper(self.canvas, self.graph, self.node_select)

    def program_run(self):
        RunWindow(self.graph)

    def program_exit(self):
        if not self.config_changed or messagebox.askokcancel("Exit", "Exit CountESS without saving config?"):
            self.tk_parent.quit()

    def node_select(self, node):
        for widget in self.subframe.winfo_children():
            widget.destroy()
        if node:
            if self.config_wrapper:
                self.config_wrapper.destroy()
            self.config_wrapper = ConfiguratorWrapper(self.subframe, node, self.node_changed)

    def node_changed(self, node):
        self.config_changed = True
        self.graph_wrapper.node_changed(node)

    def on_frame_configure(self, event):
        """Swaps layout around when the window goes from landscape to portrait"""
        if event.width > event.height:
            x = event.width // 4
            self.canvas.place(x=0, y=0, w=x, h=event.height)
            self.subframe.place(x=x, y=0, w=event.width - x, h=event.height)
        else:
            y = event.height // 4
            self.canvas.place(x=0, y=0, w=event.width, h=y)
            self.subframe.place(x=0, y=y, w=event.width, h=event.height - y)


def make_root():
    try:
        import ttkthemes  # pylint: disable=C0415

        root = ttkthemes.ThemedTk()
        themes = set(root.get_themes())
        for t in ["winnative", "aqua", "clam"]:
            if t in themes:
                root.set_theme(t)
    except ImportError:
        root = tk.Tk()
        # XXX some kind of ttk style setup goes here as a fallback

    root.title(f"CountESS {VERSION}")
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


def main():
    root = make_root()
    MainWindow(root, sys.argv[1] if len(sys.argv) > 1 else None)

    root.mainloop()


if __name__ == "__main__":
    main()
