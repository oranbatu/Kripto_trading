"""Dark, medium-sized Tkinter window for selecting detectors."""
from __future__ import annotations

import ctypes
import tkinter as tk
from tkinter import ttk

from tools.detector_control_center.registry import ALL_CATEGORIES, TASKS, categories
from tools.detector_control_center.selection import SelectionState

WINDOW_WIDTH = 820
WINDOW_HEIGHT = 640
MIN_WIDTH = 720
MIN_HEIGHT = 560
BACKGROUND = "#1e1e1e"
PANEL = "#252526"
TEXT = "#d4d4d4"
MUTED = "#9d9d9d"
ACCENT = "#0e639c"
ACCENT_ACTIVE = "#1177bb"
DISABLED = "#6a6a6a"
FIELD = "#2d2d2d"


def enable_dpi_awareness() -> None:
    if __import__("os").name != "nt":
        return
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except (AttributeError, OSError):
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except (AttributeError, OSError):
            return


class ControlCenterWindow:
    def __init__(self, state: SelectionState | None = None) -> None:
        enable_dpi_awareness()
        self.state = SelectionState() if state is None else state
        self.result: tuple | None = None
        self.closed = False
        self.root = tk.Tk()
        self.root.title("Detector Control Center")
        self.root.configure(background=BACKGROUND)
        self.root.minsize(MIN_WIDTH, MIN_HEIGHT)
        self._center()
        self._style()
        self._build()
        self.refresh()
        self.root.protocol("WM_DELETE_WINDOW", self.cancel)
        self.root.bind("<Escape>", lambda _event: self.cancel())
        self.root.bind("<Return>", self._on_return)

    def _center(self) -> None:
        self.root.update_idletasks()
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        x = max(0, (screen_w - WINDOW_WIDTH) // 2)
        y = max(0, (screen_h - WINDOW_HEIGHT) // 2)
        self.root.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}+{x}+{y}")

    def _style(self) -> None:
        style = ttk.Style(self.root)
        style.theme_use("clam")
        style.configure(".", background=BACKGROUND, foreground=TEXT, font=("Segoe UI", 10))
        style.configure("TFrame", background=BACKGROUND)
        style.configure("TLabel", background=BACKGROUND, foreground=TEXT)
        style.configure("Muted.TLabel", background=BACKGROUND, foreground=MUTED)
        style.configure("Header.TLabel", background=BACKGROUND, foreground=TEXT, font=("Segoe UI", 18, "bold"))
        style.configure("TButton", background=PANEL, foreground=TEXT, padding=(10, 6))
        style.map("TButton", background=[("active", "#333333")])
        style.configure("Run.TButton", background=ACCENT, foreground="#ffffff", font=("Segoe UI", 11, "bold"), padding=(16, 8))
        style.map(
            "Run.TButton",
            background=[("disabled", "#3a3a3a"), ("active", ACCENT_ACTIVE)],
            foreground=[("disabled", DISABLED)],
        )
        style.configure("TCombobox", fieldbackground=FIELD, background=PANEL, foreground=TEXT)
        style.configure("TEntry", fieldbackground=FIELD, foreground=TEXT)

    def _build(self) -> None:
        outer = ttk.Frame(self.root, padding=16)
        outer.pack(fill="both", expand=True)
        ttk.Label(outer, text="Detector Control Center", style="Header.TLabel").pack(anchor="w")
        ttk.Label(
            outer,
            text="Selected production detectors run one after another. Each one publishes its own next workbook revision.",
            style="Muted.TLabel",
            wraplength=760,
        ).pack(anchor="w", pady=(4, 12))

        toolbar = ttk.Frame(outer)
        toolbar.pack(fill="x", pady=(0, 10))
        ttk.Label(toolbar, text="Category").pack(side="left")
        self.category = ttk.Combobox(
            toolbar,
            state="readonly",
            width=28,
            values=(ALL_CATEGORIES, *categories()),
        )
        self.category.set(ALL_CATEGORIES)
        self.category.pack(side="left", padx=(8, 16))
        self.category.bind("<<ComboboxSelected>>", self._on_category)
        ttk.Label(toolbar, text="Search").pack(side="left")
        self.search = tk.StringVar()
        self.search.trace_add("write", lambda *_args: self._on_search())
        entry = tk.Entry(
            toolbar,
            textvariable=self.search,
            bg=FIELD,
            fg=TEXT,
            insertbackground=TEXT,
            relief="flat",
            width=28,
        )
        entry.pack(side="left", padx=(8, 16), ipady=4)
        self.search_entry = entry
        self.count_label = ttk.Label(toolbar, text="0 selected")
        self.count_label.pack(side="right")

        holder = tk.Frame(outer, bg=PANEL, highlightthickness=1, highlightbackground="#3c3c3c")
        holder.pack(fill="both", expand=True)
        self.canvas = tk.Canvas(holder, bg=BACKGROUND, highlightthickness=0)
        scroll = ttk.Scrollbar(holder, orient="vertical", command=self.canvas.yview)
        self.list_frame = tk.Frame(self.canvas, bg=BACKGROUND)
        self.list_frame.bind("<Configure>", lambda _event: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.list_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scroll.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.canvas.bind_all("<MouseWheel>", self._on_wheel)

        footer = ttk.Frame(outer)
        footer.pack(fill="x", pady=(12, 0))
        ttk.Button(footer, text="Select All Visible", command=self.select_visible).pack(side="left")
        ttk.Button(footer, text="Clear Selection", command=self.clear).pack(side="left", padx=(8, 0))
        ttk.Button(footer, text="Cancel", command=self.cancel).pack(side="right", padx=(8, 0))
        self.run_button = ttk.Button(footer, text="RUN", style="Run.TButton", command=self.submit)
        self.run_button.pack(side="right")

    def refresh(self) -> None:
        for child in self.list_frame.winfo_children():
            child.destroy()
        last_category = None
        visible = self.state.visible()
        if not visible:
            tk.Label(self.list_frame, text="No detectors match this filter.", bg=BACKGROUND, fg=MUTED).pack(anchor="w", padx=12, pady=12)
        for task in visible:
            if self.state.category == ALL_CATEGORIES and task.category != last_category:
                tk.Label(
                    self.list_frame,
                    text=task.category.upper(),
                    bg=BACKGROUND,
                    fg=MUTED,
                    font=("Segoe UI", 9, "bold"),
                ).pack(anchor="w", padx=12, pady=(12, 2))
                last_category = task.category
            row = tk.Frame(self.list_frame, bg=BACKGROUND)
            row.pack(fill="x", padx=8, pady=2)
            variable = tk.BooleanVar(value=task.id in self.state.selected and task.enabled)
            check = tk.Checkbutton(
                row,
                variable=variable,
                bg=BACKGROUND,
                fg=TEXT,
                selectcolor=FIELD,
                activebackground=BACKGROUND,
                activeforeground=TEXT,
                disabledforeground=DISABLED,
                command=lambda task_id=task.id, var=variable: self._toggle(task_id, var),
            )
            if not task.enabled:
                check.configure(state="disabled")
            check.pack(side="left", anchor="n")
            text = tk.Frame(row, bg=BACKGROUND)
            text.pack(side="left", fill="x", expand=True, pady=2)
            tk.Label(text, text=task.display_name, bg=BACKGROUND, fg=TEXT, anchor="w", font=("Segoe UI", 10, "bold")).pack(anchor="w")
            meta = f"{task.symbol}   {' / '.join(task.timeframes)}   {task.workbook_family or 'no workbook'}"
            tk.Label(text, text=meta, bg=BACKGROUND, fg=MUTED, anchor="w").pack(anchor="w")
            tk.Label(text, text=task.description, bg=BACKGROUND, fg=TEXT, anchor="w", justify="left", wraplength=680).pack(anchor="w")
            if not task.enabled:
                tk.Label(
                    text,
                    text=task.disabled_reason,
                    bg=BACKGROUND,
                    fg="#d7ba7d",
                    anchor="w",
                    justify="left",
                    wraplength=680,
                ).pack(anchor="w")
        self._update_count()

    def _toggle(self, task_id: str, variable: tk.BooleanVar) -> None:
        self.state.set_selected(task_id, bool(variable.get()))
        self._update_count()

    def _update_count(self) -> None:
        count = self.state.count()
        noun = "detector" if count == 1 else "detectors"
        self.count_label.configure(text=f"{count} selected")
        self.run_button.configure(text="RUN" if count == 0 else f"RUN {count} {noun.upper()}")
        self.run_button.state(["!disabled"] if count else ["disabled"])

    def _on_category(self, _event=None) -> None:
        self.state.category = self.category.get() or ALL_CATEGORIES
        self.refresh()

    def _on_search(self) -> None:
        self.state.query = self.search.get()
        self.refresh()

    def select_visible(self) -> None:
        self.state.select_visible()
        self.refresh()

    def clear(self) -> None:
        self.state.clear()
        self.refresh()

    def submit(self) -> None:
        chosen = self.state.ordered_tasks()
        if not chosen:
            return
        self.result = chosen
        self._close()

    def cancel(self) -> None:
        self.result = None
        self._close()

    def _on_return(self, event) -> None:
        if event.widget is self.search_entry:
            return
        if self.state.count():
            self.submit()

    def _on_wheel(self, event) -> None:
        if self.closed:
            return
        self.canvas.yview_scroll(int(-event.delta / 120), "units")

    def _close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.canvas.unbind_all("<MouseWheel>")
        self.root.destroy()

    def show(self) -> tuple | None:
        self.root.mainloop()
        return self.result


def show_control_center() -> tuple | None:
    return ControlCenterWindow().show()
