from typing import Protocol, Optional

from matplotlib import cm
from matplotlib.axes import Axes
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
from matplotlib import image as mpimg
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from pandas import DataFrame
import seaborn as sns

from utils.constants import exp_name_mapping
from utils.exp_utils import get_rep_with, OpType


class PlotFunction(Protocol):
    def __call__(self, data: DataFrame, x: str, ax: Axes, is_higher_better: bool, hue: str, log_scale: bool) -> Axes:
        pass


def __basic_plot(plot_fn: PlotFunction, data: DataFrame, x: str, x_name: str, y_name: str, title: str,
                 is_higher_better: bool = True, hue: str = "", log_scale: bool = False):
    data_len = len(data)
    fig, ax = plt.subplots(figsize=(data_len * 2, 10)
                           if data_len > 0 else None)

    plot_fn(data, x, ax, is_higher_better, hue, log_scale)

    ax.set_xlabel(x_name, fontsize=18)
    ax.set_ylabel(y_name, fontsize=18)
    ax.set_title(title, fontsize=20)
    ax.set_ylim(ymin=0)

    return fig, ax


def __errorbar_plot(data: DataFrame, x: str, ax: Axes, _is_higher_better: bool, hue: str, _log_scale: bool) -> Axes:
    return sns.pointplot(data=data, x=x, y="value", hue=hue, errorbar=lambda v: (v.min(), v.max()), capsize=.2, ax=ax)


def errorbar_plot(data: DataFrame, title: str, y_name: str, x: str = "exp_name", x_name: str = "Experiment Name"):
    data = data.rename(columns={"exp_name": "Experiment Name"})
    if x == "exp_name":
        x = "Experiment Name"

    fig, ax = __basic_plot(__errorbar_plot, data, x,
                           x_name, y_name, title, hue=x)
    return fig


def __hist_plot(data: DataFrame, x: str, ax: Axes, _is_higher_better: bool, hue: str, _log_scale: bool) -> Axes:
    return sns.histplot(data=data, x=x, hue=hue, ax=ax, stat="probability")


def hist_plot(data: DataFrame, title: str, x_name: str, y_name: str = "Probability"):
    data = data.rename(columns={"exp_name": "Experiment Name"})
    x = "value"
    fig, ax = __basic_plot(__hist_plot, data, x, x_name,
                           y_name, title, hue="Experiment Name")

    ax.get_xaxis().set_major_formatter(
        plt.FuncFormatter(lambda val, _: f"{int(val):,}")
    )

    ax.set_xlim(xmin=0)

    return fig, ax


def __ecdf_plot(data: DataFrame, x: str, ax: Axes, _is_higher_better: bool, hue: str, log_scale: bool) -> Axes:
    return sns.ecdfplot(
        data=data,
        x=x,
        hue=hue,
        ax=ax,
        linewidth=2,
        log_scale=log_scale,
    )


def ecdf_plot(data: DataFrame, title: str, x_name: str, y_name: str = "Cumulative Probability", log_scale: bool = False, is_int_axis: bool = True):
    data = data.rename(columns={"exp_name": "Experiment Name"})
    x = "value"
    fig, ax = __basic_plot(__ecdf_plot, data, x, x_name, y_name,
                           title, hue="Experiment Name", log_scale=log_scale)

    if is_int_axis:
        ax.get_xaxis().set_major_formatter(
            plt.FuncFormatter(lambda val, _: f"{int(val):,}")
        )

    ax.set_xlim(xmin=0)

    return fig


def __kde_plot(data: DataFrame, x: str, ax: Axes, _is_higher_better: bool, hue: str, log_scale: bool) -> Axes:
    return sns.kdeplot(
        data=data,
        x=x,
        hue=hue,
        ax=ax,
        fill=True,
        linewidth=2,
        bw_adjust=.5,
        cut=0,  # TODO: Check if should be used
        log_scale=log_scale,
    )


def kde_plot(data: DataFrame, title: str, x_name: str, y_name: str = "Density", log_scale=False):
    data = data.rename(columns={"exp_name": "Experiment Name"})
    x = "value"
    fig, ax = __basic_plot(__kde_plot, data, x, x_name, y_name,
                           title, hue="Experiment Name", log_scale=log_scale)

    ax.get_xaxis().set_major_formatter(
        plt.FuncFormatter(lambda val, _: f"{int(val):,}")
    )

    ax.set_xlim(xmin=0)

    return fig


def __box_plot(data: DataFrame, x: str, ax: Axes, _is_higher_better: bool, _hue: str) -> Axes:
    return sns.boxplot(data=data, x=x, y="value", ax=ax)


def boxplot(data: DataFrame, y_name: str, title: str, x: str = "exp_name", x_name: str = "Experiment Name"):
    fig, ax = __basic_plot(__box_plot, data, x, x_name, y_name, title)
    return fig


def __stacked_bar_plot(data: DataFrame, x, ax: Axes, is_higher_better: bool, _hue: str, _log_scale: bool):
    stats = (
        data
        .groupby(x)["value"]
        .agg(min_val="min", avg="mean", max_val="max")
        .reset_index()
    )

    max_color_idx = 0 if is_higher_better else 1
    min_color_idx = 1 if is_higher_better else 0
    min_alpha = 1 if is_higher_better else 0.3
    max_alpha = 0.3 if is_higher_better else 1
    min_line_style = "solid" if is_higher_better else "dashed"
    max_line_style = "dashed" if is_higher_better else "solid"
    min_hatch = "/" if is_higher_better else "o"
    max_hatch = "o" if is_higher_better else "/"

    cmap = cm.get_cmap("Set2")
    max_color = mcolors.to_hex(cmap(max_color_idx))
    min_color = mcolors.to_hex(cmap(min_color_idx))

    min_bar_height = stats["min_val"]
    max_bar_bottom = min_bar_height if is_higher_better else 0
    max_bar_height = stats["max_val"] - \
        stats["min_val"] if is_higher_better else stats["max_val"]
    max_bar_label = "Best-case" if is_higher_better else "Worst-case"
    min_bar_label = "Worst-case" if is_higher_better else "Best-case"

    max_bars = ax.bar(stats[x], max_bar_height, color=max_color, alpha=max_alpha, edgecolor="black", hatch=max_hatch,
                      linestyle=max_line_style, bottom=max_bar_bottom,
                      label=max_bar_label)

    min_bars = ax.bar(stats[x], min_bar_height, color=min_color, alpha=min_alpha, edgecolor="black", hatch=min_hatch,
                      linestyle=min_line_style, label=min_bar_label)
    for min_bar in min_bars:
        min_bar.set_linewidth(1)
    for max_bar in max_bars:
        max_bar.set_linewidth(1)

    for text in ax.legend(fontsize=12, facecolor="white", edgecolor="black", frameon=True).get_texts():
        text.set_color("black")

    return ax


def stacked_bar_plot(data: DataFrame, y_name: str, title: str, x: str = "exp_name", x_name: str = "Experiment Name",
                     base_name: str = None,
                     is_higher_better: bool = True):
    base_data = None

    if base_name is not None:
        base_name = exp_name_mapping[base_name] if base_name is not None else None
        base_data = data.loc[data[x] == base_name]

        data = data[data[x] != base_name]
    fig, ax = __basic_plot(__stacked_bar_plot, data, x,
                           x_name, y_name, title, is_higher_better)

    baseline_color = 'red'
    baseline_error_alpha = 0.2
    baseline_line_style = 'dotted'
    baseline_line_width = 1.5

    if base_data is not None:
        ax.axhspan(base_data["value"].min(), base_data["value"].max(
        ), color=baseline_color, alpha=baseline_error_alpha)

        ax.axhline(xmin=ax.get_xlim()[0], y=base_data["value"].mean(), color=baseline_color,
                   linestyle=baseline_line_style,
                   linewidth=baseline_line_width)

        handles, labels = ax.get_legend_handles_labels()

        labels = [base_name] + labels
        baseline_legend_line = Line2D([0], [1], color=baseline_color, linestyle=baseline_line_style,
                                      linewidth=baseline_line_width)
        baseline_legend_rect = Patch(
            facecolor=baseline_color, alpha=baseline_error_alpha, label=base_name)
        handles = [(baseline_legend_line, baseline_legend_rect)] + handles

        ax.legend(handles, labels)

    return fig


def event_throughput_line_plot(
        data: DataFrame,
        title: str,
        x_name: str = "Sample",
        y_name: str = "Event Throughput",
        hue: str = "repetition"
):
    if hue not in data.columns and not data.empty:
        raise ValueError(f"Column '{hue}' not found in provided data")

    if data.empty:
        fig, ax = plt.subplots(figsize=(6, 6))
    else:
        unique_series = data[hue].nunique() if hue in data else 1
        fig_width = max(6, unique_series * 2)
        fig, ax = plt.subplots(figsize=(fig_width, 6))

        sns.lineplot(
            data=data,
            x="index",
            y="value",
            hue=hue,
            units=hue,
            estimator=None,
            linewidth=2,
            ax=ax,
        )

    ax.set_xlabel(x_name, fontsize=18)
    ax.set_ylabel(y_name, fontsize=18)
    ax.set_title(title, fontsize=20)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)

    if hue in data.columns and not data.empty:
        ax.legend(title=hue, fontsize=12)

    return fig, ax


def event_throughput_boxplot(
        data: DataFrame,
        title: str,
        x: str = "repetition",
        x_name: str = "Repetition",
        y_name: str = "Event Throughput",
        hue: str | None = None,
):
    if data.empty:
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.set_xlabel(x_name, fontsize=18)
        ax.set_ylabel(y_name, fontsize=18)
        ax.set_title(title, fontsize=20)
        ax.set_ylim(bottom=0)
        return fig, ax

    required_columns = {x, "value"}
    if hue:
        required_columns.add(hue)

    missing = required_columns - set(data.columns)
    if missing:
        raise ValueError(f"Missing columns for boxplot: {', '.join(sorted(missing))}")

    num_categories = data[x].nunique()
    fig_width = max(6, num_categories * 1.5)
    fig, ax = plt.subplots(figsize=(fig_width, 6))

    sns.boxplot(data=data, x=x, y="value", hue=hue, ax=ax)

    ax.set_xlabel(x_name, fontsize=18)
    ax.set_ylabel(y_name, fontsize=18)
    ax.set_title(title, fontsize=20)
    ax.set_ylim(bottom=0)

    if hue:
        ax.legend(title=hue, fontsize=12)

    return fig, ax


def __operator_img(data: DataFrame, graph_name: str, key: str, op_type: OpType):
    graph = get_rep_with(data, key, op_type)["operator_graph"]

    graph.render(f"plots/{graph_name}", format="png")

    img = mpimg.imread(f"plots/{graph_name}.png")

    return img


def operator_plot(exp_data: dict, name: str, key: str, op_type: OpType):
    exp_name = exp_name_mapping[name]
    data = exp_data[exp_name]

    graph_name = f"{name}_graph"

    img = __operator_img(data, graph_name, key, op_type)

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.set_title(f"{exp_name}\nOperator Graph")
    ax.imshow(img)
    ax.axis('off')
    return fig


def dag_plot(exp_data: dict, name: str):
    exp_name = exp_name_mapping[name]
    data = exp_data[exp_name]

    graph = data["dag"]
    graph.render(f"plots/{name}_dag", format="png")
    img = mpimg.imread(f"plots/{name}_dag.png")

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.set_title(f"{exp_name}\nDAG")
    ax.imshow(img)
    ax.axis('off')

    return fig


def min_max_operator_plot(exp_data: dict, name: str, key: str):
    exp_name = exp_name_mapping[name]
    data = exp_data[exp_name]

    min_graph_name = f"{name}_min_graph"
    max_graph_name = f"{name}_max_graph"

    min_img = __operator_img(data, min_graph_name, key, OpType.MIN)
    max_img = __operator_img(data, max_graph_name, key, OpType.MAX)

    fig, axs = plt.subplots(1, 2, figsize=(8, 6))

    fig.suptitle(f"{exp_name}\nOperator Graphs")

    axs[0].imshow(min_img)
    axs[0].set_title(f"Min {key}")
    axs[0].axis('off')

    axs[1].imshow(max_img)
    axs[1].set_title(f"Max {key}")
    axs[1].axis('off')

    fig.tight_layout()
    return fig
