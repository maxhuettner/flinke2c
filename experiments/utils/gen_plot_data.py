from enum import Enum

import pandas as pd
import numpy as np
from utils.constants import exp_name_mapping
import os
import re
from scipy import stats


def gen_ecdf_data(df: pd.DataFrame, file_name: str, output_suffix: str = "", x_label: str = "Throughput (tps)", legend_strip: list[str] = None):
    """
    Generate ECDF data for pgfplots visualization.
    
    Args:utils
        df: DataFrame containing the data with 'exp_name' and 'value' columns
        file_name: Base name for the output CSV files
        x_label: Label for x-axis
        legend_strip: List of substrings to remove from legend entries
    """
    reverse_mapping = {label: key for key, label in exp_name_mapping.items()}

    for name, group in df.groupby("exp_name"):
        # Sort values for ECDF
        sorted_values = np.sort(group["value"])
        # Calculate ECDF values
        y_vals = np.arange(1, len(sorted_values) + 1) / len(sorted_values)
        
        # Create step-like pattern
        x_step = []
        y_step = []
        
        # Add starting point at y=0
        x_step.append(sorted_values[0])
        y_step.append(0)
        
        for i in range(len(sorted_values)):
            # Add point for current step
            x_step.append(sorted_values[i])
            y_step.append(y_vals[i])
            # Add point for next x value with same y (horizontal step)
            if i < len(sorted_values) - 1:
                x_step.append(sorted_values[i+1])
                y_step.append(y_vals[i])
        
        key = reverse_mapping[name]
        records = [{
            "x": x,
            "ecdf": y,
            "key": key,
            "label": name.replace("\n", r"\\"),
        } for x, y in zip(x_step, y_step)]

        ecdf_df = pd.DataFrame(records)
        suffix = f"_{output_suffix}" if output_suffix else ""
        ecdf_df.to_csv(f"plot_data{suffix}/{file_name}_{key}_ecdf.csv", index=False)
    
    # Generate PGFPlots code
    gen_pgfplots_code(file_name, x_label, output_suffix=output_suffix, plot_type=PlotType.ECDF, legend_strip=legend_strip)


def gen_kde_data(df: pd.DataFrame, file_name: str, x_label: str = "Throughput (tps)", legend_strip: list[str] = None):
    """
    Generate KDE data for pgfplots visualization.
    
    Args:
        df: DataFrame containing the data with 'exp_name' and 'value' columns
        file_name: Base name for the output CSV files
        x_label: Label for x-axis
        legend_strip: List of substrings to remove from legend entries
    """
    reverse_mapping = {label: key for key, label in exp_name_mapping.items()}

    for name, group in df.groupby("exp_name"):
        # Get values for KDE
        values = group["value"].values
        
        # Skip if there's only one data point
        if len(values) <= 1:
            print(f"Warning: Skipping KDE for {name} as it has only {len(values)} data point(s)")
            continue
        
        # Create a range of x values for the KDE
        x_min = values.min()
        x_max = values.max()
        x = np.linspace(x_min, x_max, 1000)
        
        # Calculate KDE
        kde = stats.gaussian_kde(values, bw_method=0.5)
        y = kde(x)
        
        # Ensure the curve starts and ends at 0
        y[0] = 0
        y[-1] = 0
        
        key = reverse_mapping[name]
        records = [{
            "x": x_val,
            "kde": y_val,
            "key": key,
            "label": name.replace("\n", r"\\"),
        } for x_val, y_val in zip(x, y)]

        kde_df = pd.DataFrame(records)
        kde_df.to_csv(f"plot_data/{file_name}_{key}_kde.csv", index=False)
    
    # Generate PGFPlots code
    gen_pgfplots_code(file_name, x_label, plot_type=PlotType.KDE, legend_strip=legend_strip)


class PlotType(Enum):
    ECDF = "ECDF"
    KDE = "KDE"


def gen_pgfplots_code(file_name: str, x_label: str, output_suffix: str = "",
                      width: str = "\\linewidth", height: str = "0.6\\linewidth",
                      plot_type: PlotType = PlotType.ECDF, legend_strip: list[str] = None):
    """
    Generate pgfplots code for visualization.
    
    Args:
        file_name: Base name of the CSV files
        x_label: Label for x-axis
        width: Width of the plot
        height: Height of the plot
        plot_type: Type of plot (ECDF or KDE)
        legend_strip: List of substrings to remove from legend entries
    """
    match plot_type:
        case PlotType.ECDF:
            y_label = "Cumulative Probability"
        case PlotType.KDE:
            y_label = "Density"

    # Find all matching CSV files
    folder_suffix = f"_{output_suffix}" if output_suffix else ""
    plot_data_dir = f"plot_data{folder_suffix}"
    plot_code_dir = f"plot_code{folder_suffix}"
    suffix = "_ecdf.csv" if plot_type == PlotType.ECDF else "_kde.csv"
    
    # Only match files that correspond to valid experiment names
    valid_keys = set(exp_name_mapping.keys())
    matching_files = []
    for f in os.listdir(plot_data_dir):
        if f.startswith(f"{file_name}_") and f.endswith(suffix):
            # Extract the key from the filename
            key = f[len(file_name) + 1:-len(suffix)]
            if key in valid_keys:
                matching_files.append(f)
    
    if not matching_files:
        raise ValueError(f"No valid data files found for {file_name} with type {plot_type.value}")
    
    # Define line styles and fill patterns for the plots
    line_styles = ['solid', 'dashed', 'dotted', 'dashdotted', 'densely dashed', 'densely dotted', 
                  'loosely dashed', 'loosely dotted', 'loosely dashdotted', 'densely dashdotted']
    fill_patterns = ['north east lines', 'north west lines', 'crosshatch', 'crosshatch dots',
                    'horizontal lines', 'vertical lines', 'grid', 'dots',
                    'bricks', 'checkerboard']
    
    # For KDE plots, find the maximum y value across all files
    ymax = 1.0  # Default for ECDF
    ymin = 0.0  # Default for ECDF
    if plot_type == PlotType.KDE:
        max_y = 0
        min_y = float('inf')
        for csv_file in matching_files:
            df = pd.read_csv(f"{plot_data_dir}/{csv_file}")
            max_y = max(max_y, df["kde"].max())
            min_y = min(min_y, df["kde"].min())
        ymax = max_y * 1.1  # Add 10% padding

    # Regular plot
    code = f"""\\begin{{tikzpicture}}
% Define colors
\\definecolor{{color1}}{{RGB}}{{31,119,180}}
\\definecolor{{color2}}{{RGB}}{{255,127,14}}
\\definecolor{{color3}}{{RGB}}{{44,160,44}}
\\definecolor{{color4}}{{RGB}}{{214,39,40}}
\\definecolor{{color5}}{{RGB}}{{148,103,189}}
\\definecolor{{color6}}{{RGB}}{{140,86,75}}
\\definecolor{{color7}}{{RGB}}{{227,119,194}}
\\definecolor{{color8}}{{RGB}}{{127,127,127}}
\\definecolor{{color9}}{{RGB}}{{188,189,34}}
\\definecolor{{color10}}{{RGB}}{{23,190,207}}

\\begin{{axis}}[
    width={width},
    height={height},
    xlabel={{{x_label}}},
    ylabel={{{y_label}}},
    xmin=0,
    ymin={ymin},
    ymax={ymax},
    ymajorgrids=true,
    xmajorgrids=true,
    legend pos=north west,
    legend style={{nodes={{scale=0.5, transform shape}}, legend cell align=left}},
    every axis plot/.append style={{very thick}},
    tick label style={{/pgf/number format/fixed}},
    x tick label style={{/pgf/number format/1000 sep=,}},
    scaled x ticks=false"""
    code += "\n]\n\n"

    # Sort files according to exp_name_mapping order
    file_order = {key: i for i, key in enumerate(exp_name_mapping.keys())}
    matching_files.sort(key=lambda f: file_order.get(f[len(file_name) + 1:-len(suffix)], float('inf')))

    # Add plots
    for i, csv_file in enumerate(matching_files):
        key = csv_file[len(file_name) + 1:-len(suffix)]
        color_name = f"color{i+1}"
        y_col = "ecdf" if plot_type == PlotType.ECDF else "kde"
        
        if plot_type == PlotType.ECDF:
            line_style = line_styles[i % len(line_styles)]
            legend_text = exp_name_mapping[key].replace("\n", " ").replace("%", "\\%")
            if legend_strip:
                for s in legend_strip:
                    legend_text = legend_text.replace(s, "")
            code += f"\\addplot[mark=none, color={color_name}, {line_style}] table[x=x, y={y_col}, col sep=comma] {{plot_data/{csv_file}}};\n"
            code += f"\\addlegendentry{{{legend_text.strip()}}}\n"
        else:
            pattern = fill_patterns[i % len(fill_patterns)]
            legend_text = exp_name_mapping[key].replace("\n", " ").replace("%", "\\%")
            if legend_strip:
                for s in legend_strip:
                    legend_text = legend_text.replace(s, "")
            code += f"\\addlegendimage{{area legend, line width=0pt, fill={color_name}!20, pattern={pattern}, pattern color={color_name}}}\n"
            code += f"\\addplot[mark=none, color={color_name}, fill={color_name}!20, pattern={pattern}, pattern color={color_name}, forget plot] table[x=x, y={y_col}, col sep=comma] {{plot_data/{csv_file}}};\n"
            code += f"\\addlegendentry{{{legend_text.strip()}}}\n"
    
    code += "\\end{axis}\n\\end{tikzpicture}"

    with open(f"{plot_code_dir}/{file_name}_{plot_type.value.lower()}.tex", "w", encoding="utf8") as f:
        f.write(code)


def gen_event_line_data(
        df: pd.DataFrame,
        file_name: str,
        output_suffix: str = "",
        use_smoothed: bool = False
) -> list[dict[str, str]]:
    """Generate per-repetition CSV data suitable for pgfplots line plots.

    Args:
        df: DataFrame containing `exp_name`, `repetition`, `index`, and
            either `value` or `value_smoothed` columns. Optional columns
            `exp_name_tex`/`repetition_tex` improve legend handling.
        file_name: Base name for the output CSV files.
        output_suffix: Optional suffix that selects `plot_data_<suffix>`.
        use_smoothed: When True, prefer the `value_smoothed` column if present.

    Returns:
        A list of metadata dicts with the csv `path`, original `exp_name`,
        `repetition`, and TeX-ready labels for convenience when
        constructing pgfplots commands.
    """

    if df.empty:
        return []

    value_column = "value_smoothed" if use_smoothed and "value_smoothed" in df.columns else "value"
    if value_column not in df.columns:
        raise ValueError(f"Column '{value_column}' not found in DataFrame")

    reverse_mapping = {label: key for key, label in exp_name_mapping.items()}

    suffix = f"_{output_suffix}" if output_suffix else ""
    output_dir = f"plot_data{suffix}"
    os.makedirs(output_dir, exist_ok=True)

    metadata: list[dict[str, str]] = []

    def _sanitize(part: str) -> str:
        return re.sub(r"[^A-Za-z0-9_-]+", "_", part)

    grouped = df.groupby(["exp_name", "repetition"])
    for (exp_name, repetition), group in grouped:
        label_key = reverse_mapping.get(exp_name, exp_name)
        exp_part = _sanitize(label_key)
        rep_part = _sanitize(repetition)

        output_path = os.path.join(output_dir, f"{file_name}_{exp_part}_{rep_part}.csv")

        export_df = group.sort_values("index")[["index", value_column]].rename(columns={
            "index": "sample",
            value_column: "value",
        })

        export_df.to_csv(output_path, index=False)

        label_tex = group["exp_name_tex"].iloc[0] if "exp_name_tex" in group.columns else exp_name
        repetition_tex = group["repetition_tex"].iloc[0] if "repetition_tex" in group.columns else repetition

        metadata.append({
            "path": output_path,
            "exp_name": exp_name,
            "repetition": repetition,
            "label_tex": label_tex,
            "repetition_tex": repetition_tex,
        })

    return metadata
