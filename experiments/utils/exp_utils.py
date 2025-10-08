from __future__ import annotations

import math
from enum import Enum
from pathlib import Path
from typing import Any, NotRequired, TypedDict, cast
from collections.abc import Mapping, Sequence

import pandas as pd
from pandas import DataFrame, Series
from pandas.errors import EmptyDataError

from utils.constants import exp_name_mapping
from utils.graph_utils import process_operator_diagram, process_flink_dag
from graphviz import Digraph


class NesRepetitionData(TypedDict):
    throughput: int


class NesExperimentData(TypedDict):
    avg_throughput: float
    repetitions: dict[str, NesRepetitionData]


class NebulaRepetitionData(TypedDict):
    throughput: float
    latency_seconds: float
    latency_ms: float
    total_events: int
    source_start: pd.Timestamp
    sink_end: pd.Timestamp
    event_throughput: list[float]


class NebulaExperimentData(TypedDict):
    avg_throughput: float
    avg_latency_seconds: float
    repetitions: dict[str, NebulaRepetitionData]


class NmRepetitionData(TypedDict):
    runtime: int
    throughput: int
    operator_graph: Digraph | None


class NmExperimentData(TypedDict):
    avg_throughput: float
    avg_runtime: float
    repetitions: dict[str, NmRepetitionData]
    dag: NotRequired[Digraph | None]


class StreamingRepetitionData(TypedDict):
    runtime: int
    source_throughput: Series
    avg_source_throughput: float
    sink_throughput: Series
    avg_sink_throughput: float
    operator_graph: Digraph | None


class ExperimentData(TypedDict):
    avg_avg_source_throughput: float
    avg_avg_sink_throughput: float
    avg_runtime: float
    repetitions: dict[str, StreamingRepetitionData]
    dag: NotRequired[Digraph | None]


ExperimentSummary = ExperimentData | NmExperimentData | NesExperimentData | NebulaExperimentData
ExperimentWithDag = ExperimentData | NmExperimentData
OperatorRepetition = NmRepetitionData | StreamingRepetitionData


class PlotRow(TypedDict):
    exp_name: str
    value: float


def process_source_throughput(repetition_path: Path | str) -> Series:
    repetition_dir = Path(repetition_path)
    source_file = next(repetition_dir.glob("benchmark_*.csv"))
    source = pd.read_csv(source_file, usecols=["time", "rate"])
    source["time"] = pd.to_datetime(source["time"])

    return source["rate"]


def process_sink_throughput(repetition_path: Path | str) -> Series:
    repetition_dir = Path(repetition_path)
    sink_file = next(repetition_dir.glob("sink_*.csv"))
    sink = pd.read_csv(sink_file, usecols=["ts", "message"])
    sink["ts_seconds"] = pd.to_datetime(sink["ts"]).dt.floor('s')

    sink_tput = sink.groupby("ts_seconds").count().reset_index()
    return sink_tput["message"]


def process_tput_and_runtime(repetition_path: Path | str) -> tuple[int, int]:
    repetition_dir = Path(repetition_path)
    with open(repetition_dir / "nexmark.txt", "r", encoding='utf8') as f:
        for line in f.read().splitlines():
            if line.strip().startswith("|Total"):
                parts = [col.strip() for col in line.strip('|').split('|')]
                events_raw = parts[1].replace(",", "")
                time_s = float(parts[3])

                if not events_raw or events_raw.lower() == "nan":
                    raise ValueError("Events Num is missing in nexmark.txt Total row")

                events_num = float(events_raw)
                if not math.isfinite(events_num) or events_num <= 0:
                    raise ValueError(f"Invalid Events Num value: {parts[1]}")

                if not math.isfinite(time_s) or time_s <= 0:
                    raise ValueError(f"Invalid Time(s) value: {parts[3]}")

                throughput = int(events_num / time_s)
                time_ms = round(time_s * 1000)

                return throughput, time_ms

    raise ValueError("No Total line found in nexmark.txt")


def process_runtime(repetition_path: Path | str) -> int:
    repetition_dir = Path(repetition_path)
    with open(repetition_dir / "runtime.txt", "r", encoding='utf8') as f:
        return int(f.read())

def process_tput_nes(repetition_path: Path) -> int:
    with open(repetition_path / "tps.txt", "r", encoding='utf8') as f:
        return int(f.read())


def process_repetitions_nes(exp_path: Path) -> NesExperimentData:
    repetitions: dict[str, NesRepetitionData] = {}
    for repetition_path in exp_path.iterdir():
        if repetition_path.is_dir():
            repetitions[repetition_path.name] = {'throughput': process_tput_nes(repetition_path)}

    avg_throughput = pd.Series([rep["throughput"] for rep in repetitions.values()]).mean()

    return {
        "avg_throughput": avg_throughput,
        "repetitions": repetitions,
    }


def _find_io_dir(exp_path: Path, dirname: str) -> Path:
    direct_path = exp_path / dirname
    if direct_path.is_dir():
        return direct_path

    logs_path = exp_path / "logs" / dirname
    if logs_path.is_dir():
        return logs_path

    candidates = sorted(
        [p for p in exp_path.glob(f"**/{dirname}") if p.is_dir()],
        key=lambda p: len(p.parts)
    )

    if candidates:
        return candidates[0]

    raise FileNotFoundError(f"Could not locate '{dirname}' directory in {exp_path}")


def _read_time_file(file_path: Path) -> DataFrame | None:
    try:
        df = pd.read_csv(file_path)
    except EmptyDataError:
        return None

    if df.empty:
        return None

    return df


def _extract_rep_id(path: Path) -> str | None:
    stem = path.stem
    if "_" not in stem:
        return None

    parts = stem.rsplit("_", 1)
    if len(parts) != 2:
        return None

    return parts[1]


def _read_events_file(file_path: Path) -> DataFrame | None:
    try:
        df = pd.read_csv(file_path)
    except EmptyDataError:
        return None

    if df.empty or "rate" not in df:
        return None

    time_col_candidates = ("time", "timestamp", "ts")
    time_col = next((col for col in time_col_candidates if col in df), None)
    if time_col is None:
        return None

    events_df = df[[time_col, "rate"]].copy()
    events_df[time_col] = pd.to_datetime(events_df[time_col], errors="coerce")
    events_df["rate"] = pd.to_numeric(events_df["rate"], errors="coerce")
    events_df = events_df.dropna(subset=[time_col, "rate"])

    if events_df.empty:
        return None

    events_df = events_df.rename(columns={time_col: "time"})
    return events_df.sort_values("time")


def _compute_boxplot_stats(values: Sequence[float]) -> dict[str, float]:
    series = pd.Series(list(values)).dropna()
    if series.empty:
        return {}

    q1 = float(series.quantile(0.25))
    q3 = float(series.quantile(0.75))
    median = float(series.quantile(0.5))
    min_val = float(series.min())
    max_val = float(series.max())

    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    lower_candidates = series[series >= lower_bound]
    upper_candidates = series[series <= upper_bound]

    lower_whisker = float(lower_candidates.min()) if not lower_candidates.empty else min_val
    upper_whisker = float(upper_candidates.max()) if not upper_candidates.empty else max_val

    return {
        "count": int(series.count()),
        "mean": float(series.mean()),
        "median": median,
        "q1": q1,
        "q3": q3,
        "min": min_val,
        "max": max_val,
        "lower_whisker": lower_whisker,
        "upper_whisker": upper_whisker,
    }


def _tex_escape(text: str) -> str:
    replacements = {
        "\\": r"\\textbackslash{}",
        "_": r"\\_",
        "%": r"\\%",
        "&": r"\\&",
        "#": r"\\#",
        "{": r"\\{",
        "}": r"\\}",
        "$": r"\\$",
        "~": r"\\textasciitilde{}",
        "^": r"\\textasciicircum{}",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = text.replace("|", r"\\textbar{}")
    text = text.replace("\n", r"\\")
    return text


def process_repetitions_nebula(exp_path: Path) -> NebulaExperimentData:
    source_dir = _find_io_dir(exp_path, "source")
    sink_dir = _find_io_dir(exp_path, "sink")

    repetition_ids: set[str] = set()
    for time_file in source_dir.rglob("time_*_*.csv"):
        rep_id = _extract_rep_id(time_file)
        if rep_id is not None:
            repetition_ids.add(rep_id)

    repetitions: dict[str, NebulaRepetitionData] = {}
    throughputs: list[float] = []
    latencies: list[float] = []

    for rep_id in sorted(repetition_ids, key=lambda x: (len(x), x)):
        start_times: list[pd.Timestamp] = []
        total_events = 0.0
        events_frames: list[DataFrame] = []

        for time_file in source_dir.rglob(f"time_*_{rep_id}.csv"):
            df = _read_time_file(time_file)
            if df is None:
                continue

            if "start_time" in df:
                start_series = pd.to_datetime(df["first_elem_time"], errors="coerce").dropna()
                if not start_series.empty:
                    start_times.append(start_series.min())

            if "num_events" in df:
                total_events += pd.to_numeric(df["num_events"], errors="coerce").fillna(0).sum()

        for events_file in sink_dir.rglob(f"events_*_{rep_id}.csv"):
            events_df = _read_events_file(events_file)
            if events_df is not None:
                events_frames.append(events_df)

        if not start_times:
            continue

        earliest_start = min(start_times)

        end_times: list[pd.Timestamp] = []
        for time_file in sink_dir.rglob(f"time_*_{rep_id}.csv"):
            df = _read_time_file(time_file)
            if df is None or "end_time" not in df:
                continue

            end_series = pd.to_datetime(df["end_time"], errors="coerce").dropna()
            if not end_series.empty:
                end_times.append(end_series.max())

        if not end_times:
            continue

        latest_end = max(end_times)
        latency = latest_end - earliest_start
        latency_seconds = latency.total_seconds()

        if latency_seconds <= 0:
            continue

        total_events_int = int(total_events)
        throughput = total_events_int / latency_seconds if total_events_int else 0.0

        if events_frames:
            combined_events = pd.concat(events_frames, ignore_index=True)
            combined_events = combined_events.groupby("time", as_index=False)["rate"].sum()
            event_throughput = combined_events.sort_values("time")["rate"].tolist()
        else:
            event_throughput = []

        repetitions[f"rep_{rep_id}"] = {
            "throughput": throughput,
            "latency_seconds": latency_seconds,
            "latency_ms": latency_seconds * 1000,
            "total_events": total_events_int,
            "source_start": earliest_start,
            "sink_end": latest_end,
            "event_throughput": event_throughput,
        }

        throughputs.append(throughput)
        latencies.append(latency_seconds)

    avg_throughput = pd.Series(throughputs).mean() if throughputs else float("nan")
    avg_latency = pd.Series(latencies).mean() if latencies else float("nan")

    return {
        "avg_throughput": avg_throughput,
        "avg_latency_seconds": avg_latency,
        "repetitions": repetitions,
    }

def process_repetition_nm(repetition_path: Path | str) -> NmRepetitionData:
    repetition_dir = Path(repetition_path)
    throughput, runtime = process_tput_and_runtime(repetition_dir)

    return {
        "runtime": runtime,
        "throughput": throughput,
        "operator_graph": process_operator_diagram(str(repetition_dir)),
    }


def process_repetition(repetition_path: Path | str) -> StreamingRepetitionData:
    repetition_dir = Path(repetition_path)
    source_tput = process_source_throughput(repetition_dir)
    sink_tput = process_sink_throughput(repetition_dir)
    runtime = process_runtime(repetition_dir)

    return {
        "runtime": runtime,
        "source_throughput": source_tput,
        "avg_source_throughput": float(source_tput.mean()),
        "sink_throughput": sink_tput,
        "avg_sink_throughput": float(sink_tput.mean()),
        "operator_graph": process_operator_diagram(str(repetition_dir)),
    }


def process_repetitions_nm(exp_path: Path) -> NmExperimentData:
    repetitions: dict[str, NmRepetitionData] = {}
    for repetition_path in exp_path.iterdir():
        if repetition_path.is_dir():
            repetitions[repetition_path.name] = process_repetition_nm(repetition_path)

    avg_throughput = pd.Series([rep["throughput"] for rep in repetitions.values()]).mean()
    avg_runtime = float(pd.Series([
        rep["runtime"] for rep in repetitions.values()
    ]).mean())

    return {
        "avg_throughput": avg_throughput,
        "avg_runtime": avg_runtime,
        "repetitions": repetitions,
    }


def process_repetitions(exp_path: Path) -> ExperimentData:
    repetitions: dict[str, StreamingRepetitionData] = {}
    for repetition_path in exp_path.iterdir():
        if repetition_path.is_dir():
            repetitions[repetition_path.name] = process_repetition(repetition_path)

    avg_exp_source_throughput = float(pd.concat([
        rep["source_throughput"] for rep in repetitions.values()
    ]).mean())
    avg_exp_sink_throughput = float(pd.concat([
        rep["sink_throughput"] for rep in repetitions.values()
    ]).mean())
    avg_runtime = float(pd.Series([
        rep["runtime"] for rep in repetitions.values()
    ]).mean())

    return {
        "avg_avg_source_throughput": avg_exp_source_throughput,
        "avg_avg_sink_throughput": avg_exp_sink_throughput,
        "avg_runtime": avg_runtime,
        "repetitions": repetitions
    }

def process_experiments_paper(folder_name: str, names: Sequence[str] | None = None) -> dict[str, NebulaExperimentData]:
    base_path = Path("data")
    path = base_path / Path(folder_name)
    experiments: dict[str, NebulaExperimentData] = {}

    experiment_names = names if names is not None else ["hom_hom", "hom_het", "het_het"]

    for exp_name in experiment_names:
        exp_path = path / exp_name
        if not exp_path.is_dir():
            continue

        label = exp_name_mapping.get(exp_name, exp_name)
        experiments[label] = process_repetitions_nebula(exp_path)

    return experiments

def process_experiments(path: str = "data", names: Sequence[str] | None = None) -> dict[str, ExperimentSummary]:
    base_path = Path(path)
    experiments: dict[str, ExperimentSummary] = {}

    exp_paths = {exp_path.name: exp_path for exp_path in base_path.iterdir() if exp_path.is_dir()}

    experiment_order = list(names) if names is not None else list(exp_name_mapping.keys())

    for exp_name in experiment_order:
        if path == "data/nes_q1":
            exp_path = exp_paths.get(exp_name)
            if exp_path is None:
                continue
            label = exp_name_mapping.get(exp_name, exp_name)
            experiments[label] = process_repetitions_nes(exp_path)
            continue

        exp_path = exp_paths.get(exp_name)
        if exp_path is None:
            continue

        label = exp_name_mapping.get(exp_name, exp_name)
        experiment_result: ExperimentWithDag
        experiment_result = process_repetitions_nm(exp_path)

        experiment_result["dag"] = process_flink_dag(exp_path)

        experiments[label] = experiment_result

    return experiments


def prepare_exp_data(
        exp_data: Mapping[str, ExperimentSummary],
        key: str
) -> DataFrame:
    plot_data: list[PlotRow] = []

    for exp_name, info in exp_data.items():
        repetitions = cast(Mapping[str, Mapping[str, Any]], info["repetitions"])
        for rep in repetitions.values():
            plot_data.append({
                "exp_name": exp_name,
                "value": float(rep[key])
            })

    return pd.DataFrame(plot_data)


def prepare_relative_exp_data(
        exp_data: Mapping[str, ExperimentSummary],
        key: str
) -> DataFrame:
    plot_data: list[PlotRow] = []

    for exp_name, info in exp_data.items():
        repetitions = cast(Mapping[str, Mapping[str, Any]], info["repetitions"])
        for rep in repetitions.values():
            plot_data.append({
                "exp_name": exp_name,
                "value": float(rep[key]),
                "abs_value": round(rep[key])
            })

    df = pd.DataFrame(plot_data)
    group_max = df.groupby("exp_name")["value"].transform("max")
    df["value"] = df["value"] / group_max

    return df


def prepare_nebula_event_throughput_data(
        exp_data: Mapping[str, NebulaExperimentData],
        smooth_window: int | None = None
) -> DataFrame:
    plot_data: list[dict[str, Any]] = []

    for exp_name, info in exp_data.items():
        repetitions = info["repetitions"]
        for rep_name, rep in repetitions.items():
            event_tput = rep.get("event_throughput", [])
            smoothed = None
            if smooth_window is not None and smooth_window > 1 and event_tput:
                series = pd.Series(event_tput, dtype=float)
                smoothed = series.rolling(window=smooth_window, center=True, min_periods=1).mean()

            for index, value in enumerate(event_tput):
                record: dict[str, Any] = {
                    "exp_name": exp_name,
                    "repetition": rep_name,
                    "exp_name_tex": _tex_escape(exp_name),
                    "repetition_tex": _tex_escape(rep_name),
                    "index": index,
                    "value": float(value),
                }
                if smoothed is not None:
                    record["value"] = float(smoothed.iat[index])

                plot_data.append(record)

    return pd.DataFrame(plot_data)


def prepare_nebula_event_boxplot_summary(
        exp_data: Mapping[str, NebulaExperimentData]
) -> DataFrame:
    plot_data: list[dict[str, Any]] = []

    for exp_name, info in exp_data.items():
        repetitions = info["repetitions"]
        for rep_name, rep in repetitions.items():
            stats = _compute_boxplot_stats(rep.get("event_throughput", []))
            if not stats:
                continue

            record: dict[str, Any] = {
                "exp_name": exp_name,
                "repetition": rep_name,
                "exp_name_tex": _tex_escape(exp_name),
                "repetition_tex": _tex_escape(rep_name),
            }
            record.update(stats)
            plot_data.append(record)

    return pd.DataFrame(plot_data)


class OpType(Enum):
    MIN = 1
    MAX = 2


def get_rep_with(data: ExperimentWithDag, key: str, op_type: OpType) -> OperatorRepetition:
    def fn(item: OperatorRepetition) -> Any:
        return item[key]

    repetitions = cast(dict[str, OperatorRepetition], data["repetitions"])

    match op_type:
        case OpType.MIN:
            return min(repetitions.values(), key=fn)
        case OpType.MAX:
            return max(repetitions.values(), key=fn)

    raise ValueError(f"Unsupported OpType: {op_type}")
