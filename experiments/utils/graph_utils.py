import json
import glob
from pathlib import Path

from graphviz import Digraph
import seaborn as sns

ip_labels = {
    "10.10.10.1": "Source",
    "10.10.10.2": "ZS02",
    "10.10.10.3": "ZS03",
    "10.10.10.5": "ZS05",
    "10.10.10.7": "ZS07",
    "10.10.10.8": "ZS08",
    "10.10.10.6": "Sink",
}


def get_distinct_colors(num_colors):
    color_palette = sns.color_palette("husl", num_colors)  # Get distinct colors
    return [f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}" for r, g, b in color_palette]


def process_flink_dag(exp_path: Path):
    if not (exp_path / "dag.json").exists():
        return None
    with open(f"{exp_path}/dag.json", "r") as f:
        dag = json.load(f)
    dot = Digraph()
    dot.attr(rankdir="LR")

    vertices = dag["vertices"]
    operator_names = [vertex["name"] for vertex in vertices]
    colors = get_distinct_colors(len(operator_names))
    color_map = {name: colors[i] for i, name in enumerate(sorted(operator_names))}

    for vertex in vertices:
        node_id = vertex["id"]
        node_label = vertex["name"]
        font_color = color_map[node_label]
        dot.node(node_id, shape='box', label=node_label, fontcolor=font_color)

    for node in dag["plan"]["nodes"]:
        node_id = node["id"]
        if "inputs" in node:
            for inp in node["inputs"]:
                dot.edge(inp["id"], node_id)

    return dot


def generate_node_label(name: str, ip: str, tasks=None, color_map=None):
    task_str = ""
    if tasks:
        task_names = []
        for task in tasks:
            color = color_map[task['name']]
            task_names.append(f"<font color=\"{color}\">" + task['name'] + "</font>")
        task_str = '<br/>'.join(task_names)

    return f"<{name} ({ip})<br/><br/>{task_str}>" if task_str else f"{name} ({ip})"


def process_operator_diagram(repetition_path: str):
    files = glob.glob(f"{repetition_path}/task_per_jobmanager.json")
    if not files:
        return None
    operator_graph_file = files[0]
    with open(operator_graph_file, "r", encoding='utf8') as f:
        task_data = json.load(f)
    dot = Digraph()
    dot.attr('node', shape='box', style='solid', color='black')

    task_names = sorted(list({task['name'] for tasks in task_data for task in tasks['tasks']}))
    colors = get_distinct_colors(len(task_names))
    color_map = {name: colors[i] for i, name in enumerate(task_names)}

    for ip, label in ip_labels.items():
        node_tasks = next((entry['tasks'] for entry in task_data if entry['name'] == ip), None)
        node_label = generate_node_label(label, ip, node_tasks, color_map)
        if label == "Source" or label == "Sink":
            dot.node(ip, label=node_label, style="dashed")
        else:
            dot.node(ip, label=node_label)

    edges = [
        ("10.10.10.1", "10.10.10.2"),
        ("10.10.10.2", "10.10.10.3"),
        ("10.10.10.2", "10.10.10.5"),
        ("10.10.10.3", "10.10.10.7"),
        ("10.10.10.5", "10.10.10.7"),
        ("10.10.10.7", "10.10.10.8"),
        ("10.10.10.8", "10.10.10.6")
    ]
    for src, dst in edges:
        if src == "10.10.10.1" or dst == "10.10.10.6":
            dot.edge(src, dst, dir="both", style="dotted")
        else:
            dot.edge(src, dst, dir="both")

    return dot
