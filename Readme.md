# FlinkE2C Artifact (EDBT 2026)

This repository bundles all software artifacts accompanying the EDBT 2026 submission on FlinkE2C and CEPless. Each subproject keeps its own build instructions and documentation—start with the individual READMEs referenced below.

## Repository Layout

- `flinke2c/` – FlinkE2C prototype built on top of Apache Flink. Contains the modified runtime, scheduler, and supporting modules used in the paper.
- `flinke2c_cepless/` – Variant of the FlinkE2C prototype integrated with the CEPless execution mode.
- `CEPless/` – Standalone CEPless stack (scripts, configs, and services).
- `experiments/` – Data, plotting utilities, and the `exps.ipynb` notebook that consolidates measurements and regenerates every figure.
- `streaming-tcp-harness/` – Rust-based harness for benchmarking experiments over TCP; see `streaming-tcp-harness/Readme.md` for build and logging options.

## Getting Started

1. Pick the component you want to inspect and open its README for setup steps.
2. Use `experiments/exps.ipynb` to explore the consolidated results and regenerate plots.
3. Consult the harness README if you need to replay workloads or capture additional event-level metrics.

## Acknowledgements

This artifact builds upon the following open-source systems:

- [Apache Flink](https://github.com/apache/flink) – the upstream stream processing engine extended by the FlinkE2C prototypes.
- [CEPless](https://github.com/luthramanisha/CEPless) – the serverless CEP middleware integrated for heterogeneous deployments.
