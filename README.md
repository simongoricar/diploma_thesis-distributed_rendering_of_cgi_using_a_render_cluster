<div align="center">
  <h2 align="center">Distributed rendering of computer-generated <br>imagery using a render cluster</h2>
  <h5 align="center">Authors: Simon Goričar and Assoc. Prof. dr. Uroš Lotrič (mentor)</h5>
</div>

This is a git repository containing both the render cluster implementation and scripts for 
testing and analyzing its performance.
It is part of a diploma thesis.

---


# 1. Project structure
```markdown
|-- analysis
|   |> Contains a set of tools for analysis and plotting of the results, written in Python.
|
|-- blender-projects
|   |> Contains a set of reference Blender projects that were ran in the performance tests. 
|
|-- master
|   |> Rust crate containing an implementation of the cluster coordinator.
|
|-- scripts
|   |> Contains an assortment of scripts used for testing the render cluster 
|      on NSC and Arnes clusters using SLURM.
|
|-- shared
|   |> Rust crate containing structures and functionality shared between 
|      the cluster coordinator and workers.
|
|-- worker
|   |> Rust crate containing an implementation of the cluster worker.
```


# 2. Compiling
First, install [Rust](https://www.rust-lang.org/learn/get-started). Then, run the following command:
```bash
cargo build --workspace --release
```

Two binaries will be generated in the `./target/release` directory: `master` and `worker`.
Execute `master --help` and/or `worker --help` to understand how to set them up. 
For more information, take a look at how the scripts (see `scripts/arnes/*.sh`) set up test runs.


# 3. Running tests
To run tests on the SLURM workload manager, take a look at the scripts provided in 
`scripts/arnes` and `scripts/arnes/exclusive`.


# 4. Comparing results
First, install [Python 3.11](https://www.python.org/) and [Poetry](https://python-poetry.org/).
Then, run the following command in the `analysis` directory:

```bash
poetry install
```

You're ready to analyze and plot. 
Run any individual script (e.g. `speedup.py`) to generate a single set of plots. 
To generate all the available plots (using `run_all.py`), like so:

```bash
poetry run python run_all.py
```

These scripts expect the directory `blender-projects/04_very-simple/results/arnes-results` to contain
one or more raw tracing files (e.g. `2023-07-13_22-25-42_job-04vs_demo_10f-1w_eager-naive-coarse_raw-trace.json`).

Plots will be generated in the `analysis/plots` directory. 
The plots from our tests are also available in the same directory. 
