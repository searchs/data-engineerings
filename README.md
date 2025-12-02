# Data Engineering

Data engineering is the process of building and maintaining the infrastructure that supports data processing and analysis. This includes tasks such as data collection, data cleaning, data transformation, data storage, and data visualization. Data engineers use a variety of tools and technologies to perform these tasks, including programming languages such as Python and SQL, data processing frameworks such as Apache Spark and Hadoop, and data visualization tools such as Tableau and Power BI.

This repository contains a collection of data engineering projects and resources. The projects are organized by topic, and each project includes a README file with instructions on how to run the code. The resources include articles, tutorials, and other materials that can help you learn more about data engineering.

I hope you find this repository helpful. If you have any questions or feedback, please feel free to contact me.

## TODO: Faster Pipelines and Solutions

Rust programming language is a good candidate for building fast and reliable data pipelines.  UV, RV and a host of other tools are built with Rust and the effect is visible in their performance. I hope to learn Rust and build a few pipelines with it or include some Rust tools in my existing pipelines.

Godspeed to me!

## Using `just` + `uv` (Monorepo)

- **Purpose**: The repository root contains a parent `justfile` (`./justfile`) with common Python workflows implemented using the `uv` tool. Subprojects (for example `azure/`) should inherit these recipes to keep behavior consistent across the monorepo.
- **Reference doc**: More details and examples are available in `JUST_UV.md`.
- **Install prerequisites**: Make sure `just` and `uv` are available on your PATH. For macOS you can install `just` via Homebrew and `uv` via `pipx` or your preferred method:

```bash
brew install just
pipx install uv   # or: pip install --user uv, or your preferred installer
```

- **List available recipes**:

```bash
just --list
```

- **Run a common recipe from a subproject** (example: `azure`):

```bash
cd azure
just test
```

- **How to inherit the parent justfile**: In each subproject create a `justfile` containing:

```makefile
# Data Engineering

Data engineering is the process of building and maintaining the infrastructure that supports data processing and analysis. This includes tasks such as data collection, data cleaning, data transformation, data storage, and data visualization. Data engineers use a variety of tools and technologies to perform these tasks, including programming languages such as Python and SQL, data processing frameworks such as Apache Spark and Hadoop, and data visualization tools such as Tableau and Power BI.

This repository contains a collection of data engineering projects and resources. The projects are organized by topic, and each project includes a README file with instructions on how to run the code. The resources include articles, tutorials, and other materials that can help you learn more about data engineering.

I hope you find this repository helpful. If you have any questions or feedback, please feel free to contact me.

## TODO: Faster Pipelines and Solutions

Rust programming language is a good candidate for building fast and reliable data pipelines. UV, RV and a host of other tools are built with Rust and the effect is visible in their performance. I hope to learn Rust and build a few pipelines with it or include some Rust tools in my existing pipelines.

Godspeed to me!

## Using `just` + `uv`

Short guide and examples for using the monorepo `justfile` with `uv` are available in `JUST_UV.md`.
