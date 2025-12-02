# Using `just` + `uv` (Monorepo)

Purpose: The repository root contains a parent `justfile` (`./justfile`) with common Python workflows implemented using the `uv` tool. Subprojects (for example `azure/`) should inherit these recipes to keep behavior consistent across the monorepo.

Prerequisites: Make sure `just` and `uv` are available on your PATH. For macOS you can install `just` via Homebrew and `uv` via `pipx` or your preferred method:

```bash
brew install just
pipx install uv   # or: pip install --user uv, or your preferred installer
```

Examples:

- List available recipes:

```bash
just --list
```

- Run a common recipe from a subproject (example: `azure`):

```bash
cd azure
just test
```

- How to inherit the parent justfile: In each subproject create a `justfile` containing:

```makefile
include '../justfile'
```

This will expose the parent recipes in the subproject so you can run `just install`, `just test`, `just shell`, etc. from inside the subproject directory.

If a subproject needs overrides or extra recipes, add them below the `include` line in the subproject `justfile`.
