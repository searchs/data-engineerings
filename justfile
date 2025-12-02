# Parent justfile for the monorepo — common uv-based Python tasks
# Subprojects can inherit by adding, for example, `include "../justfile"` in their own justfile.

# Use the system shell and assume `uv` is on PATH.

help:
azure/@echo "Monorepo parent justfile — common uv-based Python tasks"
azure/@echo ""
azure/@echo "Usage: just <recipe>"
azure/@echo "Common recipes: bootstrap, install, update, shell, run, test, lint, format, build, freeze, clean, venv"

bootstrap:
azure/uv bootstrap

install:
azure/uv install

update:
azure/uv update

shell:
azure/uv shell

run:
azure/uv run

test:
azure/uv test

lint:
azure/uv lint

format:
azure/uv format

build:
azure/uv build

freeze:
azure/uv freeze > requirements.lock

clean:
azure/uv clean || true

venv:
azure/uv venv

note:
azure/@echo "To inherit from this parent justfile add an 'include' line in your subproject justfile:"
azure/@echo "  include '../justfile'"

# Commit using Commitizen (cz)
commit:
azure/uv run cz commit
