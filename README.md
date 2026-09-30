# Data Engineering Labs

A canonical repository for practical data-engineering experiments, reusable patterns and case studies across cloud platforms, streaming, distributed processing, orchestration and data-quality workflows.

This repository is being consolidated from a number of older standalone projects so that related work can share one tooling baseline, one CI pipeline and one discoverable structure.

## Target structure

```text
cloud/
  aws/
  azure/
  gcp/
streaming/
  kafka/
processing/
  spark/
    python/
    java/
    scala/
  polars/
pipelines/
orchestration/
databases/
ingestion/
analytics/
case-studies/
docs/
tests/
```

## Tooling

The repository uses `uv` for Python environments and dependency management and `just` for repeatable development commands. Subprojects should inherit the root conventions where practical rather than creating unrelated local workflows.

See `JUST_UV.md` for the current command workflow and `MIGRATION_PLAN.md` for the consolidation roadmap.

## Principles

- Preserve useful authored implementations and case studies.
- Keep transparent attribution for course-derived projects.
- Avoid importing generated outputs, large disposable datasets or vendored dependencies.
- Prefer reproducible examples with tests and documented run instructions.
- Retire source repositories only after migrated content has been verified here.
