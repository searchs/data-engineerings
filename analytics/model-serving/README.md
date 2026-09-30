# Legacy model-serving experiments

Curated from the old `analytics` repository, which contained notebooks for deploying a prediction model/API and serving individual predictions.

The durable engineering lessons are:

- keep feature preparation identical between training and inference
- version models and their input/output contracts
- validate request payloads before inference
- separate model loading from request handling
- add health/readiness endpoints independently of prediction endpoints
- capture latency, failures and model/version metadata in observability
- make inference deterministic where possible and test representative fixtures

The original notebooks were not copied because their runtime/dependency assumptions are historical. A modern implementation should package the model behind a small typed service with contract tests and reproducible model artefact loading.
