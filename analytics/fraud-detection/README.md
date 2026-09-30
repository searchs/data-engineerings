# Fraud-detection analysis — historical experiment

The source `fraud-detection-apps` repository was actually a broad Python-learning sandbox whose README called it `pybox`. One sizeable fraud notebook was mixed with unrelated exercises, APIs, audio experiments and data-processing practice.

Rather than preserve the sandbox as a fraud product, keep the durable fraud-analysis workflow here:

1. establish class balance and fraud prevalence
2. separate identifiers/leakage-prone fields from predictive features
3. split data before fitting transformations
4. use precision/recall, PR-AUC and cost-sensitive metrics rather than accuracy alone
5. choose thresholds according to investigation capacity and business loss
6. document false-positive/false-negative trade-offs
7. validate temporal stability and data drift
8. keep feature engineering and inference contracts reproducible

The historical notebook itself was not promoted as a modern ML implementation because its stack and dataset assumptions are old. If fraud modelling becomes an active project, build a fresh reproducible case study around these principles.
