# Africa health-data analysis — legacy learning case study

Curated from `ga-data`, an older General Assembly data-course/practice repository.

The original work explored Sub-Saharan African indicators using Pandas, World Bank data, plotting, clustering and regression. Much of the code is tied to old notebook-era APIs (`pandas.ix`, `pandas.tools`, old scikit-learn `cross_validation`, notebook magics), so it is preserved here as an engineering lesson rather than copied as executable 2026 code.

## Durable pipeline

1. acquire indicator data from a documented source such as the World Bank API
2. normalise country/indicator/year dimensions
3. define missing-value policy explicitly
4. validate indicator codes and time ranges
5. separate exploratory analysis from modelling
6. split train/test data before fitting models
7. record assumptions and evaluation metrics
8. keep visualisation downstream of a clean analytical dataset

## Modernisation notes

- use `sklearn.model_selection.train_test_split`
- use current Pandas/Polars indexing APIs
- avoid notebook shell/magic commands in reusable modules
- prefer tidy/long-form data over positional column slicing
- add schema/data-quality checks before statistical modelling

The original notebook checkpoint and course files were intentionally not migrated.
