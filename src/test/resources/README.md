# Test Data

This test data is derived from the [LINCS dataset](http://www.lincsproject.org/). We have transformed the data in the form we use internally on proprietary L1000 data.

Note:
- A set of features of samples and compounds are not available for LINCS data,
  so we substituted with dummy data or placeholders.
- No known target information is available for the compounds in the LINCS
  dataset, so we inserted some dummy entries
- No p-values are available for the expression values (or derived stats), so
  we just put `p = 0.0` everywhere.
- We have only used the first 100 rows of the `t` and `p` database.

