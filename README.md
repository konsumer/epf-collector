This is a simple tool for downloading [EPF](https://performance-partners.apple.com/epf) files (apple partner data) and putting it in a database.

```bash
# you need your credentials set
export EPF_USERNAME=your_username
export EPF_PASSWORD=your_password
```

There are 2 stages:

```bash
# STAGE 1

# collect current "update" (incremental)
# note: some days this is not available
./download update

# or

# collect current "full"
./download

# STAGE 2

# take current files and insert in duckdb
# this requires pbzip2 in your path (as well as duckdb)
./epf2csv | duckdb data/epf.duckdb


# full download & import & cleanup
./download && ./epf2csv | duckdb data/epf.duckdb && rm -rf data/csv
```

After you've got all your data in duck, you can see [examples.sql](examples.sql) for useful queries you can run.


`epf2csv` will just create CSV files, which you can use however you want. `epf2csv` doesn;t automatically import to duck, but it does output all the SQL on stdout, so youc an pipe it.

Here are some other things you can do:

```
# import all CSV, if you didn't pipe it from epf2csv
duckdb data/epf.duckdb < import.sql

# export all tables to niocely partitioned parquet
mkdir -p data/parquet
duckdb data/epf.duckdb < export_parquet.sql
```