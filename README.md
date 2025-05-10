This is a simple tool for downloading [EPF](https://performance-partners.apple.com/epf) files (apple partner data) and putting it in a database.

epf2csv uses [pbzip2](https://github.com/ruanhuabin/pbzip2). It's possible to use it without (see source) but it's much faster than anything else that can uncompress bz2 files.

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
./epf2csv | duckdb data/epf.ddb


# full download & import & cleanup
./download && ./epf2csv | duckdb data/epf.ddb && rm -rf data/csv
```

After you've got all your data in duck, you can see [examples.sql](notes/examples.sql) for useful queries you can run.

`epf2csv` will just create CSV files, which you can use however you want. `epf2csv` doesn't automatically import to duck, but it does output all the SQL on stdout, so you can pipe it.

Here are some other things you can do:

```bash
# import all CSV, if you didn't pipe it from epf2csv
duckdb data/epf.ddb < notes/import.sql

# export all tables to partitioned parquet
mkdir -p data/parquet
duckdb data/epf.ddb < notes/export_parquet.sql
```
