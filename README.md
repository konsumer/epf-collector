This is a simple tool for downloading [EPF](https://performance-partners.apple.com/epf) files (apple partner data) and putting it in a parquet database.

```bash
# you need your credentials set
export EPF_USERNAME=your_username
export EPF_PASSWORD=your_password
```

```bash
# Collect all the current EPF data:
# do this once to collect all the past data, initially
# it's very big
npm run setup

# Update the current EPF data
# do this once a week to update using incremental data
npm start

# you can also collect only part of the EPF in your database
./collect.js update data/parquet itunes match popularity pricing
```

You can do just parts of the process (using interim-files) if you want:

```bash
# Get all files
./files_collect.js full
./files_collect.js full data/epf itunes match popularity pricing

# Get update files
./files_collect.js
./files_collect.js update data/epf itunes match popularity pricing

# check all the files with md5-files
./files_check.js
./files_check.js data/epf/**/*.tbz

# this updates the database with all the data in your epf dir (downloaded with collect/incremental)
./files_import.js
./files_import.js data/parquet data/epf/**/*.tbz

# get info about the EPF files, on-disk
./files_info.js
./files_info.js data/epf/**/*.tbz
```

## using the database

Once you have collected all your data as parquet files, you will need some way to read them. I like to use [duckdb](https://duckdb.org):

```
duckdb

SELECT * FROM read_parquet('data/parquet/pricing/*/application_price.parq');
```

Read more about it [here](https://duckdb.org/docs/stable/data/parquet/overview.html).
