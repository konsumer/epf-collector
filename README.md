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
./download

# or

# collect current "full"
./download full


# STAGE 2

# take current files and insert in duckdb
./import data/epf/full/**/*.tbz
```
