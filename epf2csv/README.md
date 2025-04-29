This is a very simple C program that can be used in conjunction with other unix-tools to parse an EPF file

```bash
# build
clang -o epf2csv epf2csv.c

# extract EPF file
mkdir -p out
cd out
tar --strip-components=1 -xjf ../../data/epf/full/1745737200/itunes/application_detail.tbz

# make a gzipped CSV
../epf2csv <  application_detail | gzip > application_detail.csv.gz

# or direct to duck
../epf2csv <  application_detail | duckdb epf.duckdb -c "CREATE TABLE application_detail AS SELECT * FROM read_csv('/dev/stdin')"

```
