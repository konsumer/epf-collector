This is a very simple program that can be used in conjunction with other unix-tools to parse an EPF file

## C

```bash
# build
clang -o epf2csv epf2csv.c

# extract EPF file
mkdir -p out
cd out
tar --strip-components=1 -xjf ../../data/epf/full/1745737200/itunes/application_detail.tbz

# make a gzipped CSV, then import that into duck
../epf2csv <  application_detail | gzip > application_detail.csv.gz
duckdb epf.duckdb -c "CREATE TABLE application_detail AS SELECT * FROM read_csv('application_detail.csv.gz', header=true)"

# or direct to duck
../epf2csv <  application_detail | duckdb epf.duckdb -c "CREATE TABLE application_detail AS SELECT * FROM read_csv('/dev/stdin', header=true)"
```

## JS

```bash
# extract EPF file
mkdir -p out
cd out
tar --strip-components=1 -xjf ../../data/epf/full/1745737200/itunes/application_detail.tbz

# make a gzipped CSV, then import that into duck
node epf2csv.js <  application_detail | gzip > application_detail.csv.gz
duckdb epf.duckdb -c "CREATE TABLE application_detail AS SELECT * FROM read_csv('application_detail.csv.gz', header=true)"

# or direct to duck
node epf2csv.js <  application_detail | duckdb epf.duckdb -c "CREATE TABLE application_detail AS SELECT * FROM read_csv('/dev/stdin', header=true)"
```

## performance

Surprisingly, the javascript-implementation seems to work just as well, and finish almost 4X faster on my system.

```bash
time ./test_c.sh ; ./test_js.sh

./test_c.sh  315.66s user 11.88s system 115% cpu 4:44.71 total
./test_js.sh  112.51s user 15.17s system 146% cpu 1:27.38 total


du -h 	out/epf_*.duckdb

10G	out/epf_c.duckdb
10G	out/epf_js.duckdb
```
