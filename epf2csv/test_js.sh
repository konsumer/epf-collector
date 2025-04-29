##!/bin/sh

node epf2csv.js <  out/application_detail | duckdb out/epf_js.duckdb -c "CREATE TABLE application_detail AS SELECT * FROM read_csv('/dev/stdin', header=true)"
