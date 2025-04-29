##!/bin/sh

./epf2csv <  out/application_detail | duckdb out/epf_c.duckdb -c "CREATE TABLE application_detail AS SELECT * FROM read_csv('/dev/stdin', header=true)"
