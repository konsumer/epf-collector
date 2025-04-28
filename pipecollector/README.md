This is a C++-based parser that will convert EPF data to parquet file.

You can build like this:

```bash
g++ -o epftoparquet epftoparquet.cpp $(pkg-config --cflags --libs arrow parquet) -std=c++17
```

Use it like this:

```bash
mkdir -p out
cd out

# first collect the EPF file
curl -O -u "${EPF_USERNAME}:${EPF_PASSWORD}" https://feeds.itunes.apple.com/feeds/epf/v5/current/itunes20250427/application_detail.tbz
curl -O -u "${EPF_USERNAME}:${EPF_PASSWORD}" https://feeds.itunes.apple.com/feeds/epf/v5/current/itunes20250427/application_detail.tbz.md5

# verify
md5sum -c application_detail.tbz.md5

# extract
tar --strip-components=1 -xjf application_detail.tbz

# parse
../epftoparquet application_detail application_detail.parquet < application_detail


# OR extract & parse
tar -Oxjf application_detail.tbz '*/application_detail' | ../epftoparquet application_detail application_detail.parquet
```
