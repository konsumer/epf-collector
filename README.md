This is a simple tool for downloading [EPF](https://performance-partners.apple.com/epf) files (apple partner data) and putting it in a database.

```bash
# you need your credentials set
export EPF_USERNAME=your_username
export EPF_PASSWORD=your_password
```

There are 2 stages:

- `./download` Download and check current EPF files
- `./import` import EPF downloaded files into database

Run each with `--help` for more information.
