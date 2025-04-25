This is a simple tool for downloading [EPF](https://performance-partners.apple.com/epf) files (apple partner data) and put it in a sqlite database.

```bash
# you need your credentials set
export EPF_USERNAME=your_username
export EPF_PASSWORD=your_password

# do this once to collect all the past data
# it's very big
npm run collect

# do this once a week to update using incremental data
npm run incremental

# do this to check all the files
# the files are big, so this is important to get right
npm run check

# this updates the database with all the data in your epdf dir (downloaded with collect/incremental)
npm run import
```
