#!/usr/bin/env node

import { glob } from 'glob'
import { epfToParquet, exists  } from './epf.js'

let [,,  outputDir = 'data/parquet', ...inFiles ] = process.argv

if (!inFiles.length) {
  inFiles = await glob(`data/epf/**/*.tbz`)
}

for (const file of inFiles) {
  const r = new RegExp(`/(update|full)/([a-z]+)([0-9]{4})([0-9]{2})([0-9]{2})/([a-z_]+)\\.tbz`)
  const [,type, group, year, month, day, table] = r.exec(file)
  const outFile = `${outputDir}/import_type=${type}/import_group=${group}/import_date=${new Date(`${year}-${month}-${day}`).getTime()/1000}/${table}.parq`
  const info = { type, group, table, date: [parseInt(year),parseInt(month), parseInt(day) ], outFile }

  if (await exists(outFile)) {
    console.log('\x1b[32mskipping\x1b[0m', info)
  } else {
    console.log('\x1b[32mstarting\x1b[0m', info)
    await epfToParquet(file, outFile)
      .then(result =>  console.log('\x1b[32mfinished\x1b[0m:', result.rowCount))
      .catch(err => console.error(`\x1b[41mFailed to process ${file}:\x1b[0m`, err))
  }
}
