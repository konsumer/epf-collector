#!/usr/bin/env node

import { stat } from 'node:fs/promises'
import { glob } from 'glob'
import { epfToParquet  } from './epf.js'

let [,,  outputDir = 'data/parquet', ...inFiles ] = process.argv

if (!inFiles.length) {
  inFiles = await glob(`data/epf/**/*.tbz`)
}

for (const file of inFiles) {
  const r = new RegExp(`/(update|full)/([a-z]+)([0-9]{4})([0-9]{2})([0-9]{2})/([a-z_]+)\\.tbz`)
  const [,type, group, year, month, day, table] = r.exec(file)
  const outFile = `${outputDir}/import_type=${type}/import_group=${group}/import_date=${new Date(`${year}-${month}-${day}`).getTime()/1000}/${table}.parq`
  const info = { type, group, table, date: [parseInt(year),parseInt(month), parseInt(day) ], outFile }

  try {
    await stat(outFile)
    console.log('skipping', info)
  } catch (e) {
    console.log('starting', info)
    await epfToParquet(file, outFile)
      .then(result =>  console.log('finished', result))
      .catch(err => console.error(`Failed to process ${file}:`, err))
  }
}
