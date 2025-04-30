// I am trying to work out parsing issues with data in application/application_detail

import { readFile } from 'node:fs/promises'
import { glob } from 'glob'
import chalk from 'chalk'
import * as tables from './tables.js'

let [, , ...inputFiles] = process.argv

if (!inputFiles?.length) {
  inputFiles = await glob('data/epf/**/*.issues')
}

// turn 2 arrays (keys, values) into an object
const obzip = (keys, values) => keys.reduce((a, k, i) => ({ ...a, [k]: values[i] }), {})

// process 1 EPF line: output an object with all values encoded as javascript strings
function parseRow(line, { info: { group, name, date }, fields, headers}) {
  let data = line.split('\u0001')

  if (data.length !== headers.length) {
    const fixed = {}
    let i = 0
    for (const [field, type] of Object.entries(fields)) {
      if (type === 'TEXT' || type === 'LONGTEXT') {
        // TODO: find other fields
        break
      } else {
        // fill in fields before problematic fields
        fixed[field] = data[i]
      }
      i++
    }
    console.error(chalk.red('mismatch'), 'got', data.length, 'expected', headers.length, fixed)
    return fixed
  }

  return obzip(headers, data)
}

for (const file of inputFiles) {
  const t = await readFile(file, 'utf8')
  const [header, ...lines] = t.split('\u0002\n')
  const [junk, headersT] = header.split('#')
  const headers = headersT.split('\x01')
  // pull lots of info from tar-header
  const r = /^([a-z]+)([0-9]{4})([0-9]{2})([0-9]{2})\/([a-z_]+)/
  const [, group, year, month, day, name] = r.exec(junk)
  const info = { group, name, date: new Date(year, month - 1, day) }
  const fields = tables[name]

  for (const line of lines) {
    console.log(parseRow(line, {info, headers, fields}))
  }
}
