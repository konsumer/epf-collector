import { createEpfParseStream } from './epf.js'
import { glob } from 'glob'
import Database from 'better-sqlite3'
import { dirname } from 'node:path'
import { mkdir } from 'node:fs/promises'

let [, , dbFile = 'data/epf.sqlite3', ...epfs] = process.argv

if (!epfs.length) {
  epfs = await glob('data/epf/**/*.tbz')
}

await mkdir(dirname(dbFile), { recursive: true })
const db = new Database(dbFile)

const prepared = {}

// generate a db table from info
function makeTable(info) {
  const fields = info.dbNames.map((k, i) => `${k} ${info.dbTypes[i]}`).join(', ')

  // remove primaryKey fields that are not in fields (apple does this!)
  for (const ps of info.primaryKey) {
    if (!info.dbNames.includes(ps)) {
      info.primaryKey = info.primaryKey.filter((k) => k !== ps)
    }
  }

  const p = info.primaryKey?.length ? `, UNIQUE(${info.primaryKey.join(', ')}) ON CONFLICT REPLACE` : ''
  db.exec(`CREATE TABLE IF NOT EXISTS ${info.table} (${fields}${p})`)

  prepared[info.table] = db.prepare(`INSERT INTO ${info.table} VALUES (${info.dbNames.map((k) => '?').join(', ')})`)
}

function processField(value, type) {
  if (type.startsWith('DECIMAL') || type === 'INTEGER') return Number(value)
  if (type === 'BIGINT') return BigInt(value)
  if (type === 'DATETIME') {
    if (!value) return null
    return new Date(value).toISOString()
  }
  return value
}

// save a record to db
function saveRecord(data, info) {
  const row = []
  for (const k in data) {
    row.push(processField(data[k], info.dbTypes[k], info.dbNames[k]))
  }
  const { table } = info
  console.log(table, row.join(','))
  prepared[table].run(...row)
}

for (const f of epfs) {
  let info
  createEpfParseStream(f)
    .on('info', (i) => {
      makeTable(i)
      info = i
    })
    .on('record', (r) => {
      saveRecord(r, info)
    })
}
