import { epfStream } from './epf.js'
import { glob } from 'glob'
import Database from 'better-sqlite3'

let [, , dbFile = 'data/epf.sqlite3', ...epfs] = process.argv

if (!epfs.length) {
  epfs = await glob('data/epf/**/*.tbz')
}

const db = new Database(dbFile)

const prepared = {}

// generate a db table from info
function makeTable(info) {
  const fields = Object.keys(info.db)
    .map((k) => `${k} ${info.db[k]}`)
    .join(', ')

  const p = info.primaryKey?.length ? `, PRIMARY KEY (${info.primaryKey.join(', ')}), UNIQUE(${info.primaryKey.join(', ')}) ON CONFLICT REPLACE` : ''
  db.exec(`CREATE TABLE IF NOT EXISTS ${info.table} (${fields}${p})`)
  console.log(`CREATE TABLE IF NOT EXISTS ${info.table} (${fields}${p})`)

  // setup prepared inserts
  prepared[info.table] = db.prepare(`INSERT INTO ${info.table} VALUES (${Object.keys(info.db).map((k) => '?')})`)
}

function processField(value, type) {
  if (type.startsWith('DECIMAL') || type === 'INTEGER') return Number(value)
  if (type === 'BIGINT') return BigInt(value)
  if (type === 'DATETIME') {
    if (!value) return null
    return new Date(value)
  }
  return value
}

// save a record to db
function saveRecord(data, { table, db }) {
  const row = []
  for (const k in data) {
    row.push(processField(data[k], db[k]))
  }
  console.log(table, row.join(','))
  prepared[table].run(...row)
}

for (const f of epfs) {
  // 1st pass: build table
  const info = await epfStream(f)
  await makeTable(info)

  // 2nd pass: save records
  await epfStream(f, saveRecord)
}
