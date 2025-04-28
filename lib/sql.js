// utils for interacting with sqlite

import { createReadStream } from 'node:fs'
import { Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import Database from 'better-sqlite3'
import createBunzipStream from 'unbzip2-stream'
import createSplitStream from 'split2'

import { tables } from './schema.js'
import { createEPFParserStream } from './parser.js'

// run a prepared sql statement on each row of data
export function createSqlStream(stmt) {
  return new Transform({
    objectMode: true,
    transform({ meta, row }, encoding, callback) {
      stmt.run(...Object.values(row))
      callback()
    }
  })
}

export async function importEPF(sqlifile, table, epfFile) {
  const db = new Database(sqlifile, {})
  db.pragma('journal_mode = WAL')

  let p = ''
  if (tables[table].keys?.length) {
    p = `, PRIMARY KEY (${tables[table].keys.join(', ')})`
  }

  let q = `CREATE TABLE IF NOT EXISTS ${table} (${Object.keys(tables[table].types)
    .map((key) => `${key} ${tables[table].types[key].sql}`)
    .join(', ')}${p});`
  db.exec(q)

  q = `INSERT OR REPLACE INTO ${table} VALUES (${Object.keys(tables[table].types)
    .map(() => '?')
    .join(', ')})`

  // performance increase from starting transaction before prepare
  db.exec(`BEGIN TRANSACTION;`)
  const stmt = db.prepare(q)

  const src = createReadStream(epfFile)
  const bunzip = createBunzipStream()
  const splitter = createSplitStream('\x02\n')
  const parser = createEPFParserStream(table)
  const sql = createSqlStream(stmt)

  try {
    await pipeline(src, bunzip, splitter, parser, sql)
    db.exec(`COMMIT;`)
  } catch (err) {
    db.exec(`ROLLBACK;`)
    throw err
  }
}
