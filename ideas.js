/*

This has a few ideas:

- stream directly into duck
- use pbz2 in sub-process stream
- EpfStream parses header, and then outputs CSV
*/

import { spawn } from 'node:child_process'
import { Transform, Readable, Writable } from 'node:stream'
import { promisify } from 'node:util'
import { pipeline } from 'node:stream/promises'
import { createWriteStream } from 'node:fs'
import duckdb from 'duckdb'
import createSplitter from 'split2'

// fast bzip2 stream (requires pbzip2 in path)
function createBzStream(file) {
  const s = spawn('pbzip2', ['-cd', file])
  return s.stdout
}

// get info from EPF header
async function getInfo(file) {
  const bz = createBzStream(file)
  let info = {}
  // just grabs first chunk and parses header, then quits
  const epf = new Transform({
    transform(chunk, encoding, callback) {
      const lines = chunk.toString().split('\x02\n')
      const [tarHeader, fieldsS] = lines[0].split('#')
      info.dbNames = fieldsS.split('\x01')
      const r = /^([a-z]+)([0-9]{4})([0-9]{2})([0-9]{2})\/([a-z_]+)/
      const [, group, year, month, day, name] = r.exec(tarHeader)
      info.group = group
      info.name = name
      info.date = new Date(year, month - 1, day)
      for (const line of lines.slice(1)) {
        if (line.startsWith('#')) {
          const [k, v] = line.substr(1).split(':')
          if (['primaryKey', 'dbTypes'].includes(k)) {
            info[k] = v.split('\x01')
          }
          if (['exportMode'].includes(k)) {
            info[k] = v
          }
        }
      }
      info.primaryKeys = info.primaryKey.filter((k) => info.dbNames.includes(k))
      info.types = info.dbNames.reduce((a, c, i) => ({ ...a, [c]: info.dbTypes[i] }), {})
      delete info.primaryKey
      delete info.dbNames
      delete info.dbTypes

      callback('done')
    }
  })
  // is there a better way to close immediately when I'm done?
  try {
    await pipeline(bz, epf)
  } catch (e) {}
  return info
}

// output SQL to create table
function getCreateSql(table, types, primaryKeys) {
  const fields = []
  for (const [field, type] of Object.entries(types)) {
    fields.push(`${field} ${type.replace('LONGTEXT', 'TEXT')}`)
  }
  if (primaryKeys?.length) {
    fields.push(`PRIMARY KEY(${primaryKeys.join(', ')})`)
  }
  return `CREATE OR REPLACE TABLE ${table} (${fields.join(', ')});`
}

async function createDuckStream(conn, info, options = "AUTO_DETECT TRUE, HEADER FALSE, DELIMITER '\x01'") {
  const dbExec = promisify(conn.exec.bind(conn))
  const dbPrepare = promisify(conn.prepare.bind(conn))
  await dbExec(getCreateSql(info.name, info.types, info.primaryKeys))
  const duckdbProcess = await dbPrepare(`COPY ${info.name} FROM stdin (${options})`)

  return duckdbProcess.stdin()
}

// very simple format: delimiter is \1, newlines are escaped
// SELECT * FROM read_csv('data/test.csv', ignore_errors=true,header=false);
async function epf2csv(epfFile, outFile) {
  const bz = createBzStream(epfFile) // Your bzip2 stream creator
  const split = createSplitter('\x02\n') // Your custom splitter
  let linenum = 0

  const converter = new Transform({
    transform(chunk, encoding, callback) {
      linenum++
      const line = chunk.toString('utf8')
      if (line.startsWith('#') || linenum === 1) {
        return callback()
      }
      const csvLine = line.replace(/,/g, '\\,').replace(/\x01/g, ',').replace(/\n/g, '\\n') + '\n' // Add back the newline for row separation
      callback(null, csvLine)
    }
  })

  const writer = createWriteStream(outFile)
  await pipeline(bz, split, converter, writer)
}

await epf2csv('data/epf/full/1745737200/itunes/application.tbz', 'data/test.csv')
