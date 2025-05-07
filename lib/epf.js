// utils for parsing EPF stream

import { Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { spawn } from 'node:child_process'
import { createWriteStream } from 'node:fs'

import { format as createCsvStream } from '@fast-csv/format'
import createSplitter from 'split2'

// fast bzip2 stream (requires pbzip2 in path)
// inMem will do the operation in memory (a bit faster, at cost of memory)
export function createBzStream(file, inMem = true) {
  const options = ['-cd']
  if (inMem) {
    options.push('-r')
  }
  const s = spawn('pbzip2', [...options, file])
  return s.stdout
}

// get info from EPF header
// not using here, but eventually this will setup sql
export async function getInfo(file) {
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
export function getCreateSql(info) {
  const { name, types, primaryKeys } = info
  const fields = []
  for (const [field, type] of Object.entries(types)) {
    fields.push(`${field} ${type.replace('LONGTEXT', 'TEXT')}`)
  }
  if (primaryKeys?.length) {
    fields.push(`PRIMARY KEY(${primaryKeys.join(', ')})`)
  }
  return `CREATE OR REPLACE TABLE ${name} (${fields.join(', ')});`
}

// outputs format for duck's read_csv (so you don;t have to create a separate table)
export function getSqlColumns(info) {
  const fields = []
  for (const [field, type] of Object.entries(info.types)) {
    fields.push(`'${field}': '${type.replace('LONGTEXT', 'TEXT')}'`)
  }
  return fields.join(', ')
}

// very simple format: delimiter is \1, newlines are escaped
export async function epf2csv(info, epfFile, outFile) {
  const headers = Object.keys(info.types)

  const bz = createBzStream(epfFile)
  const split = createSplitter('\x02\n')
  const csv = createCsvStream()

  let linenum = 0
  const skipped = []

  const converter = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      linenum++
      const line = chunk.toString('utf8')
      if (line.startsWith('#') || linenum === 1) {
        return callback()
      }

      const row = line.split('\x01')
      if (row.length !== headers.length) {
        // skip bad lines
        skipped.push(linenum)
        return callback()
      }

      callback(null, row)
    }
  })
  const writer = createWriteStream(outFile)
  await pipeline(bz, split, converter, csv, writer)

  return { skipped, linenum }
}
