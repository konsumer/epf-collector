import { createReadStream, createWriteStream } from 'node:fs'
import { mkdir, stat, unlink, readFile, writeFile } from 'node:fs/promises'
import { Readable, Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { dirname, basename } from 'node:path'
import { createHash } from 'node:crypto'
import parquet from 'parquetjs'

// it's depracated, but the callbacks work better with streams
import duckdb from 'duckdb'

import progress from 'progress-stream'
import bz2 from 'unbzip2-stream'
import split from 'split2'
import * as cheerio from 'cheerio'

import { sqlTypesToParquetSchema, mapValues } from './type_manipulation.js'
import { green, red, yellow } from './colors.js'

const { EPF_USERNAME, EPF_PASSWORD } = process.env

if (!EPF_USERNAME || !EPF_PASSWORD) {
  console.error('EPF_USERNAME & EPF_PASSWORD are required.')
  process.exit(1)
}

// check if a file exists
export const exists = async (f) => {
  try {
    await stat(f)
    return true
  } catch (e) {
    return false
  }
}

// check a single file with md5 file next to it
export async function md5check(filePath) {
  const check = (await readFile(`${filePath}.md5`, 'utf8')).split(' = ').pop().trim()
  const hash = createHash('md5')
  for await (const chunk of createReadStream(filePath)) {
    hash.update(chunk)
  }
  return hash.digest('hex') === check
}

// create a node stream from fetch
export async function createFetchStream(url, options = {}) {
  const response = await fetch(url, options)
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`)
  }
  if (!response.body) {
    throw new Error('Response body is empty')
  }
  const r = response.body instanceof ReadableStream ? Readable.fromWeb(response.body) : Readable.from(response.body)
  r.response = response
  return r
}

// get a stream to show fetch/stream progress
export const createRequestProgessStream = (length) => {
  const spinner = '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
  let t = 0
  const p = progress({
    length,
    time: 500
  })
  p.on('progress', ({ percentage, transferred, length, remaining, eta, runtime, delta, speed }) => {
    let bar = ''
    for (let i = 0; i < 50; i++) {
      bar += i > percentage / 2 ? '░' : '█'
    }
    process.stdout.cursorTo(0)
    process.stdout.clearLine(0)
    process.stdout.write(`${bar} ${yellow(spinner[t++ % spinner.length])} ${percentage.toFixed(2)}% (${runtime}/${runtime + eta}s)`)
  })
  return p
}

// get list of current EPF files for update/full
export async function getEPFList(type = 'update') {
  const listStartUrl = type === 'update' ? '/incremental/current/' : '/'

  const t = await fetch(`https://feeds.itunes.apple.com/feeds/epf/v5/current${listStartUrl}`, {
    headers: {
      Authorization: `Basic ${btoa(`${EPF_USERNAME}:${EPF_PASSWORD}`)}`
    }
  }).then((r) => r.text())

  const out = []
  const $ = cheerio.load(t)
  for (const a of $('a')) {
    if (!a.attribs.href.startsWith('?') && a.attribs.href !== 'incremental/') {
      const gu = `https://feeds.itunes.apple.com/feeds/epf/v5/current${listStartUrl}${a.attribs.href}`
      const t = await fetch(gu, {
        headers: {
          Authorization: `Basic ${btoa(`${EPF_USERNAME}:${EPF_PASSWORD}`)}`
        }
      }).then((r) => r.text())
      const $ = cheerio.load(t)
      for (const a of $('a')) {
        if (!a.attribs.href.startsWith('?') && !a.attribs.href.endsWith('.md5')) {
          out.push(`${gu.replace('https://feeds.itunes.apple.com/feeds/epf/v5/current', '')}${a.attribs.href}`)
        }
      }
    }
  }
  return out
}

// turns line-split messages into messages with header/data for each row
function createEPFParserStream() {
  const meta = {}
  let dbFields
  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      const line = chunk.toString()
      if (!dbFields) {
        dbFields = line.split('#').pop().split('\x01')
        callback()
      } else if (line.startsWith('#')) {
        let [key, data] = line.slice(1).split(':')
        if (key !== '#legal') {
          if (['dbTypes'].includes(key)) {
            const dbTypes = line.split('#').pop().split('\x01')
            meta.types = dbFields.reduce((a, c, i) => ({ ...a, [c]: dbTypes[i].split(':').pop() }), {})
          } else if (['primaryKey'].includes(key)) {
            meta[key] = data.split('\x01')
          } else {
            meta[key] = data.replace(/[\x02\0x00]/g, '').trim()
          }
        }
        callback()
      } else {
        callback(null, { ...meta, row: mapValues(line.split('\x01'), meta.types) })
      }
    }
  })
}

// receives row and meta-info (from createEPFParserStream) and writes to file
function createParquetStream(filename) {
  let writer
  const s = new Transform({
    objectMode: true,
    transform(data, encoding, callback) {
      const { row, types, primaryKey, ...meta } = data
      if (!writer) {
        const s = sqlTypesToParquetSchema(types, 'parquetjs')
        for (const k of Object.keys(s)) {
          if (!primaryKey.includes(k)) {
            s[k].optional = true
          }
        }
        parquet.ParquetWriter.openFile(new parquet.ParquetSchema(s), filename).then((w) => {
          writer = w
          if (row) {
            writer
              .appendRow(row)
              .then(() => callback())
              .catch((e) => callback(e))
          }
        })
      } else {
        writer
          .appendRow(row)
          .then(() => callback())
          .catch((e) => callback(e))
      }
    }
  })
  s.on('finish', () => {
    writer.close()
  })
  return s
}

// simple debug-stream that just spits out log of incoming data
function createDebugStream() {
  return new Transform({
    objectMode: true,
    transform(data, encoding, callback) {
      console.log(data)
      callback()
    }
  })
}

// wrapper to import an EPF file (show progress, parse, output parquet)
// I will need to stream-to-disk first (for resume and less errors on full imports) so this is more for an example
export async function getEpfFileAsParquet(u, outFilename) {
  const options = {
    headers: {
      Authorization: `Basic ${btoa(`${EPF_USERNAME}:${EPF_PASSWORD}`)}`
    }
  }
  await mkdir(dirname(outFilename), { recursive: true })
  const web = await createFetchStream(`https://feeds.itunes.apple.com/feeds/epf/v5/current${u}`, options)
  const prog = createRequestProgessStream(parseInt(web.response.headers.get('content-length')))
  const bunzip = bz2()
  const splitter = split('\x02\n')
  const parser = createEPFParserStream()
  const parq = createParquetStream(outFilename)

  await pipeline(web, prog, bunzip, splitter, parser, parq)
}

// this will download an EPF file with progress
export async function getEpfFileAsLocal(u, outFilename, getMd5) {
  const options = {
    headers: {
      Authorization: `Basic ${btoa(`${EPF_USERNAME}:${EPF_PASSWORD}`)}`
    }
  }
  await mkdir(dirname(outFilename), { recursive: true })
  const web = await createFetchStream(`https://feeds.itunes.apple.com/feeds/epf/v5/current${u}`, options)
  const prog = createRequestProgessStream(parseInt(web.response.headers.get('content-length')))
  const out = createWriteStream(outFilename)

  if (getMd5) {
    const d = await fetch(`https://feeds.itunes.apple.com/feeds/epf/v5/current${u}.md5`, options).then((r) => r.text())
    await writeFile(`${outFilename}.md5`, d)
  }

  await pipeline(web, prog, out)

  if (getMd5) {
    return await md5check(outFilename)
  }

  return undefined
}

// creates a stream that receives records/structure (from createEPFParserStream) and imports to database
async function createDuckStream(tableName, outFileName = ':memory:') {
  let db
  let stmt

  // console.log({ tableName, outFileName  })

  const insertRecord = (chunk, cb) => {
    if (!stmt) {
      return cb('Statement not setup.')
    }
    if (chunk.row) {
      // console.log(`INSERT: ${Object.values(chunk.row).join(', ')}`)
      stmt.run(...Object.values(chunk.row), cb)
    } else {
      cb()
    }
  }

  const ducker = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      if (!stmt) {
        let p = ''
        if (chunk?.primaryKey?.length) {
          p = `, UNIQUE(${chunk.primaryKey.join(', ')})`
        }
        const sqlCreate = `CREATE TABLE IF NOT EXISTS ${tableName} (${Object.keys(chunk.types)
          .map((k) => `${k} ${chunk.types[k]}`)
          .join(', ')}${p});`
        const sqlInsert = `INSERT OR REPLACE INTO ${tableName} VALUES (${Object.keys(chunk.types)
          .map(() => '?')
          .join(', ')});`

        db.exec(sqlCreate, (e1) => {
          if (e1) {
            return callback(e1)
          }
          stmt = db.prepare(sqlInsert)
          insertRecord(chunk, callback)
        })
      } else {
        insertRecord(chunk, callback)
      }
    }
  })

  ducker.on('finish', () => {
    if (stmt) {
      stmt.finalize()
    }
    if (db) {
      db.close()
    }
  })

  return new Promise((resolve, reject) => {
    db = new duckdb.Database(outFileName, (err) => {
      if (err) {
        return reject(err)
      } else {
        resolve(ducker)
      }
    })
  })
}

// given a tbz file, this will UPSERT all records
export async function duckImportFile(file, outFileName = ':memory:') {
  const src = createReadStream(file)
  const bunzip = bz2()
  const splitter = split('\x02\n')
  const parser = createEPFParserStream()
  const ducker = await createDuckStream(basename(file, '.tbz'), outFileName)

  return await pipeline(src, bunzip, splitter, parser, ducker)
}

// example update
const type = 'update'
const skipTables = ['video_price']
const dbFile='data/epf.duckdb'

// this tracks the urls that were downloaded on 1st pass
const filesforInsert = []

const rInfo = /([a-z_]+)([0-9]{4})([0-9]{2})([0-9]{2})\/([a-z_]+)\.tbz/
for (const u of await getEPFList(type)) {
  let [m, collection, dateY, dateM, dateD, table] = rInfo.exec(u)
  const date = new Date(dateY, dateM - 1, dateD)
  const outFile = `data/epf/${type}/${date.getTime() / 1000}/${collection}/${table}.tbz`

  if (skipTables.includes(table)) {
    process.stdout.write(yellow('skipping') + ' (from skip-tables) ')
    if (await exists(outFile)) {
      process.stdout.write(green('& deleting '))
      await unlink(outFile)
    }
    process.stdout.write(outFile + '\n')
    continue
  }

  // I do this after skipped, so I only import the current dump
  filesforInsert.push(u)

  if (await exists(outFile)) {
    console.log(yellow('skipping'), '(exists)', outFile)
    const check = await md5check(outFile)
    if (check !== true) {
      throw new Error('MD5 check failed')
    } else {
      console.log(green('verified'), outFile)
    }
  } else {
    console.log(green('\ndownloading\n'), outFile)
    try {
      const check = await getEpfFileAsLocal(u, outFile, true)
      if (check !== true) {
        throw new Error('MD5 check failed')
      } else {
        console.log(green('\nverified'), outFile)
      }
    } catch (e) {
      // No partial imports
      // TODO: I get a lot of "Terminated" errors on full + getEpfFileAsParquet, so I download, then parse in another step
      // TODO: it'd be cool if it kept a bin-log with partial download-support
      // TODO: it'd be cool if it MD5 verified from apple
      console.error(red('\nERROR'), '', outFile, ' :', e.message)
      await unlink(outFile)
    }
  }
}

// 2nd pass: import existing files to database
for (const u of filesforInsert) {
  let [m, collection, dateY, dateM, dateD, table] = rInfo.exec(u)
  const date = new Date(dateY, dateM - 1, dateD)
  const outFile = `data/epf/${type}/${date.getTime() / 1000}/${collection}/${table}.tbz`

  console.log(green('importing'), outFile)
  await duckImportFile(outFile, 'data/epf.duckdb')
}
