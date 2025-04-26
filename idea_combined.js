import { Readable, Transform } from 'node:stream'
import { mkdir } from 'node:fs/promises'
import { pipeline } from 'node:stream/promises'
import parquet from 'parquetjs'

import progress from 'progress-stream'
import bz2 from 'unbzip2-stream'
import split from 'split2'
import * as cheerio from 'cheerio'

import { sqlTypesToParquetSchema, mapValues } from './type_manipulation.js'
import { green, red, yellow, blue } from './colors.js'

const { EPF_USERNAME, EPF_PASSWORD } = process.env

if (!EPF_USERNAME || !EPF_PASSWORD) {
  console.error('EPF_USERNAME & EPF_PASSWORD are required.')
  process.exit(1)
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
export const createRequestProgessStream = (response) => {
  const spinner = '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
  let t = 0
  const p = progress({
    length: parseInt(response.headers.get('content-length') || 0),
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

// receieves row and meta-info (from createEPFParserStream) and writes to file
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
          callback()
        })
      } else {
        writer.appendRow(row).then(() => callback())
      }
    }
  })
  s.on('finish', () => {
    writer.close()
  })
  return s
}

// simple debug-stream that just spits out JSON
function createDebugStream() {
  return new Transform({
    objectMode: true,
    transform(data, encoding, callback) {
      console.log(data)
      callback()
    }
  })
}

// test query
const url = 'https://feeds.itunes.apple.com/feeds/epf/v5/current/incremental/current/itunes20250425/application.tbz'
const options = {
  headers: {
    Authorization: `Basic ${btoa(`${EPF_USERNAME}:${EPF_PASSWORD}`)}`
  }
}

await mkdir('data/itunes', { recursive: true })

const web = await createFetchStream(url, options)
const prog = createRequestProgessStream(web.response)
const bunzip = bz2()
const splitter = split('\x02\n')
const parser = createEPFParserStream()
const debug = createDebugStream()
const parq = createParquetStream('data/itunes/application.parquet')

// await pipeline(web, prog, bunzip, splitter, parser, debug, process.stdout)
await pipeline(web, prog, bunzip, splitter, parser, parq)
