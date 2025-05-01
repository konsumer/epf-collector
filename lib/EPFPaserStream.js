// This is a transform stream that will take EPF lines (buffers split by '\x02\n') and output objects
//
// usage:
// import createEPFParser from './EPFPaserStream.js'
// import createByteSplitter from './ByteSplitterStream.js'
// import { pipeline } from 'node:stream/promises'
//
// const splitter = createByteSplitter('\x02\n')
// const parser = createEPFParser()
// await pipeline(reader, splitter, parser, writer)

import { Transform } from 'node:stream'

// turn 2 arrays (keys, values) into an object
export const obzip = (keys, values) => keys.reduce((a, k, i) => ({ ...a, [k]: values[i] }), {})

// binary-only buffer-split
export function splitBuffer(buffer, delimiter) {
  if (typeof delimiter === 'number') {
    delimiter = Buffer.from([delimiter])
  } else if (typeof delimiter === 'string') {
    delimiter = Buffer.from(delimiter)
  }
  const result = []
  let startIndex = 0
  for (let i = 0; i < buffer.length; i++) {
    if (buffer[i] === delimiter[0]) {
      let found = true
      for (let j = 0; j < delimiter.length; j++) {
        if (buffer[i + j] !== delimiter[j]) {
          found = false
          break
        }
      }
      if (found) {
        result.push(buffer.slice(startIndex, i))
        i += delimiter.length - 1 // Skip the delimiter
        startIndex = i + 1
      }
    }
  }
  result.push(buffer.slice(startIndex))
  return result
}

// parse a single row (Buffer) with lots of info
export function parseRow(chunk, { info: { name, group, date }, currentLine, fields, headers, primaryKeys, exportMode }) {
  let data = chunk.toString().split('\x01')
  if (data.length !== headers.length) {
    data = chunk
      .toString()
      .replace(/\x01\x01/g, '\x01')
      .split('\x01')
    if (data.length !== headers.length) {
      return undefined
    }
  }
  return obzip(
    headers,
    data.map((b) => b.toString('utf8'))
  )
}

// this will create Objects from a stream of line-buffers
export class EPFPaserStream extends Transform {
  constructor(options = {}) {
    super({ objectMode: true, ...options })
    this.currentLine = -1
  }

  _transform(chunk, encoding, callback) {
    this.currentLine++

    if (this.currentLine === 0) {
      // first line is header
      const [junk, headersT] = chunk.toString().split('#')
      this.headers = headersT.split('\x01')
      // pull lots of info from tar-header
      const r = /^([a-z]+)([0-9]{4})([0-9]{2})([0-9]{2})\/([a-z_]+)/
      const [, group, year, month, day, name] = r.exec(junk)
      this.info = { group, name, date: new Date(year, month - 1, day) }
      this.emit('info', ['header', this.info])
    } else if (chunk[0] === 35) {
      // # comment

      let [k, v] = chunk.toString().substr(1).split(':')
      if (k === 'primaryKey') {
        this.primaryKeys = v.split('\x01')
        this.emit('info', ['primaryKeys', this.primaryKeys])
      } else if (k === 'dbTypes') {
        this.fields = obzip(this.headers, v.split('\x01'))
        this.emit('info', ['fields', this.fields])
      } else if (k === 'exportMode') {
        this.exportMode = v
        this.emit('info', ['exportMode', v])
      } else if (k !== '#legal') {
        this.emit('info', ['legal', v])
      } else {
        this[k] = v
      }
    } else {
      // data

      const row = parseRow(chunk, this)
      if (row) {
        this.push(row)
        this.emit('parsed', { row, chunk, lineNumber: this.currentLine })
      } else {
        this.emit('skipped', { chunk, lineNumber: this.currentLine })
      }
    }
    callback()
  }
}

export const createEPFParser = (options) => new EPFPaserStream(options)
export default createEPFParser
