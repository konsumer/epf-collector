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


// split on EPF \1, but not if it's an extended-character thing (like japanese)
const splitRow = chunk => chunk.toString('binary').split(/(?<!\uD800-\uDFFF)\u0001(?!\uD800-\uDFFF)/)


// parse a single row (Buffer) with lots of info
export function parseRow(chunk, { info: { name, group, date }, currentLine, fields, headers, primaryKeys, exportMode }) {
  let data = splitRow(chunk)

  // this handles a mismatch, just return undefined to tell the stream it was bad
  if (data.length !== headers.length) {
    return undefined
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
        // row was good, so push to next stream (as object, in data event) and also a parsed event
        // so you can record/count it
        this.push(row)
        this.emit('parsed', { row, chunk, lineNumber: this.currentLine })
      } else {
        // it was not able to be parsed, so emit a seperate message, so you can respond to that
        // it's not really an error, but you might want to record/count it, or try to fix it
        this.emit('skipped', { chunk, lineNumber: this.currentLine })
      }
    }
    callback()
  }
}

export const createEPFParser = (options) => new EPFPaserStream(options)
export default createEPFParser
