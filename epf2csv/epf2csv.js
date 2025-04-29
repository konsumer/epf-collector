// this is same as C program, implemented in node.js

import { Transform } from 'node:stream'
import createSplitStream from 'split2'

import createCSVStream from '../lib/csv.js'

// minimal transformer that takes EPF rows and outputs objects
function createEPFParserStream() {
  let headers
  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      const line = chunk.toString()
      if (!line.startsWith('#')) {
        const d = line.split('\x01').reduce((a, v, i) => ({...a, [headers[i]]: v}), {})
        callback(null, d)
      } else {
        if (!headers) {
          headers = line.slice(1).split('\x01')
        }
        callback()
      }
    }
  })
}

const splitter = createSplitStream('\x02\n')
const parser = createEPFParserStream()
const csv = createCSVStream()

process.stdin
  .pipe(splitter)
  .pipe(parser)
  .pipe(csv)
  .pipe(process.stdout)
