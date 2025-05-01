// I am trying to work out parsing issues with data in application/application_detail

import { Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { glob } from 'glob'
import createBunzipper from 'unbzip2-stream'
import { createReadStream, createWriteStream } from 'node:fs'
import { stringify } from 'csv-stringify'
import chalk from 'chalk'
import hexy from 'hexy'

let [, , ...inputFiles] = process.argv

if (!inputFiles?.length) {
  inputFiles = await glob('data/epf/**/*.tbz')
}

// turn 2 arrays (keys, values) into an object
const obzip = (keys, values) => keys.reduce((a, k, i) => ({ ...a, [k]: values[i] }), {})

class ByteSplitter extends Transform {
  constructor(delimiter, options = {}) {
    super(options);
    this.delimiter = delimiter;
    this.buffer = Buffer.alloc(0);
  }

  _transform(chunk, encoding, callback) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    let delimiterIndex;
    while ((delimiterIndex = this.buffer.indexOf(this.delimiter)) !== -1) {
      const data = this.buffer.slice(0, delimiterIndex);
      this.push(data);
      this.buffer = this.buffer.slice(delimiterIndex + this.delimiter.length);
    }
    callback();
  }

  _flush(callback) {
    if (this.buffer.length) {
      this.push(this.buffer);
    }
    callback();
  }
}

// parse a single row (Buffer) with lots of info
function parseRow(chunk, { info: {name, group, date }, fields, primaryKeys, exportMode}) {
  // these have problematic fields that need more specific parsing
  // if (['application', 'application_detail'].includes(name)) {
  //   return undefined
  // }
  const data = chunk.toString().split('\x01')
  const keys = Object.keys(fields)
  if (data.length !== keys.length) {
    console.error(chalk.red('MISMATCH'), 'got', data.length, 'expected', keys.length, data )
    return undefined
  }
  return obzip(keys, data)
}

// this will create Objects from a stream of line-buffers
export default function createEPFParserStream() {
  let currentLine = -1
  let headers
  const stream = new Transform({
    objectMode: true,

    transform(chunk, encoding, callback) {
      currentLine++
      let output = null

      if (currentLine === 0) { // first line is header
        const [junk, headersT] = chunk.toString().split('#')
        headers = headersT.split('\x01')
        // pull lots of info from tar-header
        const r = /^([a-z]+)([0-9]{4})([0-9]{2})([0-9]{2})\/([a-z_]+)/
        const [, group, year, month, day, name] = r.exec(junk)
        stream.info = { group, name, date: new Date(year, month - 1, day) }
        console.error(chalk.yellow('info'), stream.info)
      } else if (chunk[0] === 35){ // # comment
        let [k, v] = chunk.toString().substr(1).split(':')
        if (k === 'primaryKey') {
          stream.primaryKeys = v.split('\x01')
          console.error(chalk.yellow('primaryKeys'), stream.primaryKeys)
        } else if (k === 'dbTypes') {
          stream.fields = obzip(headers, v.split('\x01'))
          console.error(chalk.yellow('fields'), stream.fields)
        } else if (k === 'exportMode') {
          stream.exportMode = v
          console.error(chalk.yellow('exportMode'), stream.exportMode)
        } else if (k !== '#legal') {
          console.error(chalk.yellow(k), v)
        } else {
          stream[k] = v
        }
      } else { // data
        output = parseRow(chunk, stream)
        console.log(output)
      }

      callback(undefined, output)
    }
  })
  return stream
}

for (const file of inputFiles) {
  console.log(file)
  const reader = createReadStream(file)
  const unbzipper = createBunzipper()
  const splitter = new ByteSplitter('\x02\n')
  const parser = createEPFParserStream()

  await pipeline(reader, unbzipper, splitter, parser)
}
