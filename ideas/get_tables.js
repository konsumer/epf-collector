// this will build the js so I can know structure of all EPF types (as it appears in the files)

import { createReadStream } from 'node:fs'
import { Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { basename } from 'node:path'

import { glob } from 'glob'
import createSplitStream from 'split2'
import createBunzipper from 'unbzip2-stream'

let [, , ...inputFiles] = process.argv

if (!inputFiles?.length) {
  inputFiles = await glob('data/epf/**/*.tbz')
}

// turn 2 arrays (keys, values) into an object
const obzip = (keys, values) => keys.reduce((a, k, i) => ({ ...a, [k]: values[i] }), {})


// receives a stream of EPF lines, outputs objects for each row
export default function createEPFParserStream() {
  let outputinfo = false
  const stream = new Transform({
    objectMode: true,

    transform(chunk, encoding, callback) {
      const line = chunk.toString('utf8')
      stream.lineNum ||= -1
      stream.problems ||= []
      stream.lineNum++

      // 1st line is header
      if (!stream.headers) {
        stream.problems.push(line)
        const [junk, headersT] = line.split('#')
        stream.headers = headersT.split('\x01')
        // pull lots of info from tar-header
        const r = /^([a-z]+)([0-9]{4})([0-9]{2})([0-9]{2})\/([a-z_]+)/
        const [, group, year, month, day, name] = r.exec(junk)
        stream.info = { group, name, date: new Date(year, month - 1, day) }
        return callback()
      } else {
        // handle comments
        if (line.startsWith('#')) {
          let [k, v] = line.substr(1).split(':')
          if (k === 'primaryKey') {
            stream.primaryKeys = v.split('\x01')
          } else if (k === 'dbTypes') {
            stream.fields = obzip(stream.headers, v.split('\x01'))
          } else if (k === 'exportMode') {
            stream.exportMode = v
          } else if (k !== '#legal') {
          } else {
            stream[k] = v
          }
          return callback()
        }
        if (!outputinfo) {
          outputinfo = true
          console.log(`export const ${stream.info.name} = ` + JSON.stringify({
            ...stream.info,
            primaryKeys: stream.primaryKeys,
            fields: stream.fields,
            exportMode: stream.exportMode
          }))
          callback()
        } else {
          callback('stop')
        }
      }
    }
  })
  return stream
}



for (const file of inputFiles) {
  const name = basename(file, '.tbz')

  const reader = createReadStream(file)
  const unbzipper = createBunzipper()
  const splitter = createSplitStream('\u0002\n')
  const parser = createEPFParserStream()

  // fast exit and move on
  try{
    await pipeline(reader, unbzipper, splitter, parser)
  }catch (e) { }
}
