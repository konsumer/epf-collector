// similar to epf2csv, but for building a test-file (smaller, only has a few good lines, the meta-data, and all the bad lines)

import { Transform } from 'node:stream'
import createSplitStream from 'split2'

// minimal transformer that takes EPF rows and outputs objects (with header)
export default function createEPFParserStream() {
  let headers
  let goodCount = 0
  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      const line = chunk.toString()

      // 1st line
      if (!headers) {
        headers = line.split('#').pop().split('\x01')
        // output original header: this will actually create invalid tar file, but that's fine for our parser
        callback(null, `\x02${line}`)
      } else {
        if (line.startsWith('#')) {
          // output original comments
          callback(null, `\x02${line}`)
        } else {
          // check the data
          let l = line.split('\x01')

          // if headers do not match, it's bad
          if (l.length !== headers.length) {
            console.error('found a bad one', l)
            callback(null, `\x02${line}`)
          } else {
            // it's good: we only want 10 of these
            if (goodCount++ < 10) {
              callback(null, `\x02${line}`)
            } else {
              callback()
            }
          }
        }
      }
    }
  })
}


const splitter = createSplitStream('\x02\n')
const parser = createEPFParserStream()

process.stdin.pipe(splitter).pipe(parser).pipe(process.stdout)
