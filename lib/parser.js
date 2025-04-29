import { Transform } from 'node:stream'

// minimal transformer that takes EPF rows and outputs objects (with header)
export default function createEPFParserStream() {
  let headers
  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      const line = chunk.toString()

      // 1st line
      if (!headers) {
        headers = line.split('#').pop().split('\x01')
        callback()
      } else {
        if (!line.startsWith('#')) {
          const l = line.split('\x01')

          // if these don't match up, something is very wrong
          if (l.length !== headers.length) {
            console.error(`bad line (${l.length} != ${headers.length})`, l, headers)
            return callback()
          }

          const d = l.reduce((a, v, i) => ({ ...a, [headers[i]]: v }), {})
          callback(null, d)
        } else {
          callback()
        }
      }
    }
  })
}
