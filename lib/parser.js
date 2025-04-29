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
          let l = line.split('\x01')

          // if these don't match up, there is probly escapes in it
          if (l.length !== headers.length) {
            l = line.replace(/\x01\x01/, '').split('\x01')

            // if still no good, just show problem
            if (l.length !== headers.length) {
              console.error(`bad line (${l.length} != ${headers.length})`, l, headers)
              return callback()
            }
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
