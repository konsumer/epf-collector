import { Transform } from 'node:stream'

// minimal transformer that takes EPF rows and outputs objects (with header)
export default function createEPFParserStream() {
  let headers
  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      const line = chunk.toString()
      if (!line.startsWith('#')) {
        const d = line.split('\x01').reduce((a, v, i) => ({ ...a, [headers[i]]: v }), {})
        callback(null, d)
      } else {
        if (!headers) {
          headers = line.split('#').pop().split('\x01')
        }
        callback()
      }
    }
  })
}
