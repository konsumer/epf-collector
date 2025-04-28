import { Transform } from 'node:stream'

import { tables, mapRow } from './schema.js'

// turns line-split messages into messages with header/data for each row
export function createEPFParserStream(table) {
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
        callback(null, { meta, row: mapRow(table, line.split('\x01')) })
      }
    }
  })
}
