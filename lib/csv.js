// this is a stream that outputs well-formed CSV that duck is happy with

import { Transform } from 'node:stream'

export default function createCSVStream(options = {}) {
  const { headers = true, delimiter = ',', columns = null } = options

  let headerWritten = false
  let knownColumns = columns ? [...columns] : []

  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      try {
        if (!knownColumns.length && chunk) {
          knownColumns = Object.keys(chunk)
        }
        if (headers && !headerWritten && knownColumns.length) {
          this.push(knownColumns.join(delimiter) + '\n')
          headerWritten = true
        }
        const row = knownColumns.map((column) => escapeCSVField(chunk[column], delimiter).replace(/\n/g, '\\n').replace(/\r/g, ''))
        callback(null, row.join(delimiter) + '\n')
      } catch (error) {
        callback(error)
      }
    }
  })
}

function escapeCSVField(field, delimiter) {
  if (field === null || field === undefined) {
    return ''
  }
  const stringField = String(field)
  if (stringField.includes(delimiter) || stringField.includes('"')) {
    return '"' + stringField.replace(/"/g, '""') + '"'
  }
  return stringField
}
