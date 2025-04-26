// Library functions for turning EPF data into parquet files

import { createReadStream } from 'node:fs'
import { readFile, stat }  from 'node:fs/promises'
import { dirname } from 'node:path'
import { Transform } from 'node:stream'
import { mkdirSync } from 'node:fs'
import { createHash } from 'node:crypto'

import bz2 from 'unbzip2-stream'
import split from 'split2'
import parquetjs from 'parquetjs'
import { PassThrough } from 'node:stream'

export const exists = async f => {
  try {
    await stat(f)
    return true
  } catch (e) {
    return false
  }
}

// check a single file with md5 file next to it
export async function md5check(filePath) {
  const check = (await readFile(`${filePath}.md5`, 'utf8')).split(' = ').pop().trim()
  const hash = createHash('md5')
  for await (const chunk of createReadStream(filePath)) {
    hash.update(chunk)
  }
  return hash.digest('hex') === check
}

// Create a stream that reads EPF data and emits structured objects
export function createEpfStream(filename) {
  let headerProcessed = false
  let lineCount = 0
  const info = {}

  // Create transform stream that processes the EPF file
  const transform = new Transform({
    objectMode: true,

    transform(chunk, encoding, callback) {
      try {
        const line = chunk.toString()
        if (lineCount === 0) {
          info.dbNames = line.split('#').pop().split('\x01')
          lineCount++
          return callback()
        }
        if (line.startsWith('#')) {
          const [key, data] = line.slice(1).split(':')
          if (!key.startsWith('#')) {
            info[key] = data.split('\x01')
          }
          lineCount++
          return callback()
        }
        if (!headerProcessed) {
          headerProcessed = true
          transform.emit('schema', info)
        }
        if (line.trim() === '') {
          return callback()
        }
        const values = line.split('\x01')
        const row = {}
        for (const name of info.dbNames) {
          row[name] = null
        }
        for (let i = 0; i < info.dbNames.length && i < values.length; i++) {
          const name = info.dbNames[i]
          const type = info.dbTypes?.[i]
          const value = values[i]

          if (value === undefined || value === null || value === '') {
            row[name] = null
          } else if (type === 'BIGINT') {
            try {
              row[name] = parseInt(value, 10)
              if (isNaN(row[name])) row[name] = null
            } catch (e) {
              row[name] = null
            }
          } else if (type === 'DATETIME') {
            try {
              row[name] = new Date(value)
              if (isNaN(row[name].getTime())) row[name] = null
            } catch (e) {
              row[name] = null
            }
          } else {
            row[name] = value
          }
        }
        this.push(row)
        lineCount++
        callback()
      } catch (err) {
        callback(err)
      }
    }
  })

  return createReadStream(filename)
    .pipe(bz2())
    .pipe(split('\x02\n'))
    .pipe(transform)
}

// Create a direct file-writing ParquetWriter
export async function createParquetWriter(outputPath, info) {
  const schemaDefinition = {}
  for (let i = 0; i < info.dbNames.length; i++) {
    const name = info.dbNames[i]
    const type = info.dbTypes[i]
    let parquetType
    if (type === 'BIGINT') {
      parquetType = { type: 'INT64', optional: true }
    } else if (type === 'DATETIME') {
      parquetType = { type: 'TIMESTAMP_MILLIS', optional: true }
    } else {
      parquetType = { type: 'UTF8', optional: true }
    }
    schemaDefinition[name] = parquetType
  }
  const schema = new parquetjs.ParquetSchema(schemaDefinition)
  mkdirSync(dirname(outputPath), { recursive: true })
  const writer = await parquetjs.ParquetWriter.openFile(schema, outputPath, {
    compression: 'GZIP',
    rowGroupSize: 100000
  })
  return { writer, schema }
}

// Process an EPF file and convert to Parquet
export async function epfToParquet(inputFile, outPath) {
  return new Promise((resolve, reject) => {
    let writer = null
    let rowCount = 0
    let pendingWrites = Promise.resolve()
    const epfStream = createEpfStream(inputFile)
    const bufferStream = new PassThrough({ objectMode: true })
    epfStream.on('schema', async (info) => {
      try {
        const result = await createParquetWriter(outPath, info)
        writer = result.writer
        bufferStream.resume()
      } catch (err) {
        epfStream.destroy(err)
        reject(err)
      }
    })
    bufferStream.pause()
    bufferStream.on('data', (row) => {
      if (!writer) {
        return
      }
      rowCount++
      pendingWrites = pendingWrites.then(async () => {
        try {
          await writer.appendRow(row)
          if (rowCount % 10000 === 0) {
            console.log(`Processed ${rowCount} rows`)
          }
        } catch (err) {
          console.error('Error writing row:', err)
          epfStream.destroy(err)
        }
      })
    })

    bufferStream.on('end', async () => {
      try {
        await pendingWrites
        if (writer) {
          await writer.close()
        }
        resolve({
          path: outPath,
          rowCount
        })
      } catch (err) {
        reject(err)
      }
    })

    epfStream.on('error', reject)
    bufferStream.on('error', reject)
    epfStream.pipe(bufferStream)
  })
}
