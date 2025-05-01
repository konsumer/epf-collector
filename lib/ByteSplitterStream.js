// This is a transform stream that will split bytes by other bytes (without converting to string)
//
// usage:
// import createByteSplitter from './ByteSplitterStream.js'
// import { pipeline } from 'node:stream/promises'
//
// const splitter = createByteSplitter('\x02\n')
// await pipeline(reader, splitter, writer)

import { Transform } from 'node:stream'

export class ByteSplitterStream extends Transform {
  constructor(delimiter, options = {}) {
    super(options)
    this.delimiter = delimiter
    this.buffer = Buffer.alloc(0)
  }

  _transform(chunk, encoding, callback) {
    this.buffer = Buffer.concat([this.buffer, chunk])
    let delimiterIndex
    while ((delimiterIndex = this.buffer.indexOf(this.delimiter)) !== -1) {
      const data = this.buffer.slice(0, delimiterIndex)
      this.push(data)
      this.buffer = this.buffer.slice(delimiterIndex + this.delimiter.length)
    }
    callback()
  }

  _flush(callback) {
    if (this.buffer.length) {
      this.push(this.buffer)
    }
    callback()
  }
}

export const createByteSplitter = (delimiter, options = {}) => new ByteSplitterStream(delimiter, options)
export default createByteSplitter
