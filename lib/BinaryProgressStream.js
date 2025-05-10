// This is a passthrough-stream that will show progress, if you give it the total-size
//
// usage:
// import createBinaryProgressStream from './BinaryProgressStream.js'
// import { pipeline } from 'node:stream/promises'
//
// const progger = createBinaryProgressStream(size)
// await pipeline(reader, progger, writer)

import { Transform } from 'node:stream'

import { formatBytes, formatTime } from './utils.js'

export class BinaryProgressStream extends Transform {
  constructor(size, outstream = process.stdout, options = {}) {
    super(options)
    this.outstream = outstream
    this.size = size
    this.updateMore = options.updateMore // set to true to update much more often
    this.bytesPassed = 0
    this.lastPercentage = -1
    this.startTime = Date.now()
    this.counter = 0
  }

  _transform(chunk, encoding, callback) {
    this.bytesPassed += chunk.length

    // Calculate percentage and display progress
    const percentage = Math.min(100, Math.floor((this.bytesPassed / this.size) * 100))

    // Only update the progress bar when percentage changes
    if (percentage !== 0 && (this.updateMore || percentage !== this.lastPercentage)) {
      this.lastPercentage = percentage
      const elapsed = (Date.now() - this.startTime) / 1000
      this.outstream.write(
        this.updateProgressBar({
          percentage,
          elapsed,
          size: this.size,
          bytesPerSec: this.bytesPassed / elapsed,
          remaining: this.size - this.bytesPassed,
          counter: this.counter++,
          bytesPassed: this.bytesPassed,
          clearLine: this.clearLine,
          startTime: this.startTime
        })
      )
    }

    // Push the chunk through the transform stream
    this.push(chunk)
    callback()
  }

  _flush(callback) {
    // Make sure we display 100% at the end
    if (this.lastPercentage !== 100) {
      this.lastPercentage = 100
      this.updateProgressBar(100)
    }

    // Add a newline after we're done
    this.outstream.write('\n')

    callback()
  }

  // override this to display things differently
  updateProgressBar({ percentage, elapsed, bytesPerSec, remaining, counter, bytesPassed, startTime, size }) {
    const spinner = '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    const barChar = '█'
    const emptyChar = '░'
    const barLength = 30

    const etaText = bytesPerSec > 0 ? formatTime(remaining / bytesPerSec) : 'N/A'
    const filledLength = Math.round((barLength * percentage) / 100)
    const bar = barChar.repeat(filledLength) + emptyChar.repeat(barLength - filledLength)

    return `\r\x1b[K${bar} ${percentage}% ${spinner[counter % spinner.length]} ${formatBytes(bytesPassed, 0)}/${formatBytes(size, 0)} - ${formatBytes(bytesPerSec)}/s, ETA: ${etaText}`
  }
}

export const createBinaryProgressStream = (size, outstream = process.stdout, options = {}) => {
  return new BinaryProgressStream(size, outstream, options)
}

export default createBinaryProgressStream
