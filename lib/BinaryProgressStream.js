// This is a passthrough-stream that will show progress, if you give it the total-size
//
// usage:
// import createBinaryProgressStream from './BinaryProgressStream.js'
// import { pipeline } from 'node:stream/promises'
//
// const progger = createBinaryProgressStream(size)
// await pipeline(reader, progger, writer)

import { Transform } from 'node:stream'

export class BinaryProgressStream extends Transform {
  constructor(size, outstream = process.stdout, options = {}) {
    super(options)
    this.outstream = outstream
    this.size = size
    this.updateMore = options.updateMore
    this.bytesPassed = 0
    this.lastPercentage = -1
    this.barLength = options.barLength || 30
    this.clearLine = options.clearLine !== false
    this.showPercentage = options.showPercentage !== false
    this.barChar = options.barChar || '█'
    this.emptyChar = options.emptyChar || '░'
    this.spinner = options.spinner || '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    this.startTime = Date.now()
    this.spin = 0
  }

  _transform(chunk, encoding, callback) {
    this.bytesPassed += chunk.length

    // Calculate percentage and display progress
    const percentage = Math.min(100, Math.floor((this.bytesPassed / this.size) * 100))

    // Only update the progress bar when percentage changes
    if (this.updateMore || percentage !== this.lastPercentage) {
      this.lastPercentage = percentage
      this.updateProgressBar(percentage)
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

  updateProgressBar(percentage) {
    this.spin++
    if (percentage === 0) {
      return
    }
    const elapsed = (Date.now() - this.startTime) / 1000
    const bytesPerSec = this.bytesPassed / elapsed
    let speed = ''

    // Format speed
    if (bytesPerSec > 1024 * 1024 * 1024) {
      speed = `${(bytesPerSec / (1024 * 1024 * 1024)).toFixed(2)} GB/s`
    } else if (bytesPerSec > 1024 * 1024) {
      speed = `${(bytesPerSec / (1024 * 1024)).toFixed(2)} MB/s`
    } else if (bytesPerSec > 1024) {
      speed = `${(bytesPerSec / 1024).toFixed(2)} KB/s`
    } else {
      speed = `${Math.floor(bytesPerSec)} B/s`
    }

    // Calculate and format ETA
    const remaining = this.size - this.bytesPassed
    let eta = 'N/A'
    if (bytesPerSec > 0) {
      const etaSeconds = remaining / bytesPerSec
      if (etaSeconds < 60) {
        eta = `${Math.ceil(etaSeconds)}s`
      } else if (etaSeconds < 3600) {
        eta = `${Math.floor(etaSeconds / 60)}m ${Math.ceil(etaSeconds % 60)}s`
      } else {
        eta = `${Math.floor(etaSeconds / 3600)}h ${Math.floor((etaSeconds % 3600) / 60)}m`
      }
    }

    // Create the progress bar
    const filledLength = Math.round((this.barLength * percentage) / 100)
    const bar = this.barChar.repeat(filledLength) + this.emptyChar.repeat(this.barLength - filledLength)

    // Format the progress message
    let progressText = `${bar} `
    if (this.showPercentage) {
      progressText += `${percentage}% `
    }

    if (this.spinner) {
      progressText += `${this.spinner[this.spin % this.spinner.length]} `
    }

    progressText += `${(this.bytesPassed / (1024 * 1024)).toFixed(2)}/${(this.size / (1024 * 1024)).toFixed(2)} MB `
    progressText += `${speed} ETA: ${eta}`

    // Clear the line before writing new progress
    if (this.clearLine) {
      this.outstream.write('\r\x1b[K')
    } else {
      this.outstream.write('\r')
    }

    this.outstream.write(progressText)
  }
}

export const createBinaryProgressStream = (size, outstream = process.stdout, options = {}) => {
  return new BinaryProgressStream(size, outstream, options)
}

export default createBinaryProgressStream
