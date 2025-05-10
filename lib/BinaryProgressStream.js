// This is a passthrough-stream that will show progress, if you give it the total-size
//
// usage:
// import createBinaryProgressStream from './BinaryProgressStream.js'
// import { pipeline } from 'node:stream/promises'
//
// const progger = createBinaryProgressStream(size)
// await pipeline(reader, progger, writer)

import { Transform } from 'node:stream'

function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + ' ' + sizes[i]
}

function formatTime(seconds, decimals = 2) {
  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const remainingSeconds = seconds % 60
  const formattedHours = String(hours).padStart(2, '0')
  const formattedMinutes = String(minutes).padStart(2, '0')
  const formattedSeconds = String(remainingSeconds.toFixed(decimals)).padStart(2, '0')
  return `${formattedHours}:${formattedMinutes}:${formattedSeconds}`
}

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
    if (percentage !== 0 && (this.updateMore || percentage !== this.lastPercentage)) {
      this.lastPercentage = percentage
      this.elapsed = (Date.now() - this.startTime) / 1000
      this.bytesPerSec = this.bytesPassed / this.elapsed
      this.remaining = this.size - this.bytesPassed
      this.spin++
      this.outstream.write(
        this.updateProgressBar({
          percentage,
          showPercentage: this.showPercentage,
          elapsed: this.elapsed,
          bytesPerSec: this.bytesPerSec,
          remaining: this.remaining,
          barLength: this.barLength,
          barChar: this.barChar,
          emptyChar: this.emptyChar,
          spin: this.spin,
          spinner: this.spinner,
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
  updateProgressBar({ percentage, showPercentage, elapsed, bytesPerSec, remaining, barLength, barChar, emptyChar, spin, spinner, bytesPassed, clearLine, startTime }) {
    const speedText = formatBytes(bytesPerSec) + '/s'
    const etaText = bytesPerSec > 0 ? formatTime(remaining / bytesPerSec) : 'N/A'

    // Create the progress bar
    const filledLength = Math.round((barLength * percentage) / 100)
    const bar = barChar.repeat(filledLength) + emptyChar.repeat(barLength - filledLength)

    let progressText = `${bar} `

    if (showPercentage) {
      progressText += `${percentage}% `
    }

    if (spinner) {
      progressText += `${spinner[spin % spinner.length]} `
    }

    progressText += `${formatBytes(bytesPassed)} ${speedText} ETA: ${etaText}`

    return clearLine ? `\r\x1b[K${progressText}` : `\r${progressText}`
  }
}

export const createBinaryProgressStream = (size, outstream = process.stdout, options = {}) => {
  return new BinaryProgressStream(size, outstream, options)
}

export default createBinaryProgressStream
