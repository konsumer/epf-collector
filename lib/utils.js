import { createReadStream } from 'node:fs'
import { readFile, stat } from 'node:fs/promises'
import { createHash } from 'node:crypto'

// does the file exist?
export const exists = async (f) => {
  try {
    await stat(f)
    return true
  } catch (e) {
    return false
  }
}

export async function getFileSize(filePath) {
  try {
    const stats = await stat(filePath)
    return stats.size // Returns size in bytes
  } catch (error) {
    console.error(`Error getting file size: ${error.message}`)
    throw error
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

export function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + ' ' + sizes[i]
}

export function formatTime(seconds, decimals = 2) {
  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const remainingSeconds = seconds % 60
  const formattedHours = String(hours).padStart(2, '0')
  const formattedMinutes = String(minutes).padStart(2, '0')
  const formattedSeconds = String(remainingSeconds.toFixed(decimals)).padStart(2, '0')
  return `${formattedHours}:${formattedMinutes}:${formattedSeconds}`
}
