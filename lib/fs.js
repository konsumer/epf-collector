// utils for interacting with filesystem

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

// check a single file with md5 file next to it
export async function md5check(filePath) {
  const check = (await readFile(`${filePath}.md5`, 'utf8')).split(' = ').pop().trim()
  const hash = createHash('md5')
  for await (const chunk of createReadStream(filePath)) {
    hash.update(chunk)
  }
  return hash.digest('hex') === check
}
