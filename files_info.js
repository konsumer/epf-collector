#!/usr/bin/env node

// Gets info about EPF files, on-disk
import { basename } from 'node:path'

import { glob } from 'glob'

import { createEpfStream } from './epf.js'

let [,, ...inFiles ] = process.argv
if (!inFiles.length) {
  inFiles = await glob(`data/epf/**/*.tbz`)
}

for (const f of inFiles) {
  const epfStream = createEpfStream(f)
  epfStream.on('schema', async (info) => {
    try {
      console.log(basename(f, '.tbz'), { file: f, ...info })
      epfStream.destroy()
    } catch (err) {
      epfStream.destroy(err)
    }
  })
}
