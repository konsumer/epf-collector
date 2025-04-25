import { createEpfParseStream } from './epf.js'
import { glob } from 'glob'
import { dirname } from 'node:path'

let [, , ...epfs] = process.argv

if (!epfs.length) {
  epfs = await glob('data/epf/**/*.tbz')
}

for (const f of epfs) {
  createEpfParseStream(f, true)
    .on('info', (i) => {
      console.log(i)
    })
}
