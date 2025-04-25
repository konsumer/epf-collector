// this grabs the current incremental collection
// normally you would run this once a week

import { writeFile, mkdir } from 'node:fs/promises'
import { get, getList, exists } from './epf.js'

const outDir = 'data/epf/incremental'

// get all the full dumps
console.log('Getting collections...')
for (const collection of await getList('v5/current/incremental/current/')) {
  console.log(collection)
  const files = await getList(`v5/current/incremental/current/${collection}`)
  for (const file of files) {
    const fe = await exists(`${outDir}/${collection}${file}`)
    try {
      await mkdir(`${outDir}/${collection}`, { recursive: true })
    } catch (e) {}
    console.log(`  ${outDir}/${collection}${file}: ${fe ? 'exists' : 'downloading'}`)
    if (!fe) {
      const bytes = await get(`v5/current/incremental/current/${collection}${file}`).then((r) => r.arrayBuffer())
      await writeFile(`${outDir}/${collection}${file}`, new Uint8Array(bytes))
    }
  }
}
