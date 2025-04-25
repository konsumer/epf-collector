// this just grabs the initial collection (full dumps)
// normally you would also do incremental once a week

import { writeFile, stat, mkdir } from 'node:fs/promises'
import { get, getList } from './epf.js'

const exists = async (path) => {
  try {
    await stat(path)
    return true
  } catch (e) {
    return false
  }
}

// get all the full dumps
for (const collection of await getList()) {
  if (collection !== 'incremental/') {
    const files = await getList(`v5/current/${collection}`)
    console.log(collection)
    for (const file of files) {
      const fe = await exists(`data/${collection}${file}`)
      try {
        await mkdir(`data/${collection}`, { recursive: true })
      } catch (e) {}
      console.log(`  data/${collection}${file}: ${fe ? 'exists' : 'downloading'}`)
      if (!fe) {
        const bytes = await get(`v5/current/${collection}${file}`).then((r) => r.arrayBuffer())
        await writeFile(`data/${collection}${file}`, new Uint8Array(bytes))
      }
    }
  }
}
