#!/usr/bin/env node

import { getList, get } from './epf_remote.js'
import { exists } from './epf.js'

// this grabs the initial collection (full dumps)
// normally you would also do update once a week
// and full only once, initially

import { basename } from 'node:path'
import { mkdir } from 'node:fs/promises'


let [,,type='update', outputDir='data/epf', ...groups] = process.argv
if (!groups.length) {
  groups = ['itunes', 'match', 'popularity', 'pricing']
}

const listStartUrl = type === 'update' ? 'v5/current/incremental/current/' : 'v5/current/'

console.log(`Collecting ${groups.map(s => `\x1b[33m${s}\x1b[0m`).join(', ')}`)

for (const groupList of await getList(listStartUrl)) {
  if (groupList !== 'incremental/') {
    const groupUrl = `${listStartUrl}${groupList}`
    const group = groupUrl.split('/').at(-2)
    await mkdir( `${outputDir}/${type}/${group}`, { recursive: true })
    for (const fileUrl of await getList(groupUrl)) {
      const filename = `${outputDir}/${type}/${group}/${basename(fileUrl)}`
      if (await exists(filename)) {
        console.log(`\x1b[32mskipping\x1b[0m ${filename}`)
      } else {
        console.log(`\x1b[32mdownloading\x1b[0m ${filename}`)
        await get(`${groupUrl}${fileUrl}`, filename)
      }
    }
  }
}
