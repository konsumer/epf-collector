#!/usr/bin/env node

// Check file MD5 sums

import { glob } from 'glob'
import { md5check } from './epf.js'

let [,, ...inFiles ] = process.argv
if (!inFiles.length) {
  inFiles = await glob(`data/epf/**/*.tbz`)
}

let allOk = true

for (const f of inFiles) {
  const ok = await md5check(f)
  if (!ok) {
    allOk = false
  }
  console.log(`${ok ? '\x1b[32mOK\x1b[0m' : '\x1b[41mNO\x1b[0m'} ${f}`)
}

if (!allOk) {
  process.exit(1)
}
