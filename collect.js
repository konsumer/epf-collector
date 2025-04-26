#!/usr/bin/env node

// This grabs EPF data and inserts it into database
// normally you would also do update once a week
// and full only once, initially

let [,,type='update', outputDir='data/parquet', ...groups] = process.argv
if (!groups.length) {
  groups = ['itunes', 'match', 'popularity', 'pricing']
}

console.log('TODO')
console.log({type, outputDir, groups})
