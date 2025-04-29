// this is a tiny importer
// It takes EPF data on stdin, and outputs CSV on stdout

import createSplitStream from 'split2'

import createCSVStream from './csv.js'
import createEPFParserStream from './parser.js'

const splitter = createSplitStream('\x02\n')
const parser = createEPFParserStream()
const csv = createCSVStream()

process.stdin.pipe(splitter).pipe(parser).pipe(csv).pipe(process.stdout)
