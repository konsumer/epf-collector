// utils for interacting with apple's EPF server

import { basename } from 'node:path'
import { createWriteStream } from 'node:fs'
import { writeFile } from 'node:fs/promises'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'

import * as cheerio from 'cheerio'
import progress from 'progress-stream'
import chalk from 'chalk'
import createBunzipStream from 'unbzip2-stream'

const { green, yellow, red } = chalk

import { md5check } from './fs.js'

const { EPF_USERNAME, EPF_PASSWORD } = process.env

if (!EPF_USERNAME || !EPF_PASSWORD) {
  console.error('EPF_USERNAME && EPF_PASSWORD are required.')
  process.exit(1)
}

// fetch-wrapper
export const get = (u) =>
  fetch(`https://feeds.itunes.apple.com/feeds/epf/v5/current${u}`, {
    headers: {
      Authorization: `Basic ${btoa(`${EPF_USERNAME}:${EPF_PASSWORD}`)}`
    }
  })

// get list of current EPF files for update/full
export async function getEPFList(type = 'update') {
  const listStartUrl = type === 'update' ? '/incremental/current/' : '/'
  const t = await get(listStartUrl).then((r) => r.text())
  const out = []
  const $ = cheerio.load(t)
  for (const a of $('a')) {
    if (!a.attribs.href.startsWith('?') && a.attribs.href !== 'incremental/') {
      const epf = a.attribs.href.replace('/', '')
      const t = await get(`${listStartUrl}/${epf}/`).then((r) => r.text())
      const $ = cheerio.load(t)
      for (const a of $('a')) {
        if (!a.attribs.href.startsWith('?') && !a.attribs.href.endsWith('.md5')) {
          const [m, group, year, month, day] = /([a-z_]+)([0-9]{4})([0-9]{2})([0-9]{2})/.exec(epf)
          out.push({
            url: `${listStartUrl}${epf}/${a.attribs.href}`,
            epf,
            group,
            date: new Date(year, month - 1, day),
            table: basename(a.attribs.href, '.tbz')
          })
        }
      }
    }
  }
  return out
}

// get a stream to show fetch/stream progress
export const createRequestProgessStream = (length) => {
  const spinner = '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
  let t = 0
  const p = progress({
    length,
    time: 500
  })
  p.on('progress', ({ percentage, transferred, length, remaining, eta, runtime, delta, speed }) => {
    let bar = ''
    for (let i = 0; i < 50; i++) {
      bar += i > percentage / 2 ? '░' : '█'
    }
    process.stdout.cursorTo(0)
    process.stdout.clearLine(0)
    process.stdout.write(`${bar} ${yellow(spinner[t++ % spinner.length])} ${percentage.toFixed(2)}% (${runtime}/${runtime + eta}s)`)
  })
  return p
}

// create a node stream from fetch
export async function createFetchStream(url, options = {}) {
  const response = await fetch(url, options)
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`)
  }
  if (!response.body) {
    throw new Error('Response body is empty')
  }
  const r = response.body instanceof ReadableStream ? Readable.fromWeb(response.body) : Readable.from(response.body)
  r.response = response
  return r
}

// this will download an EPF file with progress
export async function getEpfFileAsLocal(u, outFilename, getMd5) {
  const options = {
    headers: {
      Authorization: `Basic ${btoa(`${EPF_USERNAME}:${EPF_PASSWORD}`)}`
    }
  }
  const web = await createFetchStream(`https://feeds.itunes.apple.com/feeds/epf/v5/current${u}`, options)
  const prog = createRequestProgessStream(parseInt(web.response.headers.get('content-length')))
  const out = createWriteStream(outFilename)

  if (getMd5) {
    const d = await fetch(`https://feeds.itunes.apple.com/feeds/epf/v5/current${u}.md5`, options).then((r) => r.text())
    await writeFile(`${outFilename}.md5`, d)
  }

  await pipeline(web, prog, out)

  console.log()

  if (getMd5) {
    return await md5check(outFilename)
  }
}
