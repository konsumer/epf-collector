import { stat } from 'node:fs/promises'
import * as cheerio from 'cheerio'
import cliProgress from 'cli-progress'

const { EPF_USERNAME, EPF_PASSWORD } = process.env

if (!EPF_USERNAME || !EPF_PASSWORD) {
  console.error('EPF_USERNAME && EPF_PASSWORD are required.')
  process.exit(1)
}

// simply check if file exists
export const exists = async (path) => {
  try {
    await stat(path)
    return true
  } catch (e) {
    return false
  }
}

// fetch function to show progress
const showProgress = (progress) => (r) => {
  if (!r.ok) {
    throw Error(r.status + ' ' + r.statusText)
  }

  if (!r.body) {
    throw Error('ReadableStream not yet supported here.')
  }

  const contentEncoding = r.headers.get('content-encoding')
  const contentLength = r.headers.get(contentEncoding ? 'x-file-size' : 'content-length')
  if (contentLength === null) {
    throw Error('Response size header unavailable')
  }

  const total = parseInt(contentLength, 10)
  let loaded = 0

  return new Response(
    new ReadableStream({
      start(controller) {
        const reader = r.body.getReader()

        read()
        function read() {
          reader
            .read()
            .then(({ done, value }) => {
              if (done) {
                controller.close()
                return
              }
              loaded += value.byteLength
              progress({ loaded, total })
              controller.enqueue(value)
              read()
            })
            .catch((error) => {
              console.error(error)
              controller.error(error)
            })
        }
      }
    })
  )
}

// Get an EPF url
export const get = (u) => {
  const bar = new cliProgress.SingleBar({ etaBuffer: 10000, format: '{bar} {percentage}% | {duration_formatted}/{eta_formatted}' }, cliProgress.Presets.shades_classic)
  bar.start()
  return fetch(`https://feeds.itunes.apple.com/feeds/epf/${u}`, {
    headers: {
      Authorization: 'Basic ' + btoa(EPF_USERNAME + ':' + EPF_PASSWORD)
    }
  }).then(
    showProgress(({ loaded, total }) => {
      bar.total = parseInt(total / 1024 / 1024)
      if (total == loaded) {
        bar.stop()
      }
      bar.update(loaded / 1024 / 1024)
    })
  )
}

// get current list of tbz's
export async function getList(u = 'v5/current/') {
  const out = []
  const $ = cheerio.load(await get(u).then((r) => r.text()))
  for (const a of $('a')) {
    if (!a.attribs.href.startsWith('?')) {
      out.push(a.attribs.href)
    }
  }
  return out
}
