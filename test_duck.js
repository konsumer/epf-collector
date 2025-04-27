import duckdb from 'duckdb'
import { promisify } from 'node:util'

// this will create 100 records then exit

const sql = s => s.join('\n')

const sqlCreate = sql`
  CREATE TABLE IF NOT EXISTS video_price (
    export_date BIGINT,
    video_id BIGINT,
    retail_price DECIMAL(9,3),
    currency_code VARCHAR(20),
    storefront_id INTEGER,
    availability_date DATETIME,
    sd_price DECIMAL(9,3),
    hq_price DECIMAL(9,3),
    lc_rental_price DECIMAL(9,3),
    sd_rental_price DECIMAL(9,3),
    hd_rental_price DECIMAL(9,3),
    hd_price DECIMAL(9,3),
    UNIQUE(video_id, storefront_id)
  );
`

const sqlInsert = sql`
  INSERT INTO video_price VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`

const row = [1745240509n, 1089772648n, 1.99, 'CAD', 143455, new Date(0), 1.99, 0, 0, 0, 0, 2.99]

// setup db
const db = new duckdb.Database(':memory:', async err => {
  if (err) {
    return console.error('ERROR (db open):', err)
  }

  // setup promise db-runner & create database
  const dbAll = promisify(db.all).bind(db)
  await dbAll(sqlCreate)

  // setup prepared statement runner
  const stmt = db.prepare(sqlInsert)
  const stmtRun =  promisify(stmt.run).bind(stmt)

  // inject 100 results
  for (let i = 0; i < 100; i++) {
    await stmtRun(...row)
  }

  // show that it worked
  console.log(await dbAll(sql`SELECT * FROM video_price;`))

  // cleanup
  stmt.finalize()
  db.close()
})
