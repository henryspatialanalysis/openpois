import { ref, shallowRef } from 'vue'
import * as duckdb from '@duckdb/duckdb-wasm'

const db = shallowRef(null)
const conn = shallowRef(null)
const ready = ref(false)
const initError = ref(null)
let initPromise = null

async function initDuckDB() {
  if (initPromise) return initPromise
  initPromise = _init()
  return initPromise
}

async function _init() {
  try {
    // Use local worker/wasm files served from public/ to avoid
    // cross-origin Worker restrictions with CDN bundles.
    const baseUrl = import.meta.env.BASE_URL
    const workerUrl = `${baseUrl}duckdb-browser-eh.worker.js`
    const wasmUrl = `${baseUrl}duckdb-eh.wasm`

    const worker = new Worker(workerUrl)
    const logger = new duckdb.VoidLogger()
    const instance = new duckdb.AsyncDuckDB(logger, worker)
    await instance.instantiate(wasmUrl)

    db.value = instance
    const connection = await instance.connect()
    conn.value = connection

    // Load extensions (WASM bundles auto-install from CDN on first LOAD)
    await connection.query(`LOAD httpfs;`)
    await connection.query(`LOAD spatial;`)
    await connection.query(`SET s3_region = 'us-west-2';`)
    // Allow anonymous S3 access for public buckets
    await connection.query(`SET s3_access_key_id = '';`)
    await connection.query(`SET s3_secret_access_key = '';`)

    ready.value = true
    console.log('DuckDB-WASM initialized')
  } catch (err) {
    initError.value = err
    console.error('DuckDB-WASM init failed:', err)
    throw err
  }
}

async function runQuery(sql) {
  if (!conn.value) throw new Error('DuckDB not initialized')
  const result = await conn.value.query(sql)
  return result
}

export function useDuckDB() {
  return { db, conn, ready, initError, initDuckDB, runQuery }
}
