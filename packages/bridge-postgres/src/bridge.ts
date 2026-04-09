import pg from 'pg'
import type {
  Bridge,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
  TargetSchema,
} from '@semilayer/core'
import { introspect, listTables } from './introspect.js'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

export interface PostgresBridgeConfig {
  url: string
  pool?: { min?: number; max?: number }
}

export class PostgresBridge implements Bridge {
  private pool: pg.Pool | null = null
  private config: PostgresBridgeConfig
  private pkCache = new Map<string, string>()

  constructor(config: Record<string, unknown>) {
    const url = (config['url'] ?? config['connectionString']) as string | undefined
    if (!url || typeof url !== 'string') {
      throw new Error('PostgresBridge requires a "url" config string')
    }
    this.config = {
      url,
      pool: config['pool'] as PostgresBridgeConfig['pool'],
    }
  }

  async connect(): Promise<void> {
    this.pool = new pg.Pool({
      connectionString: this.config.url,
      min: this.config.pool?.min ?? 0,
      max: this.config.pool?.max ?? 3,
    })
    const client = await this.pool.connect()
    try {
      await client.query('SELECT 1')
    } finally {
      client.release()
    }
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const pk = await this.getPrimaryKey(table)
    const fields = options?.fields
    const selectClause = fields ? fields.map(quote).join(', ') : '*'
    const limit = options?.limit ?? 1000

    const conditions: string[] = []
    const params: unknown[] = []
    let paramIdx = 1

    if (options?.cursor) {
      conditions.push(`${quote(pk)} > $${paramIdx}`)
      params.push(options.cursor)
      paramIdx++
    }

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const hasCol = await this.hasColumn(table, col)
      if (hasCol) {
        conditions.push(`${quote(col)} > $${paramIdx}`)
        params.push(options.changedSince)
        paramIdx++
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // Fetch limit+1 to detect whether there's a next page
    params.push(limit + 1)

    const sql = `SELECT ${selectClause} FROM ${quote(table)} ${whereClause} ORDER BY ${quote(pk)} ASC LIMIT $${paramIdx}`

    const result = await pool.query(sql, params)
    const allRows: BridgeRow[] = result.rows as BridgeRow[]

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore
      ? String(rows[rows.length - 1]![pk])
      : undefined

    const countResult = await pool.query(
      `SELECT count(*)::int AS total FROM ${quote(table)}`,
    )
    const total = (countResult.rows as Array<{ total: number }>)[0]!.total

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const result = await pool.query(
      `SELECT count(*)::int AS total FROM ${quote(table)}`,
    )
    return (result.rows as Array<{ total: number }>)[0]!.total
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.end()
      this.pool = null
    }
    this.pkCache.clear()
  }

  async query(
    target: string,
    options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const selectClause = options.select
      ? options.select.map(quote).join(', ')
      : '*'

    const params: unknown[] = []
    let paramIdx = 1

    // WHERE
    const conditions: string[] = []
    if (options.where) {
      for (const [key, value] of Object.entries(options.where)) {
        if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
          const ops = value as Record<string, unknown>
          for (const [op, opVal] of Object.entries(ops)) {
            switch (op) {
              case '$eq':
                conditions.push(`${quote(key)} = $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$gt':
                conditions.push(`${quote(key)} > $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$gte':
                conditions.push(`${quote(key)} >= $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$lt':
                conditions.push(`${quote(key)} < $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$lte':
                conditions.push(`${quote(key)} <= $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$in':
                conditions.push(
                  `${quote(key)} = ANY($${paramIdx})`,
                )
                params.push(opVal)
                paramIdx++
                break
              default:
                throw new Error(`Unknown operator "${op}" on field "${key}"`)
            }
          }
        } else {
          conditions.push(`${quote(key)} = $${paramIdx}`)
          params.push(value)
          paramIdx++
        }
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // ORDER BY
    let orderByClause = ''
    if (options.orderBy) {
      const clauses = Array.isArray(options.orderBy)
        ? options.orderBy
        : [options.orderBy]
      const parts = clauses.map(
        (c) => `${quote(c.field)} ${c.dir === 'desc' ? 'DESC' : 'ASC'}`,
      )
      orderByClause = `ORDER BY ${parts.join(', ')}`
    }

    // LIMIT / OFFSET
    let limitClause = ''
    if (options.limit != null) {
      limitClause = `LIMIT $${paramIdx}`
      params.push(options.limit)
      paramIdx++
    }

    let offsetClause = ''
    if (options.offset != null) {
      offsetClause = `OFFSET $${paramIdx}`
      params.push(options.offset)
      paramIdx++
    }

    const sql = [
      `SELECT ${selectClause} FROM ${quote(table)}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    // Get total count (with same WHERE, without LIMIT/OFFSET)
    const countSql = `SELECT count(*)::int AS total FROM ${quote(table)} ${whereClause}`
    const countParams = options.where ? params.slice(0, conditions.length) : []

    const [dataResult, countResult] = await Promise.all([
      pool.query(sql, params),
      pool.query(countSql, countParams),
    ])

    return {
      rows: dataResult.rows as BridgeRow[],
      total: (countResult.rows as Array<{ total: number }>)[0]!.total,
    }
  }

  // -------------------------------------------------------------------
  // Introspection
  // -------------------------------------------------------------------

  async listTargets(): Promise<string[]> {
    const pool = this.assertPool()
    return listTables(pool)
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    const pool = this.assertPool()
    const info = await introspect(pool, target)
    return {
      name: info.name,
      columns: info.columns,
      rowCount: info.rowCount,
    }
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertPool(): pg.Pool {
    if (!this.pool) throw new Error('PostgresBridge is not connected')
    return this.pool
  }

  private async getPrimaryKey(table: string): Promise<string> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const pool = this.assertPool()
    const result = await pool.query(
      `SELECT kcu.column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY'
         AND tc.table_name = $1
       LIMIT 1`,
      [table],
    )

    const row = (result.rows as Array<{ column_name: string }>)[0]
    if (!row) {
      throw new Error(
        `Could not detect primary key for table "${table}"`,
      )
    }
    this.pkCache.set(table, row.column_name)
    return row.column_name
  }

  private async hasColumn(table: string, column: string): Promise<boolean> {
    const pool = this.assertPool()
    const result = await pool.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_name = $1 AND column_name = $2
       LIMIT 1`,
      [table, column],
    )
    return result.rowCount != null && result.rowCount > 0
  }
}

function quote(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`
}

function assertTableName(table: string): void {
  if (!TABLE_NAME_RE.test(table)) {
    throw new Error(`Invalid table name: "${table}"`)
  }
}
