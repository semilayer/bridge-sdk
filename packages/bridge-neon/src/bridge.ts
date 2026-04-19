import { neon, type NeonQueryFunction } from '@neondatabase/serverless'
import type {
  BatchReadOptions,
  Bridge,
  BridgeCapabilities,
  BridgeManifest,
  BridgeRow,
  QueryOptions,
  QueryResult,
  ReadOptions,
  ReadResult,
} from '@semilayer/core'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

export interface NeonBridgeConfig {
  url: string
}

export class NeonBridge implements Bridge {
  readonly capabilities: Partial<BridgeCapabilities> = {
    batchRead: true,
    wherePushdown: true,
    orderByPushdown: true,
    limitPushdown: true,
    selectProjection: true,
    nativeJoin: false,
    cursor: true,
    changedSince: true,
    perKeyLimit: false,
  }

  private sql: NeonQueryFunction<false, true> | null = null
  private pkCache = new Map<string, string>()
  private config: NeonBridgeConfig

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-neon',
    displayName: 'Neon',
    icon: 'neon',
    supportsUrl: true,
    urlPlaceholder: 'postgresql://user:pass@ep-xxx.us-east-2.aws.neon.tech/dbname?sslmode=require',
    fields: [],
  }

  constructor(config: Record<string, unknown>) {
    const url = config['url'] as string | undefined
    if (!url || typeof url !== 'string') {
      throw new Error('NeonBridge requires a "url" config string')
    }
    this.config = { url }
  }

  async connect(): Promise<void> {
    this.sql = neon(this.config.url, { fullResults: true, arrayMode: false }) as NeonQueryFunction<false, true>
    await this.sql('SELECT 1')
  }

  async disconnect(): Promise<void> {
    this.sql = null
    this.pkCache.clear()
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const sql = this.assertSql()
    assertTableName(target)

    const pk = await this.getPrimaryKey(target)
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
      const hasCol = await this.hasColumn(target, col)
      if (hasCol) {
        conditions.push(`${quote(col)} > $${paramIdx}`)
        params.push(options.changedSince)
        paramIdx++
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    params.push(limit + 1)
    const querySql = `SELECT ${selectClause} FROM ${quote(target)} ${whereClause} ORDER BY ${quote(pk)} ASC LIMIT $${paramIdx}`

    const result = await sql(querySql, params)
    const allRows: BridgeRow[] = result.rows as BridgeRow[]

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore ? String(rows[rows.length - 1]![pk]) : undefined

    const total = (await sql(`SELECT count(*)::int AS total FROM ${quote(target)}`)).rows[0]!['total'] as number

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    const sql = this.assertSql()
    assertTableName(target)
    const result = await sql(`SELECT count(*)::int AS total FROM ${quote(target)}`)
    return result.rows[0]!['total'] as number
  }

  async batchRead(
    target: string,
    options: BatchReadOptions,
  ): Promise<BridgeRow[]> {
    const result = await this.query(target, {
      where: options.where,
      select: options.select && options.select !== '*' ? options.select : undefined,
      orderBy: options.orderBy,
      limit: options.limit,
    })
    return result.rows
  }

  async query(
    target: string,
    options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    const sql = this.assertSql()
    assertTableName(target)

    const selectClause = options.select
      ? options.select.map(quote).join(', ')
      : '*'

    const params: unknown[] = []
    let paramIdx = 1

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
                conditions.push(`${quote(key)} = ANY($${paramIdx})`)
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

    let orderByClause = ''
    if (options.orderBy) {
      const raw = Array.isArray(options.orderBy) ? options.orderBy : [options.orderBy]
      const parts: string[] = []
      for (const clause of raw) {
        const obj = clause as unknown as Record<string, unknown>
        if (typeof obj.field === 'string') {
          parts.push(`${quote(obj.field)} ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`)
        } else {
          for (const [col, dir] of Object.entries(obj)) {
            if (dir === 'asc' || dir === 'desc') {
              parts.push(`${quote(col)} ${dir === 'desc' ? 'DESC' : 'ASC'}`)
            }
          }
        }
      }
      if (parts.length > 0) orderByClause = `ORDER BY ${parts.join(', ')}`
    }

    const limitParams: unknown[] = []
    let limitClause = ''
    if (options.limit != null) {
      limitClause = `LIMIT $${paramIdx}`
      limitParams.push(options.limit)
      paramIdx++
    }

    let offsetClause = ''
    if (options.offset != null) {
      offsetClause = `OFFSET $${paramIdx}`
      limitParams.push(options.offset)
      paramIdx++
    }

    const allParams = [...params, ...limitParams]
    const querySql = [
      `SELECT ${selectClause} FROM ${quote(target)}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT count(*)::int AS total FROM ${quote(target)} ${whereClause}`

    const [dataResult, countResult] = await Promise.all([
      sql(querySql, allParams),
      sql(countSql, params),
    ])

    return {
      rows: dataResult.rows as BridgeRow[],
      total: countResult.rows[0]!['total'] as number,
    }
  }

  async listTargets(): Promise<string[]> {
    const sql = this.assertSql()
    const result = await sql(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`,
    )
    return (result.rows as Array<{ table_name: string }>).map(r => r.table_name)
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertSql(): NeonQueryFunction<false, true> {
    if (!this.sql) throw new Error('NeonBridge is not connected')
    return this.sql
  }

  private async getPrimaryKey(table: string): Promise<string> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const sql = this.assertSql()
    const result = await sql(
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
      throw new Error(`Could not detect primary key for table "${table}"`)
    }
    this.pkCache.set(table, row.column_name)
    return row.column_name
  }

  private async hasColumn(table: string, column: string): Promise<boolean> {
    const sql = this.assertSql()
    const result = await sql(
      `SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2 LIMIT 1`,
      [table, column],
    )
    return result.rows.length > 0
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
