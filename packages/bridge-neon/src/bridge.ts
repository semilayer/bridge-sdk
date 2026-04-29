import { neon, type NeonQueryFunction } from '@neondatabase/serverless'
import type {
  BatchReadOptions,
  Bridge,
  BridgeCapabilities,
  BridgeManifest,
  BridgeRow,
  CountOptions,
  QueryOptions,
  QueryResult,
  ReadOptions,
  ReadResult,
} from '@semilayer/core'
import {
  buildAggregateSql,
  buildWhereSql,
  executeAggregateQueries,
  POSTGRES_DIALECT,
  POSTGRES_FAMILY_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
  type WhereSqlDialect,
} from '@semilayer/bridge-sdk'

const PG_WHERE_DIALECT: WhereSqlDialect = {
  quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
  placeholder: (i) => `$${i}`,
  inUsesArrayParam: true,
  inList: (col, [ph]) => `${col} = ANY(${ph})`,
  notInList: (col, [ph]) => `${col} <> ALL(${ph})`,
}

const PG_LOGICAL_OPS = ['or', 'and', 'not'] as const
const PG_STRING_OPS = ['ilike', 'contains', 'startsWith', 'endsWith'] as const
const NEON_BRIDGE_NAME = '@semilayer/bridge-neon'

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
    whereLogicalOps: PG_LOGICAL_OPS,
    whereStringOps: PG_STRING_OPS,
    exactCount: true,
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

  async count(target: string, options?: CountOptions): Promise<number> {
    const sql = this.assertSql()
    assertTableName(target)
    const built = buildWhereSql(options?.where, PG_WHERE_DIALECT, {
      logicalOps: PG_LOGICAL_OPS,
      stringOps: PG_STRING_OPS,
      bridge: NEON_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const result = await sql(
      `SELECT count(*)::int AS total FROM ${quote(target)} ${whereClause}`,
      built.params,
    )
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

  aggregateCapabilities(): BridgeAggregateCapabilities {
    return POSTGRES_FAMILY_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const sql = this.assertSql()
    return executeAggregateQueries(
      buildAggregateSql(opts, POSTGRES_DIALECT),
      async (q, params) => {
        const result = await sql(q, params as unknown[])
        return result.rows as Array<Record<string, unknown>>
      },
    )
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

    const built = buildWhereSql(options.where, PG_WHERE_DIALECT, {
      logicalOps: PG_LOGICAL_OPS,
      stringOps: PG_STRING_OPS,
      bridge: NEON_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    let paramIdx = built.nextSlot

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

    const allParams = [...built.params, ...limitParams]
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
      sql(countSql, built.params),
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
