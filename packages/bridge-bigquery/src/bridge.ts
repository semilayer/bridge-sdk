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
  TargetColumnInfo,
  TargetSchema,
} from '@semilayer/core'
import { BigQuery } from '@google-cloud/bigquery'
import {
  buildAggregateSql,
  buildWhereSql,
  executeAggregateQueries,
  BIGQUERY_DIALECT,
  BIGQUERY_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
  type WhereSqlDialect,
} from '@semilayer/bridge-sdk'

// BigQuery where dialect — backtick-quoted identifiers, `?` positional binds
// (BigQuery's Node SDK accepts `params: unknown[]` for positional `?`).
// BigQuery has no native ILIKE — emit `LOWER(col) LIKE LOWER(?)`. BigQuery's
// LIKE does NOT accept the trailing `ESCAPE '\'` clause that ANSI / Postgres /
// MySQL accept, so we disable the helper's escape clause and live with the
// (rare) edge case where users would need to LIKE-match a literal `%`.
const BQ_WHERE_DIALECT: WhereSqlDialect = {
  quoteIdent: (n) => '`' + n.replace(/`/g, '\\`') + '`',
  placeholder: () => '?',
  ilike: (col, p) => `LOWER(${col}) LIKE LOWER(${p})`,
  supportsLikeEscape: false,
}

const BQ_LOGICAL_OPS = ['or', 'and', 'not'] as const
const BQ_STRING_OPS = ['ilike', 'contains', 'startsWith', 'endsWith'] as const
const BQ_BRIDGE_NAME = '@semilayer/bridge-bigquery'

export interface BigqueryBridgeConfig {
  projectId: string
  dataset: string
  serviceAccountEmail?: string
  serviceAccountKey?: string
  credentials?: { client_email: string; private_key: string }
  keyFilename?: string
}

interface BigQuerySchemaField {
  name: string
  type: string
  mode?: string
}

interface BigQueryTableMetadata {
  schema?: {
    fields?: BigQuerySchemaField[]
  }
}

type CountRow = { total: { value: string } | number | bigint }

function quoteRef(projectId: string, dataset: string, table: string): string {
  return `\`${projectId}.${dataset}.${table}\``
}

type QueryParams = Record<string, unknown> | unknown[]

async function runQuery(
  bq: BigQuery,
  query: string,
  params?: QueryParams,
): Promise<BridgeRow[]> {
  const [job] = await bq.createQueryJob({
    query,
    params: (params ?? {}) as Record<string, unknown>,
  })
  const [rows] = await job.getQueryResults()
  return rows as BridgeRow[]
}

async function countQuery(
  bq: BigQuery,
  sql: string,
  params?: QueryParams,
): Promise<number> {
  const rows = await runQuery(bq, sql, params)
  const row = (rows as CountRow[])[0]
  const raw = row?.total
  if (raw == null) return 0
  if (typeof raw === 'object' && 'value' in raw) return Number(raw.value)
  return Number(raw)
}

export class BigqueryBridge implements Bridge {
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
    whereLogicalOps: BQ_LOGICAL_OPS,
    whereStringOps: BQ_STRING_OPS,
    exactCount: true,
  }

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-bigquery',
    displayName: 'BigQuery',
    icon: 'bigquery',
    supportsUrl: false,
    fields: [
      {
        key: 'projectId',
        label: 'Project ID',
        type: 'string',
        required: true,
        placeholder: 'my-gcp-project',
      },
      {
        key: 'dataset',
        label: 'Dataset',
        type: 'string',
        required: true,
        placeholder: 'my_dataset',
        hint: 'Dataset ID',
      },
      {
        key: 'serviceAccountEmail',
        label: 'Service Account Email',
        type: 'string',
        required: false,
        group: 'advanced',
        hint: 'Service account email',
      },
      {
        key: 'serviceAccountKey',
        label: 'Service Account Private Key',
        type: 'password',
        required: false,
        group: 'advanced',
        hint: 'Service account private key',
      },
      {
        key: 'keyFilename',
        label: 'Key File Path',
        type: 'string',
        required: false,
        group: 'advanced',
        hint: 'Path to service account JSON key file',
      },
    ],
  }

  private bq: BigQuery | null = null
  private readonly cfg: BigqueryBridgeConfig

  constructor(config: Record<string, unknown>) {
    const projectId = config['projectId'] as string | undefined
    if (!projectId) throw new Error('BigqueryBridge requires a "projectId" config value')

    const dataset = config['dataset'] as string | undefined
    if (!dataset) throw new Error('BigqueryBridge requires a "dataset" config value')

    const email = config['serviceAccountEmail'] as string | undefined
    const key = config['serviceAccountKey'] as string | undefined

    const credentials =
      email && key
        ? { client_email: email, private_key: key }
        : (config['credentials'] as { client_email: string; private_key: string } | undefined)

    this.cfg = {
      projectId,
      dataset,
      credentials,
      keyFilename: config['keyFilename'] as string | undefined,
    }
  }

  async connect(): Promise<void> {
    const { projectId, credentials, keyFilename } = this.cfg

    const bqOptions: ConstructorParameters<typeof BigQuery>[0] = { projectId }
    if (credentials) bqOptions.credentials = credentials
    if (keyFilename) bqOptions.keyFilename = keyFilename

    this.bq = new BigQuery(bqOptions)

    // Verify connectivity by listing datasets (lightweight probe)
    await this.bq.getDatasets({ maxResults: 1 })
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const bq = this.assertClient()
    const { projectId, dataset } = this.cfg

    const fields = options?.fields
    const selectClause = fields ? fields.map((f) => `\`${f}\``).join(', ') : '*'
    const limit = options?.limit ?? 1000
    const offset = options?.cursor ? parseInt(options.cursor, 10) : 0

    const ref = quoteRef(projectId, dataset, target)

    const conditions: string[] = []
    const params: Record<string, unknown> = {}
    let paramIdx = 0

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const pName = `p${paramIdx++}`
      conditions.push(`\`${col}\` > @${pName}`)
      params[pName] = options.changedSince.toISOString()
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // Fetch limit+1 to detect next page
    const sql = `SELECT ${selectClause} FROM ${ref} ${whereClause} LIMIT ${limit + 1} OFFSET ${offset}`
    const countSql = `SELECT COUNT(*) as total FROM ${ref} ${whereClause}`

    const [allRows, total] = await Promise.all([
      runQuery(bq, sql, params),
      countQuery(bq, countSql, params),
    ])

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore ? String(offset + limit) : undefined

    return { rows, nextCursor, total }
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    const bq = this.assertClient()
    const { projectId, dataset } = this.cfg
    const ref = quoteRef(projectId, dataset, target)
    const built = buildWhereSql(options?.where, BQ_WHERE_DIALECT, {
      logicalOps: BQ_LOGICAL_OPS,
      stringOps: BQ_STRING_OPS,
      bridge: BQ_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    return countQuery(
      bq,
      `SELECT COUNT(*) as total FROM ${ref} ${whereClause}`,
      built.params,
    )
  }

  async disconnect(): Promise<void> {
    // BigQuery SDK is stateless HTTP — no persistent connection to close
    this.bq = null
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
    return BIGQUERY_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const bq = this.assertClient()
    const { projectId, dataset } = this.cfg
    // Build a fully-qualified target so the SQL builder's
    // `qualifyTarget` quotes each segment correctly. Splitting on `.` is
    // what every SQL bridge expects — BigQuery just has three segments
    // instead of two.
    const qualifiedOpts = {
      ...opts,
      target: `${projectId}.${dataset}.${opts.target}`,
    }
    return executeAggregateQueries(
      buildAggregateSql(qualifiedOpts, BIGQUERY_DIALECT),
      async (query, params) => {
        const [rows] = await bq.query({ query, params: params as unknown[] })
        return rows as Array<Record<string, unknown>>
      },
    )
  }

  async query(target: string, options: QueryOptions): Promise<QueryResult<BridgeRow>> {
    const bq = this.assertClient()
    const { projectId, dataset } = this.cfg
    const ref = quoteRef(projectId, dataset, target)

    const selectClause = options.select ? options.select.map((f) => `\`${f}\``).join(', ') : '*'

    const built = buildWhereSql(options.where, BQ_WHERE_DIALECT, {
      logicalOps: BQ_LOGICAL_OPS,
      stringOps: BQ_STRING_OPS,
      bridge: BQ_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const params: unknown[] = built.params

    // ORDER BY
    let orderByClause = ''
    if (options.orderBy) {
      const raw = Array.isArray(options.orderBy) ? options.orderBy : [options.orderBy]
      const parts: string[] = []
      for (const clause of raw) {
        const obj = clause as unknown as Record<string, unknown>
        if (typeof obj.field === 'string') {
          parts.push(`\`${obj.field}\` ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`)
        } else {
          for (const [col, dir] of Object.entries(obj)) {
            if (dir === 'asc' || dir === 'desc') {
              parts.push(`\`${col}\` ${dir === 'desc' ? 'DESC' : 'ASC'}`)
            }
          }
        }
      }
      if (parts.length > 0) orderByClause = `ORDER BY ${parts.join(', ')}`
    }

    const limitClause = options.limit != null ? `LIMIT ${options.limit}` : ''
    const offsetClause = options.offset != null ? `OFFSET ${options.offset}` : ''

    const sql = [
      `SELECT ${selectClause} FROM ${ref}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT COUNT(*) as total FROM ${ref} ${whereClause}`

    const [rows, total] = await Promise.all([
      runQuery(bq, sql, params),
      countQuery(bq, countSql, params),
    ])

    return { rows, total }
  }

  async listTargets(): Promise<string[]> {
    const bq = this.assertClient()
    const { dataset } = this.cfg
    const [tables] = await bq.dataset(dataset).getTables()
    return tables.map((t) => t.id ?? '').filter(Boolean)
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    const bq = this.assertClient()
    const { projectId, dataset } = this.cfg
    const ref = quoteRef(projectId, dataset, target)

    const [meta] = (await bq.dataset(dataset).table(target).getMetadata()) as [BigQueryTableMetadata, unknown]
    const fields = meta?.schema?.fields ?? []

    const columns: TargetColumnInfo[] = fields.map((field) => ({
      name: field.name,
      type: field.type,
      nullable: field.mode !== 'REQUIRED',
      primaryKey: false,
    }))

    const rowCount = await countQuery(bq, `SELECT COUNT(*) as total FROM ${ref}`)

    return { name: target, columns, rowCount }
  }

  private assertClient(): BigQuery {
    if (!this.bq) throw new Error('BigqueryBridge is not connected')
    return this.bq
  }
}
