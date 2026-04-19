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
  TargetColumnInfo,
  TargetSchema,
} from '@semilayer/core'
import { BigQuery } from '@google-cloud/bigquery'

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

async function runQuery(bq: BigQuery, query: string, params?: Record<string, unknown>): Promise<BridgeRow[]> {
  const [job] = await bq.createQueryJob({ query, params: params ?? {} })
  const [rows] = await job.getQueryResults()
  return rows as BridgeRow[]
}

async function countQuery(bq: BigQuery, sql: string, params?: Record<string, unknown>): Promise<number> {
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

  async count(target: string): Promise<number> {
    const bq = this.assertClient()
    const { projectId, dataset } = this.cfg
    const ref = quoteRef(projectId, dataset, target)
    return countQuery(bq, `SELECT COUNT(*) as total FROM ${ref}`)
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

  async query(target: string, options: QueryOptions): Promise<QueryResult<BridgeRow>> {
    const bq = this.assertClient()
    const { projectId, dataset } = this.cfg
    const ref = quoteRef(projectId, dataset, target)

    const selectClause = options.select ? options.select.map((f) => `\`${f}\``).join(', ') : '*'

    const conditions: string[] = []
    const params: Record<string, unknown> = {}
    let paramIdx = 0

    // WHERE — BigQuery named params with @name syntax
    if (options.where) {
      for (const [key, value] of Object.entries(options.where)) {
        if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
          const ops = value as Record<string, unknown>
          for (const [op, opVal] of Object.entries(ops)) {
            const pName = `p${paramIdx++}`
            switch (op) {
              case '$eq':
                conditions.push(`\`${key}\` = @${pName}`)
                params[pName] = opVal
                break
              case '$gt':
                conditions.push(`\`${key}\` > @${pName}`)
                params[pName] = opVal
                break
              case '$gte':
                conditions.push(`\`${key}\` >= @${pName}`)
                params[pName] = opVal
                break
              case '$lt':
                conditions.push(`\`${key}\` < @${pName}`)
                params[pName] = opVal
                break
              case '$lte':
                conditions.push(`\`${key}\` <= @${pName}`)
                params[pName] = opVal
                break
              case '$in': {
                // Expand array to individual named params
                const inVals = opVal as unknown[]
                const inParams = inVals.map((v) => {
                  const ip = `p${paramIdx++}`
                  params[ip] = v
                  return `@${ip}`
                })
                conditions.push(`\`${key}\` IN (${inParams.join(', ')})`)
                break
              }
              default:
                throw new Error(`Unknown operator "${op}" on field "${key}"`)
            }
          }
        } else {
          const pName = `p${paramIdx++}`
          conditions.push(`\`${key}\` = @${pName}`)
          params[pName] = value
        }
      }
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

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
