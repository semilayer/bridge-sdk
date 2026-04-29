import {
  DynamoDBClient,
  ListTablesCommand,
  DescribeTableCommand,
} from '@aws-sdk/client-dynamodb'
import {
  DynamoDBDocumentClient,
  ScanCommand,
  type ScanCommandInput,
} from '@aws-sdk/lib-dynamodb'
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
  assertSupportedOps,
  streamingAggregate,
  STREAMING_AGGREGATE_CAPABILITIES,
  UnsupportedOperatorError,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
} from '@semilayer/bridge-sdk'

// DynamoDB FilterExpression supports OR/AND/NOT and contains/begins_with,
// but only after a Scan/Query has already shipped rows over the wire.
// Declaring those ops as supported would make us the first place callers
// expect cheap pushdown — too risky for v1. Declare empty for both, and
// keep `exactCount: false` because Scan with FilterExpression returns a
// post-filter `Count` only over the partition that was scanned.
const DYNAMODB_LOGICAL_OPS = [] as const
const DYNAMODB_STRING_OPS = [] as const
const DYNAMODB_BRIDGE_NAME = '@semilayer/bridge-dynamodb'

export interface DynamodbBridgeConfig {
  region: string
  accessKeyId?: string
  secretAccessKey?: string
  sessionToken?: string
  endpoint?: string
}

export class DynamodbBridge implements Bridge {
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
    whereLogicalOps: DYNAMODB_LOGICAL_OPS,
    whereStringOps: DYNAMODB_STRING_OPS,
    exactCount: false,
  }

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-dynamodb',
    displayName: 'DynamoDB',
    icon: 'dynamodb',
    supportsUrl: false,
    fields: [
      {
        key: 'region',
        label: 'Region',
        type: 'string',
        required: true,
        placeholder: 'us-east-1',
        hint: 'AWS region where your DynamoDB tables are',
      },
      {
        key: 'accessKeyId',
        label: 'Access Key ID',
        type: 'string',
        required: true,
        hint: 'AWS access key ID with DynamoDB read permissions',
      },
      {
        key: 'secretAccessKey',
        label: 'Secret Access Key',
        type: 'password',
        required: true,
        hint: 'AWS secret access key',
      },
      {
        key: 'sessionToken',
        label: 'Session Token',
        type: 'password',
        required: false,
        group: 'advanced',
        hint: 'Session token (only for temporary credentials)',
      },
      {
        key: 'endpoint',
        label: 'Endpoint',
        type: 'string',
        required: false,
        group: 'advanced',
        hint: 'Custom endpoint URL (for local testing with DynamoDB Local)',
      },
    ],
  }

  private rawClient: DynamoDBClient | null = null
  private docClient: DynamoDBDocumentClient | null = null
  private pkCache = new Map<string, string>()
  private config: DynamodbBridgeConfig

  constructor(config: Record<string, unknown>) {
    const region = config['region'] as string | undefined
    if (!region || typeof region !== 'string')
      throw new Error('DynamodbBridge requires a "region" config string')
    const accessKeyId = config['accessKeyId'] as string | undefined
    const secretAccessKey = config['secretAccessKey'] as string | undefined
    if (accessKeyId && !secretAccessKey)
      throw new Error('DynamodbBridge requires "secretAccessKey" when "accessKeyId" is provided')
    this.config = {
      region,
      accessKeyId,
      secretAccessKey,
      sessionToken: config['sessionToken'] as string | undefined,
      endpoint: config['endpoint'] as string | undefined,
    }
  }

  async connect(): Promise<void> {
    const { region, accessKeyId, secretAccessKey, sessionToken, endpoint } = this.config
    this.rawClient = new DynamoDBClient({
      region,
      endpoint,
      ...(accessKeyId
        ? { credentials: { accessKeyId, secretAccessKey: secretAccessKey!, sessionToken } }
        : {}),
    })
    this.docClient = DynamoDBDocumentClient.from(this.rawClient)
    await this.rawClient.send(new ListTablesCommand({ Limit: 1 }))
  }

  async disconnect(): Promise<void> {
    this.rawClient?.destroy()
    this.rawClient = null
    this.docClient = null
    this.pkCache.clear()
  }

  private assertClients(): { rawClient: DynamoDBClient; docClient: DynamoDBDocumentClient } {
    if (!this.rawClient || !this.docClient)
      throw new Error('DynamodbBridge is not connected')
    return { rawClient: this.rawClient, docClient: this.docClient }
  }

  async listTargets(): Promise<string[]> {
    const { rawClient } = this.assertClients()
    const tables: string[] = []
    let lastKey: string | undefined
    do {
      const result = await rawClient.send(new ListTablesCommand({ ExclusiveStartTableName: lastKey }))
      tables.push(...(result.TableNames ?? []))
      lastKey = result.LastEvaluatedTableName
    } while (lastKey)
    return tables
  }

  private async getPrimaryKey(target: string): Promise<string> {
    const cached = this.pkCache.get(target)
    if (cached) return cached
    const { rawClient } = this.assertClients()
    const result = await rawClient.send(new DescribeTableCommand({ TableName: target }))
    const hashKey = result.Table?.KeySchema?.find(k => k.KeyType === 'HASH')
    if (!hashKey?.AttributeName)
      throw new Error(`Cannot detect primary key for "${target}"`)
    this.pkCache.set(target, hashKey.AttributeName)
    return hashKey.AttributeName
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    assertSupportedOps(options?.where, {
      logicalOps: DYNAMODB_LOGICAL_OPS,
      stringOps: DYNAMODB_STRING_OPS,
      bridge: DYNAMODB_BRIDGE_NAME,
      target,
    })
    if (options?.where && Object.keys(options.where).length > 0) {
      // Reuse query() — it already builds a FilterExpression from the
      // existing $eq/$gt/$in family, runs a Scan, and returns the
      // matching rows. Counting them here is honest given exactCount:false
      // (large tables would need a Scan with Select:COUNT + paginated
      // accumulation, which we defer to a future PR).
      const result = await this.query(target, { where: options.where })
      return result.rows.length
    }
    const { docClient } = this.assertClients()
    const result = await docClient.send(new ScanCommand({ TableName: target, Select: 'COUNT' }))
    return result.Count ?? 0
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const { rawClient, docClient } = this.assertClients()
    const limit = options?.limit ?? 1000
    const pk = await this.getPrimaryKey(target)

    const input: ScanCommandInput = {
      TableName: target,
      Limit: limit + 1,
      ...(options?.cursor ? { ExclusiveStartKey: JSON.parse(options.cursor) } : {}),
    }
    if (options?.fields?.length) {
      input.ProjectionExpression = options.fields.map((_, i) => `#f${i}`).join(', ')
      input.ExpressionAttributeNames = Object.fromEntries(
        options.fields.map((f, i) => [`#f${i}`, f]),
      )
    }

    const result = await docClient.send(new ScanCommand(input))
    const allRows = (result.Items ?? []) as BridgeRow[]
    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor =
      hasMore && result.LastEvaluatedKey
        ? JSON.stringify(result.LastEvaluatedKey)
        : hasMore
          ? JSON.stringify({ [pk]: rows[rows.length - 1]?.[pk] })
          : undefined

    const desc = await rawClient.send(new DescribeTableCommand({ TableName: target }))
    const total =
      desc.Table?.ItemCount !== undefined ? Number(desc.Table.ItemCount) : undefined

    return { rows, nextCursor, total }
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

  async query(target: string, opts: QueryOptions): Promise<QueryResult<BridgeRow>> {
    assertSupportedOps(opts.where, {
      logicalOps: DYNAMODB_LOGICAL_OPS,
      stringOps: DYNAMODB_STRING_OPS,
      bridge: DYNAMODB_BRIDGE_NAME,
      target,
    })
    const { docClient } = this.assertClients()
    const expNames: Record<string, string> = {}
    const expValues: Record<string, unknown> = {}
    const conditions: string[] = []
    let ni = 0
    let vi = 0

    if (opts.where) {
      for (const [field, value] of Object.entries(opts.where)) {
        const nk = `#n${ni++}`
        expNames[nk] = field
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          for (const [op, v] of Object.entries(value as Record<string, unknown>)) {
            const vk = `:v${vi++}`
            const opMap: Record<string, string> = {
              $eq: '=',
              $gt: '>',
              $gte: '>=',
              $lt: '<',
              $lte: '<=',
            }
            if (op in opMap) {
              conditions.push(`${nk} ${opMap[op]} ${vk}`)
              expValues[vk] = v
            } else if (op === '$in') {
              const vals = v as unknown[]
              const parts = vals.map(item => {
                const ik = `:v${vi++}`
                expValues[ik] = item
                return `${nk} = ${ik}`
              })
              conditions.push(`(${parts.join(' OR ')})`)
            } else {
              throw new UnsupportedOperatorError({
                op,
                bridge: DYNAMODB_BRIDGE_NAME,
                target,
              })
            }
          }
        } else {
          const vk = `:v${vi++}`
          expValues[vk] = value
          conditions.push(`${nk} = ${vk}`)
        }
      }
    }

    const input: ScanCommandInput = {
      TableName: target,
      ...(conditions.length
        ? {
            FilterExpression: conditions.join(' AND '),
            ExpressionAttributeNames: expNames,
            ExpressionAttributeValues: expValues,
          }
        : {}),
      // Limit applies before filter in Dynamo — over-fetch to allow for filtering
      ...(opts.limit ? { Limit: opts.limit * 10 } : {}),
    }

    const result = await docClient.send(new ScanCommand(input))
    let rows = (result.Items ?? []) as BridgeRow[]
    if (opts.offset) rows = rows.slice(opts.offset)
    if (opts.limit) rows = rows.slice(0, opts.limit)
    return { rows, total: result.ScannedCount }
  }

  /**
   * Aggregate via streaming reducer. DynamoDB has no native group-by;
   * the bridge applies `candidatesWhere` via `query()` (which translates
   * to a Scan with FilterExpression), then reduces in memory.
   */
  aggregateCapabilities(): BridgeAggregateCapabilities {
    return STREAMING_AGGREGATE_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    return streamingAggregate(this, opts)
  }
}
