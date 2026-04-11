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
  Bridge,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
} from '@semilayer/core'

export interface DynamodbBridgeConfig {
  region: string
  accessKeyId?: string
  secretAccessKey?: string
  sessionToken?: string
  endpoint?: string
}

export class DynamodbBridge implements Bridge {
  private rawClient: DynamoDBClient | null = null
  private docClient: DynamoDBDocumentClient | null = null
  private pkCache = new Map<string, string>()
  private config: DynamodbBridgeConfig

  constructor(config: Record<string, unknown>) {
    const region = config['region'] as string | undefined
    if (!region || typeof region !== 'string')
      throw new Error('DynamodbBridge requires a "region" config string')
    this.config = {
      region,
      accessKeyId: config['accessKeyId'] as string | undefined,
      secretAccessKey: config['secretAccessKey'] as string | undefined,
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

  async count(target: string): Promise<number> {
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

  async query(target: string, opts: QueryOptions): Promise<QueryResult<BridgeRow>> {
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
              throw new Error(`Unknown operator "${op}"`)
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
}
