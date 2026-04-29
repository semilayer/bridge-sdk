import type {
  Bridge,
  BridgeCapabilities,
  BridgeRow,
  CountOptions,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
} from '@semilayer/core'
import {
  STREAMING_AGGREGATE_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
} from './aggregate.js'
import { rowMatches, streamingAggregate } from './streaming-aggregate.js'

/**
 * Reference bridge implementation backed by an in-memory map. Used by the
 * compliance suite as an oracle: any operator declared as supported is
 * evaluated against the same `rowMatches` predicate that
 * `streamingAggregate` uses, so MockBridge results are exactly what every
 * bridge claiming the same capability should produce.
 */
export class MockBridge implements Bridge {
  private data = new Map<string, BridgeRow[]>()
  private connected = false

  /**
   * Reference implementation declares every capability the SDK can verify
   * — logical combinators, all four string operators, exact counts. Real
   * bridges declare a subset honestly; the compliance suite gates each
   * test block on what the bridge under test claims.
   */
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
    whereLogicalOps: ['or', 'and', 'not'],
    whereStringOps: ['ilike', 'contains', 'startsWith', 'endsWith'],
    exactCount: true,
  }

  seed(target: string, rows: BridgeRow[]): void {
    this.data.set(target, [...rows])
  }

  async connect(): Promise<void> {
    this.connected = true
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    this.assertConnected()
    const allRows = this.getRows(target)

    let rows = allRows

    if (options?.changedSince) {
      const since = options.changedSince.getTime()
      rows = rows.filter((r) => {
        const updatedAt = r['updated_at']
        return updatedAt instanceof Date && updatedAt.getTime() > since
      })
    }

    if (options?.fields) {
      const fields = options.fields
      rows = rows.map((r) => {
        const picked: BridgeRow = {}
        for (const f of fields) {
          if (f in r) picked[f] = r[f]
        }
        return picked
      })
    }

    const cursorIndex = options?.cursor ? Number(options.cursor) : 0
    const limit = options?.limit ?? rows.length

    const page = rows.slice(cursorIndex, cursorIndex + limit)
    const nextIndex = cursorIndex + limit
    const hasMore = nextIndex < rows.length

    return {
      rows: page,
      nextCursor: hasMore ? String(nextIndex) : undefined,
      total: allRows.length,
    }
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    this.assertConnected()
    const rows = this.getRows(target)
    if (!options?.where || Object.keys(options.where).length === 0) {
      return rows.length
    }
    const where = options.where as Record<string, unknown>
    let n = 0
    for (const r of rows) if (rowMatches(r, where)) n++
    return n
  }

  async disconnect(): Promise<void> {
    this.connected = false
  }

  async query(
    target: string,
    options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    this.assertConnected()
    let rows = this.getRows(target)

    if (options.where) {
      const where = options.where as Record<string, unknown>
      rows = rows.filter((r) => rowMatches(r, where))
    }

    const total = rows.length

    if (options.orderBy) {
      const clauses = Array.isArray(options.orderBy) ? options.orderBy : [options.orderBy]
      rows = [...rows].sort((a, b) => {
        for (const clause of clauses) {
          const av = a[clause.field]
          const bv = b[clause.field]
          if (av === bv) continue
          const dir = clause.dir === 'desc' ? -1 : 1
          if (av == null) return dir
          if (bv == null) return -dir
          return av < bv ? -dir : dir
        }
        return 0
      })
    }

    if (options.offset) {
      rows = rows.slice(options.offset)
    }

    if (options.limit) {
      rows = rows.slice(0, options.limit)
    }

    if (options.select) {
      const fields = options.select
      rows = rows.map((r) => {
        const picked: BridgeRow = {}
        for (const f of fields) {
          if (f in r) picked[f] = r[f]
        }
        return picked
      })
    }

    return { rows, total }
  }

  /**
   * Aggregate via the shared streaming reducer. MockBridge declares full
   * `STREAMING_AGGREGATE_CAPABILITIES` so the compliance suite exercises
   * the entire matrix against the fallback path.
   */
  aggregateCapabilities(): BridgeAggregateCapabilities {
    return STREAMING_AGGREGATE_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    this.assertConnected()
    return streamingAggregate(this, opts)
  }

  private assertConnected(): void {
    if (!this.connected) {
      throw new Error('MockBridge is not connected')
    }
  }

  private getRows(target: string): BridgeRow[] {
    const rows = this.data.get(target)
    if (!rows) throw new Error(`MockBridge: target "${target}" not seeded`)
    return rows
  }
}
