import type {
  Bridge,
  BridgeRow,
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
import { streamingAggregate } from './streaming-aggregate.js'

export class MockBridge implements Bridge {
  private data = new Map<string, BridgeRow[]>()
  private connected = false

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

    const cursorIndex = options?.cursor
      ? Number(options.cursor)
      : 0
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

  async count(target: string): Promise<number> {
    this.assertConnected()
    return this.getRows(target).length
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
      const where = options.where
      rows = rows.filter((r) => mockMatches(r, where))
    }

    const total = rows.length

    if (options.orderBy) {
      const clauses = Array.isArray(options.orderBy)
        ? options.orderBy
        : [options.orderBy]
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

/**
 * Tiny operator interpreter that mirrors what the streaming reducer
 * exposes — kept private here so MockBridge.query() can answer `$in`,
 * range, and shorthand-equality predicates that the compliance suite
 * exercises through `streamingAggregate`'s pre-filter path.
 */
function mockMatches(row: BridgeRow, where: Record<string, unknown>): boolean {
  for (const [field, expected] of Object.entries(where)) {
    if (field === '$and' && Array.isArray(expected)) {
      if (!(expected as Array<Record<string, unknown>>).every((s) => mockMatches(row, s))) return false
      continue
    }
    if (field === '$or' && Array.isArray(expected)) {
      if (!(expected as Array<Record<string, unknown>>).some((s) => mockMatches(row, s))) return false
      continue
    }
    const actual = row[field]
    if (expected !== null && typeof expected === 'object' && !Array.isArray(expected) && !(expected instanceof Date)) {
      const ops = expected as Record<string, unknown>
      for (const [op, exp] of Object.entries(ops)) {
        if (!evalMockOp(actual, op, exp)) return false
      }
    } else if (Array.isArray(expected)) {
      if (!expected.includes(actual as never)) return false
    } else {
      if (actual !== expected) return false
    }
  }
  return true
}

function evalMockOp(actual: unknown, op: string, expected: unknown): boolean {
  switch (op) {
    case '$eq':
      return actual === expected
    case '$ne':
      return actual !== expected
    case '$gt':
      return cmpVal(actual, expected) > 0
    case '$gte':
      return cmpVal(actual, expected) >= 0
    case '$lt':
      return cmpVal(actual, expected) < 0
    case '$lte':
      return cmpVal(actual, expected) <= 0
    case '$in':
      return Array.isArray(expected) && expected.includes(actual as never)
    case '$nin':
      return Array.isArray(expected) && !expected.includes(actual as never)
    default:
      return false
  }
}

function cmpVal(a: unknown, b: unknown): number {
  if (typeof a === 'number' && typeof b === 'number') return a - b
  if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0
  if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime()
  return 0
}
