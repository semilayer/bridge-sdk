import type {
  Bridge,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
} from '@semilayer/core'

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
      rows = rows.filter((r) =>
        Object.entries(where).every(([key, val]) => r[key] === val),
      )
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
