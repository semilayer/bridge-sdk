import type {
  Bridge,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
} from '@semilayer/core'

export interface SnowflakeBridgeConfig {
  // TODO: connection options (host, port, credentials, etc.)
}

export class SnowflakeBridge implements Bridge {
  constructor(private readonly config: SnowflakeBridgeConfig) {}

  async connect(): Promise<void> {
    // TODO: open connection / pool
    throw new Error('SnowflakeBridge.connect() not yet implemented')
  }

  async read(target: string, _options?: ReadOptions): Promise<ReadResult> {
    // TODO: paginated read with cursor
    throw new Error('SnowflakeBridge.read() not yet implemented')
  }

  async count(target: string): Promise<number> {
    // TODO: row count
    throw new Error('SnowflakeBridge.count() not yet implemented')
  }

  async query(
    target: string,
    _options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    // TODO: filtered query (where / orderBy / limit / offset / select)
    throw new Error('SnowflakeBridge.query() not yet implemented')
  }

  async disconnect(): Promise<void> {
    // TODO: close connection / pool
    throw new Error('SnowflakeBridge.disconnect() not yet implemented')
  }
}
