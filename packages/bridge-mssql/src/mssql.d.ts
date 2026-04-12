/**
 * Minimal ambient type shim for `mssql` v11.
 * mssql does not ship its own TypeScript declarations; this covers the
 * surface area used by MssqlBridge.
 */
declare module 'mssql' {
  export interface IResult<T = unknown> {
    recordset: T[]
    rowsAffected: number[]
  }

  export interface Request {
    input(name: string, value: unknown): this
    query<T = unknown>(sqlText: string): Promise<IResult<T>>
  }

  export interface ConnectionPool {
    request(): Request
    close(): Promise<void>
  }

  export interface PoolOpts {
    min?: number
    max?: number
  }

  export interface config {
    server: string
    port?: number
    user?: string
    password?: string
    database?: string
    options?: {
      encrypt?: boolean
      trustServerCertificate?: boolean
    }
    pool?: PoolOpts
  }

  export function connect(config: config): Promise<ConnectionPool>
}
