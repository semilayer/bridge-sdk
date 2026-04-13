/**
 * Ambient type declarations for the `oracledb` npm package.
 * The oracledb package ships without bundled TypeScript types,
 * so we declare the subset of the API surface that this bridge uses.
 */
declare module 'oracledb' {
  export const OUT_FORMAT_OBJECT: number

  export interface PoolAttributes {
    user?: string
    password?: string
    connectString?: string
    poolMax?: number
    poolMin?: number
    [key: string]: unknown
  }

  export interface ExecuteOptions {
    outFormat?: number
    [key: string]: unknown
  }

  export interface Result<T = unknown> {
    rows?: T[]
    rowsAffected?: number
    [key: string]: unknown
  }

  export interface Connection {
    execute(
      sql: string,
      binds?: unknown[],
      options?: ExecuteOptions,
    ): Promise<Result>
    execute(sql: string): Promise<Result>
    close(): Promise<void>
  }

  export interface Pool {
    getConnection(): Promise<Connection>
    close(drainTime?: number): Promise<void>
  }

  export function createPool(config: PoolAttributes): Promise<Pool>

  const oracledb: {
    OUT_FORMAT_OBJECT: number
    createPool(config: PoolAttributes): Promise<Pool>
  }
  export default oracledb
}
