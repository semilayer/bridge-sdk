/**
 * Postgres dialect for the shared SQL aggregate builder. The actual
 * dialect object lives in `@semilayer/bridge-sdk` (`POSTGRES_DIALECT`)
 * so every Postgres-protocol bridge (cockroachdb, neon, supabase) can
 * reuse it. We re-export here for callers who want a stable per-bridge
 * import path.
 */
import {
  buildAggregateSql,
  POSTGRES_DIALECT,
  POSTGRES_FAMILY_CAPABILITIES,
  type AggregateOptions,
  type BridgeAggregateCapabilities,
  type BuiltAggregateSql,
  type SqlAggregateDialect,
} from '@semilayer/bridge-sdk'

export const POSTGRES_AGGREGATE_CAPABILITIES: BridgeAggregateCapabilities =
  POSTGRES_FAMILY_CAPABILITIES

export const postgresAggregateDialect: SqlAggregateDialect = POSTGRES_DIALECT

export function buildPostgresAggregate(opts: AggregateOptions): BuiltAggregateSql {
  return buildAggregateSql(opts, POSTGRES_DIALECT)
}
