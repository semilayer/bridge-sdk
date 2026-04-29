export type {
  Bridge,
  BridgeRow,
  BridgeConstructor,
  BridgeCapabilities,
  BatchReadOptions,
  CountOptions,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
  WhereClause,
  FieldOps,
  WhereLogicalOp,
  WhereStringOp,
} from '@semilayer/core'

export {
  DEFAULT_BRIDGE_CAPABILITIES,
  resolveBridgeCapabilities,
  WHERE_LOGICAL_OPS,
  WHERE_STRING_OPS,
} from '@semilayer/core'

export { MockBridge } from './mock-bridge.js'
export { UnsupportedOperatorError } from './errors.js'

// Test-harness exports (createBridgeTestSuite, runAggregateCompliance,
// aggregateFixture, etc.) live in a separate entry point —
// `@semilayer/bridge-sdk/testing` — to keep `vitest` out of the runtime
// bundle path. Importing vitest from a non-test runtime crashes at
// module evaluation; bridges depend on this entry at runtime for
// dialect helpers + types so it must stay vitest-free.

// ─── Aggregate surface ───────────────────────────────────────────────
export type {
  AnalyzeMeasureAgg,
  AnalyzeTimeBucket,
  DimensionBucket,
  AggregateDimension,
  AggregateMeasure,
  AggregateOptions,
  AggregateRow,
  BridgeAggregateCapabilities,
  BridgeExecutionContext,
} from './aggregate.js'

export {
  DEFAULT_AGGREGATE_CAPABILITIES,
  STREAMING_AGGREGATE_CAPABILITIES,
} from './aggregate.js'

export {
  streamingAggregate,
  bucketValue,
  bucketize,
  formatTimeBucket,
  rowMatches,
} from './streaming-aggregate.js'
export type { StreamingAggregateOptions } from './streaming-aggregate.js'

export {
  buildAggregateSql,
  decodeAggregateRow,
  stitchTopK,
  executeAggregateQueries,
} from './sql-aggregate.js'
export type {
  SqlAggregateDialect,
  BuiltAggregateSql,
} from './sql-aggregate.js'

export {
  POSTGRES_DIALECT,
  COCKROACH_DIALECT,
  MYSQL_DIALECT,
  SQLITE_DIALECT,
  MSSQL_DIALECT,
  CLICKHOUSE_DIALECT,
  BIGQUERY_DIALECT,
  SNOWFLAKE_DIALECT,
  ORACLE_DIALECT,
  DUCKDB_DIALECT,
  POSTGRES_FAMILY_CAPABILITIES,
  COCKROACH_CAPABILITIES,
  MYSQL_FAMILY_CAPABILITIES,
  SQLITE_FAMILY_CAPABILITIES,
  MSSQL_CAPABILITIES,
  CLICKHOUSE_CAPABILITIES,
  BIGQUERY_CAPABILITIES,
  SNOWFLAKE_CAPABILITIES,
  ORACLE_CAPABILITIES,
  DUCKDB_CAPABILITIES,
} from './sql-dialects.js'

