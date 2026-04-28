export type {
  Bridge,
  BridgeRow,
  BridgeConstructor,
  BridgeCapabilities,
  BatchReadOptions,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
} from '@semilayer/core'

export { DEFAULT_BRIDGE_CAPABILITIES, resolveBridgeCapabilities } from '@semilayer/core'

export { MockBridge } from './mock-bridge.js'
export { createBridgeTestSuite } from './test-suite.js'
export type { BridgeTestSuiteOptions } from './test-suite.js'

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
  MYSQL_DIALECT,
  SQLITE_DIALECT,
  MSSQL_DIALECT,
  CLICKHOUSE_DIALECT,
  BIGQUERY_DIALECT,
  SNOWFLAKE_DIALECT,
  ORACLE_DIALECT,
  DUCKDB_DIALECT,
  POSTGRES_FAMILY_CAPABILITIES,
  MYSQL_FAMILY_CAPABILITIES,
  SQLITE_FAMILY_CAPABILITIES,
  MSSQL_CAPABILITIES,
  CLICKHOUSE_CAPABILITIES,
  BIGQUERY_CAPABILITIES,
  SNOWFLAKE_CAPABILITIES,
  ORACLE_CAPABILITIES,
  DUCKDB_CAPABILITIES,
} from './sql-dialects.js'

export {
  runAggregateCompliance,
  aggregateFixture,
  fixtureToBridgeRows,
  collect as collectAggregateStream,
} from './aggregate-suite.js'
export type {
  AggregateSuiteOptions,
  AggregateFixtureRow,
} from './aggregate-suite.js'
