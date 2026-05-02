/**
 * Postgres dialect for the shared SQL aggregate builder. The base
 * dialect lives in `@semilayer/bridge-sdk` (`POSTGRES_DIALECT`) so
 * every Postgres-protocol bridge (cockroachdb, neon, supabase) can
 * reuse it.
 *
 * ### Geospatial pushdown (opt-in)
 *
 * `enablePostgis` composes `postgisGeohashExpr` + `postgisDecodeGeoField`
 * into the dialect and flips the `geohashBucket` / `geoBucket` caps to
 * `true`. Callers must have the PostGIS extension installed
 * (`CREATE EXTENSION postgis`); the bridge does not install it for
 * you. Default is `false` so naïve callers see the same SQL surface
 * as before. The bridge does not advertise `h3Bucket` — the planner
 * upstream falls back to streaming reduce when an H3 dim is requested
 * against a non-h3 bridge.
 */
import {
  buildAggregateSql,
  POSTGRES_DIALECT,
  POSTGRES_FAMILY_CAPABILITIES,
  postgisGeohashExpr,
  postgisDecodeGeoField,
  type AggregateOptions,
  type BridgeAggregateCapabilities,
  type BuiltAggregateSql,
  type SqlAggregateDialect,
} from '@semilayer/bridge-sdk'

export interface PostgresAggregateOptions {
  /**
   * Enable geohash pushdown via PostGIS. Requires the `postgis`
   * extension on the connected database. Default `false` — leaving
   * this off keeps the bridge dependency-free at the SQL layer.
   */
  enablePostgis?: boolean
}

export const POSTGRES_AGGREGATE_CAPABILITIES: BridgeAggregateCapabilities =
  POSTGRES_FAMILY_CAPABILITIES

export function postgresAggregateCapabilities(
  options?: PostgresAggregateOptions,
): BridgeAggregateCapabilities {
  if (!options?.enablePostgis) return POSTGRES_FAMILY_CAPABILITIES
  return {
    ...POSTGRES_FAMILY_CAPABILITIES,
    geoBucket: true,
    geohashBucket: true,
  }
}

export function postgresAggregateDialect(
  options?: PostgresAggregateOptions,
): SqlAggregateDialect {
  if (!options?.enablePostgis) return POSTGRES_DIALECT
  return {
    ...POSTGRES_DIALECT,
    geohashExpr: postgisGeohashExpr,
    decodeGeoField: postgisDecodeGeoField,
  }
}

export function buildPostgresAggregate(
  opts: AggregateOptions,
  options?: PostgresAggregateOptions,
): BuiltAggregateSql {
  return buildAggregateSql(opts, postgresAggregateDialect(options))
}
