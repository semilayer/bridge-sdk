import type { BridgeConstructor, BridgeManifest } from '@semilayer/core'
import { PostgresBridge } from '@semilayer/bridge-postgres'
import { MysqlBridge } from '@semilayer/bridge-mysql'
import { SqliteBridge } from '@semilayer/bridge-sqlite'
import { MssqlBridge } from '@semilayer/bridge-mssql'
import { CockroachdbBridge } from '@semilayer/bridge-cockroachdb'
import { NeonBridge } from '@semilayer/bridge-neon'
import { TursoBridge } from '@semilayer/bridge-turso'
import { PlanetscaleBridge } from '@semilayer/bridge-planetscale'
import { D1Bridge } from '@semilayer/bridge-d1'
import { SupabaseBridge } from '@semilayer/bridge-supabase'
import { MongodbBridge } from '@semilayer/bridge-mongodb'
import { FirestoreBridge } from '@semilayer/bridge-firestore'
import { RedisBridge } from '@semilayer/bridge-redis'
import { UpstashBridge } from '@semilayer/bridge-upstash'
import { DynamodbBridge } from '@semilayer/bridge-dynamodb'
import { ElasticsearchBridge } from '@semilayer/bridge-elasticsearch'
import { CassandraBridge } from '@semilayer/bridge-cassandra'
import { ClickhouseBridge } from '@semilayer/bridge-clickhouse'
import { BigqueryBridge } from '@semilayer/bridge-bigquery'
import { DuckdbBridge } from '@semilayer/bridge-duckdb'
import { SnowflakeBridge } from '@semilayer/bridge-snowflake'
import { MariadbBridge } from '@semilayer/bridge-mariadb'
import { OracleBridge } from '@semilayer/bridge-oracle'

const BUILT_IN_BRIDGES: Record<string, BridgeConstructor> = {
  '@semilayer/bridge-postgres': PostgresBridge,
  '@semilayer/bridge-mysql': MysqlBridge,
  '@semilayer/bridge-sqlite': SqliteBridge,
  '@semilayer/bridge-mssql': MssqlBridge,
  '@semilayer/bridge-cockroachdb': CockroachdbBridge,
  '@semilayer/bridge-neon': NeonBridge,
  '@semilayer/bridge-turso': TursoBridge,
  '@semilayer/bridge-planetscale': PlanetscaleBridge,
  '@semilayer/bridge-d1': D1Bridge,
  '@semilayer/bridge-supabase': SupabaseBridge,
  '@semilayer/bridge-mongodb': MongodbBridge,
  '@semilayer/bridge-firestore': FirestoreBridge,
  '@semilayer/bridge-redis': RedisBridge,
  '@semilayer/bridge-upstash': UpstashBridge,
  '@semilayer/bridge-dynamodb': DynamodbBridge,
  '@semilayer/bridge-elasticsearch': ElasticsearchBridge,
  '@semilayer/bridge-cassandra': CassandraBridge,
  '@semilayer/bridge-clickhouse': ClickhouseBridge,
  '@semilayer/bridge-bigquery': BigqueryBridge,
  '@semilayer/bridge-duckdb': DuckdbBridge,
  '@semilayer/bridge-snowflake': SnowflakeBridge,
  '@semilayer/bridge-mariadb': MariadbBridge,
  '@semilayer/bridge-oracle': OracleBridge,
}

let customBridges: Record<string, BridgeConstructor> = {}

/**
 * Register a custom bridge at runtime.
 *
 * Used to add community or enterprise bridges to the resolver without
 * modifying the built-in registry. Custom bridges take priority over
 * built-in bridges with the same name.
 *
 * @example
 * ```ts
 * import { registerBridge } from '@semilayer/bridge-resolver'
 * import { MySQLBridge } from '@community/bridge-mysql'
 *
 * registerBridge('@community/bridge-mysql', MySQLBridge)
 * ```
 */
export function registerBridge(name: string, ctor: BridgeConstructor): void {
  customBridges[name] = ctor
}

/**
 * Clear all custom bridges (useful in tests).
 */
export function clearCustomBridges(): void {
  customBridges = {}
}

/**
 * Resolve a bridge constructor by name.
 * Checks custom bridges first, then built-in registry.
 *
 * @throws if the bridge is not registered
 */
export function resolveBridge(name: string): BridgeConstructor {
  const Ctor = customBridges[name] ?? BUILT_IN_BRIDGES[name]
  if (!Ctor) {
    const available = [
      ...Object.keys(BUILT_IN_BRIDGES),
      ...Object.keys(customBridges),
    ].join(', ')
    throw new Error(`Unknown bridge: "${name}". Available: ${available}`)
  }
  return Ctor
}

/**
 * List all available bridge names (built-in + custom).
 */
export function listBridges(): string[] {
  return [...new Set([...Object.keys(BUILT_IN_BRIDGES), ...Object.keys(customBridges)])]
}

/**
 * Return the manifest for a specific bridge, or undefined if the bridge
 * has not declared one yet.
 */
export function getManifest(name: string): BridgeManifest | undefined {
  const Ctor = customBridges[name] ?? BUILT_IN_BRIDGES[name]
  return Ctor?.manifest
}

/**
 * Return manifests for every registered bridge that has declared one.
 * Bridges without a static `manifest` property are silently skipped —
 * they remain usable via `resolveBridge`, but the console/CLI cannot
 * render a dynamic config form for them.
 */
export function listManifests(): BridgeManifest[] {
  const all = { ...BUILT_IN_BRIDGES, ...customBridges }
  return Object.values(all)
    .map((Ctor) => Ctor.manifest)
    .filter((m): m is BridgeManifest => m !== undefined)
}
