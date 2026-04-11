import type { BridgeConstructor, BridgeManifest } from '@semilayer/core'
import { PostgresBridge } from '@semilayer/bridge-postgres'

const BUILT_IN_BRIDGES: Record<string, BridgeConstructor> = {
  '@semilayer/bridge-postgres': PostgresBridge,
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
