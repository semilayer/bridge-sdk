/**
 * Test-harness entry point for `@semilayer/bridge-sdk`.
 *
 * This entry imports `vitest` and is therefore unsafe to load from
 * production code paths (it crashes at module evaluation when not
 * inside a vitest worker). Bridges that author compliance tests
 * import from here:
 *
 * ```ts
 * import { runAggregateCompliance, aggregateFixture } from '@semilayer/bridge-sdk/testing'
 * ```
 *
 * The main entry (`@semilayer/bridge-sdk`) is vitest-free so bridges
 * that depend on the SDK at runtime (every adapter that uses the
 * shared dialect or `streamingAggregate`) don't accidentally pull
 * vitest into their consumers' bundles.
 */
export { createBridgeTestSuite } from './test-suite.js'
export type { BridgeTestSuiteOptions } from './test-suite.js'

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
