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
