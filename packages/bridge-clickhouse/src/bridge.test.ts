import { describe, it } from 'vitest'
// import { createBridgeTestSuite } from '@semilayer/bridge-sdk'
// import { ClickhouseBridge } from './bridge.js'

// Once the bridge is implemented, wire up the compliance suite:
//
// createBridgeTestSuite({
//   factory: () => new ClickhouseBridge({ /* config */ }),
//   seed: {
//     target: 'test_items',
//     rows: [
//       { id: 1, name: 'Alpha' },
//       { id: 2, name: 'Beta' },
//     ],
//     primaryKey: 'id',
//   },
//   beforeSeed: async (bridge) => {
//     // TODO: create the test_items table and insert seed rows
//   },
//   afterCleanup: async (bridge) => {
//     // TODO: drop the test_items table
//   },
// })

describe('ClickhouseBridge', () => {
  it.todo('implement bridge methods then wire up createBridgeTestSuite above')
})
