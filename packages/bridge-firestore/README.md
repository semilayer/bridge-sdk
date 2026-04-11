# @semilayer/bridge-firestore

SemiLayer bridge for firestore.

## Install

```bash
npm install @semilayer/bridge-firestore
```

## Usage

```ts
import { FirestoreBridge } from '@semilayer/bridge-firestore'

const bridge = new FirestoreBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `FirestoreBridgeConfig` fields.

## License

MIT
