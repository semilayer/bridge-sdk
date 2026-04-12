# @semilayer/bridge-mongodb

SemiLayer bridge for mongodb.

## Install

```bash
npm install @semilayer/bridge-mongodb
```

## Usage

```ts
import { MongodbBridge } from '@semilayer/bridge-mongodb'

const bridge = new MongodbBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `MongodbBridgeConfig` fields.

## License

MIT
