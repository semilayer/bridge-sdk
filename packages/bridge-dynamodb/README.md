<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-dynamodb</strong>
</p>

---

# @semilayer/bridge-dynamodb

SemiLayer bridge for dynamodb.

## Install

```bash
npm install @semilayer/bridge-dynamodb
```

## Usage

```ts
import { DynamodbBridge } from '@semilayer/bridge-dynamodb'

const bridge = new DynamodbBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `DynamodbBridgeConfig` fields.

## License

MIT
