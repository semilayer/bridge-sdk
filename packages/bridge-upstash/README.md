# @semilayer/bridge-upstash

SemiLayer bridge for upstash.

## Install

```bash
npm install @semilayer/bridge-upstash
```

## Usage

```ts
import { UpstashBridge } from '@semilayer/bridge-upstash'

const bridge = new UpstashBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `UpstashBridgeConfig` fields.

## License

MIT
