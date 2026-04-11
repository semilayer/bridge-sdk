# @semilayer/bridge-turso

SemiLayer bridge for turso.

## Install

```bash
npm install @semilayer/bridge-turso
```

## Usage

```ts
import { TursoBridge } from '@semilayer/bridge-turso'

const bridge = new TursoBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `TursoBridgeConfig` fields.

## License

MIT
