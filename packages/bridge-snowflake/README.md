# @semilayer/bridge-snowflake

SemiLayer bridge for snowflake.

## Install

```bash
npm install @semilayer/bridge-snowflake
```

## Usage

```ts
import { SnowflakeBridge } from '@semilayer/bridge-snowflake'

const bridge = new SnowflakeBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `SnowflakeBridgeConfig` fields.

## License

MIT
