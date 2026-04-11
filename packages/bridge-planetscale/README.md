# @semilayer/bridge-planetscale

SemiLayer bridge for planetscale.

## Install

```bash
npm install @semilayer/bridge-planetscale
```

## Usage

```ts
import { PlanetscaleBridge } from '@semilayer/bridge-planetscale'

const bridge = new PlanetscaleBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `PlanetscaleBridgeConfig` fields.

## License

MIT
