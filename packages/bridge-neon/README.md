<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-neon</strong>
</p>

---

# @semilayer/bridge-neon

SemiLayer bridge for neon.

## Install

```bash
npm install @semilayer/bridge-neon
```

## Usage

```ts
import { NeonBridge } from '@semilayer/bridge-neon'

const bridge = new NeonBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `NeonBridgeConfig` fields.

## License

MIT
