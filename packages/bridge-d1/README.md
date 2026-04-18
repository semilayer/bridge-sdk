<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-d1</strong>
</p>

---

# @semilayer/bridge-d1

SemiLayer bridge for d1.

## Install

```bash
npm install @semilayer/bridge-d1
```

## Usage

```ts
import { D1Bridge } from '@semilayer/bridge-d1'

const bridge = new D1Bridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `D1BridgeConfig` fields.

## License

MIT
