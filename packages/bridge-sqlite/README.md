<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-sqlite</strong>
</p>

---

# @semilayer/bridge-sqlite

SemiLayer bridge for sqlite.

## Install

```bash
npm install @semilayer/bridge-sqlite
```

## Usage

```ts
import { SqliteBridge } from '@semilayer/bridge-sqlite'

const bridge = new SqliteBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `SqliteBridgeConfig` fields.

## License

MIT
