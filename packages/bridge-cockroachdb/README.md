<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-cockroachdb</strong>
</p>

---

# @semilayer/bridge-cockroachdb

SemiLayer bridge for cockroachdb.

## Install

```bash
npm install @semilayer/bridge-cockroachdb
```

## Usage

```ts
import { CockroachdbBridge } from '@semilayer/bridge-cockroachdb'

const bridge = new CockroachdbBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `CockroachdbBridgeConfig` fields.

## License

MIT
