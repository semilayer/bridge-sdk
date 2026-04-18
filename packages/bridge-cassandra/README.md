<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-cassandra</strong>
</p>

---

# @semilayer/bridge-cassandra

SemiLayer bridge for cassandra.

## Install

```bash
npm install @semilayer/bridge-cassandra
```

## Usage

```ts
import { CassandraBridge } from '@semilayer/bridge-cassandra'

const bridge = new CassandraBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `CassandraBridgeConfig` fields.

## License

MIT
