<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-oracle</strong>
</p>

---

# @semilayer/bridge-oracle

SemiLayer bridge for Oracle Database.

## Install

```bash
npm install @semilayer/bridge-oracle
```

## Usage

```ts
import { OracleBridge } from '@semilayer/bridge-oracle'

const bridge = new OracleBridge({
  connectString: 'host:1521/service',
  user: 'scott',
  password: 'tiger',
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `OracleBridgeConfig` fields.

## License

MIT
