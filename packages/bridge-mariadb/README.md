<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-mariadb</strong>
</p>

---

# @semilayer/bridge-mariadb

SemiLayer bridge for MariaDB.

## Install

```bash
npm install @semilayer/bridge-mariadb
```

## Usage

```ts
import { MariadbBridge } from '@semilayer/bridge-mariadb'

const bridge = new MariadbBridge({
  url: 'mariadb://user:pass@host:3306/db',
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `MariadbBridgeConfig` fields.

## License

MIT
