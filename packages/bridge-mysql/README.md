<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-mysql</strong>
</p>

---

# @semilayer/bridge-mysql

SemiLayer bridge for mysql.

## Install

```bash
npm install @semilayer/bridge-mysql
```

## Usage

```ts
import { MysqlBridge } from '@semilayer/bridge-mysql'

const bridge = new MysqlBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `MysqlBridgeConfig` fields.

## License

MIT
