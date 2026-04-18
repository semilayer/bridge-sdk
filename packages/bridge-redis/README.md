<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-redis</strong>
</p>

---

# @semilayer/bridge-redis

SemiLayer bridge for redis.

## Install

```bash
npm install @semilayer/bridge-redis
```

## Usage

```ts
import { RedisBridge } from '@semilayer/bridge-redis'

const bridge = new RedisBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `RedisBridgeConfig` fields.

## License

MIT
