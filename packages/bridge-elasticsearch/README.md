<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-elasticsearch</strong>
</p>

---

# @semilayer/bridge-elasticsearch

SemiLayer bridge for elasticsearch.

## Install

```bash
npm install @semilayer/bridge-elasticsearch
```

## Usage

```ts
import { ElasticsearchBridge } from '@semilayer/bridge-elasticsearch'

const bridge = new ElasticsearchBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `ElasticsearchBridgeConfig` fields.

## License

MIT
