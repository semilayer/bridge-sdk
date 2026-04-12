# @semilayer/bridge-supabase

SemiLayer bridge for supabase.

## Install

```bash
npm install @semilayer/bridge-supabase
```

## Usage

```ts
import { SupabaseBridge } from '@semilayer/bridge-supabase'

const bridge = new SupabaseBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `SupabaseBridgeConfig` fields.

## License

MIT
