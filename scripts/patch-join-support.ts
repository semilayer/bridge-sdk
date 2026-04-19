// Ad-hoc patcher: adds batchRead + capabilities to every
// packages/bridge-*\/src\/bridge.ts that hasn't already been patched. Not
// meant to be run more than once — we keep it in the repo as a record of
// the mechanical change.
//
//   pnpm tsx scripts/patch-join-support.ts
import fs from 'node:fs'
import path from 'node:path'
import url from 'node:url'

const REPO_ROOT = path.dirname(path.dirname(url.fileURLToPath(import.meta.url)))
const PACKAGES_DIR = path.join(REPO_ROOT, 'packages')

const SKIP = new Set([
  'bridge-postgres', // already patched manually — different batchRead body (skips COUNT)
])

const CAPABILITIES_BLOCK = `  readonly capabilities: Partial<BridgeCapabilities> = {
    batchRead: true,
    wherePushdown: true,
    orderByPushdown: true,
    limitPushdown: true,
    selectProjection: true,
    nativeJoin: false,
    cursor: true,
    changedSince: true,
    perKeyLimit: false,
  }

`

const BATCH_READ_METHOD = `  async batchRead(
    target: string,
    options: BatchReadOptions,
  ): Promise<BridgeRow[]> {
    const result = await this.query(target, {
      where: options.where,
      select: options.select && options.select !== '*' ? options.select : undefined,
      orderBy: options.orderBy,
      limit: options.limit,
    })
    return result.rows
  }

`

function addToImportBlock(source: string): string {
  // Find the `import type { ... } from '@semilayer/core'` block and add the two
  // new symbols. All bridges have this import already.
  const re = /import\s+type\s+\{\s*([\s\S]*?)\s*\}\s+from\s+'@semilayer\/core'/
  const match = re.exec(source)
  if (!match) throw new Error(`No @semilayer/core type import found`)

  const inner = match[1]!
  const names = new Set(
    inner
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
  )
  names.add('BatchReadOptions')
  names.add('BridgeCapabilities')
  const sorted = Array.from(names).sort()
  const replacement = `import type {\n  ${sorted.join(',\n  ')},\n} from '@semilayer/core'`
  return source.replace(match[0], replacement)
}

function insertCapabilities(source: string): string {
  // Match `class XxxBridge implements Bridge {` + whatever follows (usually
  // blank line or field declarations). Insert capabilities immediately after
  // the opening brace.
  const re = /(export class \w+Bridge implements Bridge \{\n)/
  return source.replace(re, `$1${CAPABILITIES_BLOCK}`)
}

function insertBatchRead(source: string): string {
  // Insert BEFORE the first `async query(` method declaration. Every bridge
  // has one.
  const re = /(\n  async query\()/
  return source.replace(re, `\n${BATCH_READ_METHOD}  async query(`)
}

function patch(file: string): { touched: boolean; reason?: string } {
  const original = fs.readFileSync(file, 'utf8')
  if (original.includes('batchRead(')) return { touched: false, reason: 'already patched' }
  if (!/async query\(/.test(original)) {
    return { touched: false, reason: 'no async query() method — skipping, planner will see batchRead=false' }
  }
  let next = original
  next = addToImportBlock(next)
  next = insertCapabilities(next)
  next = insertBatchRead(next)
  fs.writeFileSync(file, next)
  return { touched: true }
}

function main() {
  const dirs = fs.readdirSync(PACKAGES_DIR).filter((d) => d.startsWith('bridge-'))
  for (const dir of dirs) {
    if (SKIP.has(dir)) {
      console.log(`skip  ${dir} (manual patch)`)
      continue
    }
    const file = path.join(PACKAGES_DIR, dir, 'src', 'bridge.ts')
    if (!fs.existsSync(file)) {
      console.log(`skip  ${dir} (no bridge.ts)`)
      continue
    }
    const result = patch(file)
    if (result.touched) console.log(`ok    ${dir}`)
    else console.log(`skip  ${dir} (${result.reason})`)
  }
}

main()
