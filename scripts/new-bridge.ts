#!/usr/bin/env tsx
/**
 * Scaffold a new bridge package from templates/bridge/.
 *
 * Usage:
 *   pnpm new-bridge <name>
 *
 * Example:
 *   pnpm new-bridge mysql        → creates packages/bridge-mysql/
 */
import { readFileSync, writeFileSync, mkdirSync, readdirSync, statSync, existsSync } from 'node:fs'
import { join, dirname, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..')
const TEMPLATE_DIR = join(ROOT, 'templates', 'bridge')
const PACKAGES_DIR = join(ROOT, 'packages')
const MANIFEST_PATH = join(ROOT, '.release-please-manifest.json')
const CONFIG_PATH = join(ROOT, 'release-please-config.json')

function fail(msg: string): never {
  console.error(`\u2717 ${msg}`)
  process.exit(1)
}

function toPascalCase(name: string): string {
  return name
    .split(/[-_]/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('')
}

function substitute(content: string, vars: Record<string, string>): string {
  return content.replace(/\{\{(\w+)\}\}/g, (_, key) => {
    if (!(key in vars)) throw new Error(`Unknown template var: ${key}`)
    return vars[key]!
  })
}

function copyTemplateTree(srcDir: string, destDir: string, vars: Record<string, string>): void {
  for (const entry of readdirSync(srcDir)) {
    const srcPath = join(srcDir, entry)
    const stat = statSync(srcPath)

    if (stat.isDirectory()) {
      const subDest = join(destDir, entry)
      mkdirSync(subDest, { recursive: true })
      copyTemplateTree(srcPath, subDest, vars)
      continue
    }

    // Strip .tmpl suffix if present
    const destName = entry.endsWith('.tmpl') ? entry.slice(0, -'.tmpl'.length) : entry
    const destPath = join(destDir, destName)
    const content = readFileSync(srcPath, 'utf-8')
    writeFileSync(destPath, substitute(content, vars))
  }
}

function main(): void {
  const rawName = process.argv[2]
  if (!rawName) fail('Usage: pnpm new-bridge <name>  (e.g., pnpm new-bridge mysql)')

  const name = rawName.toLowerCase().replace(/^bridge-/, '')

  if (!/^[a-z][a-z0-9-]*$/.test(name)) {
    fail(`Invalid bridge name "${name}" — must be lowercase letters/digits/hyphens, starting with a letter`)
  }

  const packageDir = join(PACKAGES_DIR, `bridge-${name}`)
  if (existsSync(packageDir)) {
    fail(`packages/bridge-${name}/ already exists`)
  }

  const vars = {
    NAME: name,
    NAME_PASCAL: toPascalCase(name),
  }

  console.log(`\u2192 Scaffolding packages/bridge-${name}/ from templates/bridge/`)
  mkdirSync(packageDir, { recursive: true })
  copyTemplateTree(TEMPLATE_DIR, packageDir, vars)

  // Update .release-please-manifest.json
  const manifest = JSON.parse(readFileSync(MANIFEST_PATH, 'utf-8'))
  manifest[`packages/bridge-${name}`] = '0.0.0'
  writeFileSync(MANIFEST_PATH, JSON.stringify(manifest, null, 2) + '\n')

  // Update release-please-config.json packages map
  const config = JSON.parse(readFileSync(CONFIG_PATH, 'utf-8'))
  config.packages[`packages/bridge-${name}`] = {
    'package-name': `@semilayer/bridge-${name}`,
  }
  writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2) + '\n')

  console.log(`\u2713 Created packages/bridge-${name}/`)
  console.log(`\u2713 Registered in .release-please-manifest.json`)
  console.log(`\u2713 Registered in release-please-config.json`)
  console.log()
  console.log('Next steps:')
  console.log(`  1. cd packages/bridge-${name}`)
  console.log(`  2. Implement Bridge methods in src/bridge.ts`)
  console.log(`  3. Wire up createBridgeTestSuite() in src/bridge.test.ts`)
  console.log(`  4. From the repo root: pnpm install && pnpm turbo build test --filter @semilayer/bridge-${name}`)
  console.log(`  5. Register your bridge in packages/bridge-resolver/src/index.ts BUILT_IN_BRIDGES`)
  console.log(`  6. Open a PR with a 'feat:' commit — release-please will publish on merge`)
}

main()
