// Compute the list of release-please paths whose manifest version is
// ahead of what's published on npm. The Release Please workflow uses
// the output to drive the publish job's matrix.
//
// Stdout: a JSON array of paths (relative to the repo root). When
// nothing is ahead, the output is `[]` and the publish job is skipped.

import { readFileSync } from 'node:fs'
import { execSync } from 'node:child_process'

const manifest = JSON.parse(readFileSync('.release-please-manifest.json', 'utf8'))

const cmp = (a, b) => {
  const [as, bs] = [a, b].map((v) => v.split('.').map(Number))
  for (let i = 0; i < 3; i++) if (as[i] !== bs[i]) return as[i] - bs[i]
  return 0
}

const out = []
for (const [path, version] of Object.entries(manifest)) {
  const pkg = JSON.parse(readFileSync(`${path}/package.json`, 'utf8'))
  let published = '0.0.0'
  try {
    published =
      execSync(`npm view ${pkg.name} version --silent`, { encoding: 'utf8' }).trim() || '0.0.0'
  } catch {
    // Package never published — treat any manifest version as new.
  }
  process.stderr.write(`${pkg.name} manifest=${version} npm=${published}\n`)
  if (cmp(version, published) > 0) out.push(path)
}

process.stdout.write(JSON.stringify(out))
