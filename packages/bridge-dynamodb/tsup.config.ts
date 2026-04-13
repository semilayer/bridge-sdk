import { defineConfig } from 'tsup'

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm'],
  dts: true,
  clean: true,
  sourcemap: true,
  target: 'node22',
  external: ['@aws-sdk/client-dynamodb', '@aws-sdk/lib-dynamodb', '@semilayer/core', '@semilayer/bridge-sdk'],
})
