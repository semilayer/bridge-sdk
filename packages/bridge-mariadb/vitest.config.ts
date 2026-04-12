import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // Exclude integration tests from the standard unit-test run.
    // Run them separately with: pnpm test:integration
    exclude: ['src/**/*.integration.test.ts', '**/node_modules/**'],
  },
})
