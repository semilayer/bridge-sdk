import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['src/**/*.integration.test.ts'],
    // DB operations can be slow — give each test and hook generous time.
    testTimeout: 30_000,
    hookTimeout: 60_000,
  },
})
