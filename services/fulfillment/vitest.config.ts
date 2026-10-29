import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['src/**/*.test.ts'],
    environment: 'node',
    // Each file installs its own global tracer provider, so files must not
    // share a process: a parallel file would observe another file's spans and
    // the assertions about exact span counts would be meaningless.
    fileParallelism: false,
  },
});
