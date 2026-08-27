import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: { timeout: 8_000 },
  use: {
    baseURL: "http://127.0.0.1:4173",
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "pnpm exec tsx e2e/mock-server.ts",
    url: "http://127.0.0.1:4173/healthz",
    reuseExistingServer: true,
  },
});
