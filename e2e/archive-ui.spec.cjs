const { test, expect } = require("@playwright/test");

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem(
      "cli-proxy-auth",
      JSON.stringify({
        state: {
          apiBase: "http://127.0.0.1:4173",
          managementKey: "mock-management-key",
        },
      }),
    );
  });
  await page.goto("/#/sessions");
  await expect(page.locator(".key-pill").first()).toHaveText("长文本测试 Key");
});

test("long previews remain lightweight while turn details expose full content", async ({
  page,
}) => {
  await expect(page.getByText("项目与代码库")).toBeVisible();
  await expect(page.locator(".sessions-panel .conversation-meta")).toContainText(
    "mock-long-project",
  );

  await page.locator("tbody tr").first().click();
  await expect(page.locator(".turn-card")).toBeVisible();
  await expect(page.locator(".turn-card")).not.toContainText(
    "END-OF-COMPLETE-USER-COMMAND",
  );

  await page.locator(".turn-card").click();
  await expect(page).toHaveURL(/\/turns\/mock-turn-long-content$/);
  const primary = page.locator("#turnPrimary");
  await expect(primary).not.toContainText("END-OF-COMPLETE-USER-COMMAND");
  await primary
    .getByRole("button", { name: "展开完整内容" })
    .first()
    .click();
  await expect(primary).toContainText("END-OF-COMPLETE-USER-COMMAND");

  await page.locator(".process-step").first().click();
  await expect(page.getByText("shell_command · 查看完整调用")).toBeVisible();
  await page.getByText("shell_command · 查看完整调用").click();
  await expect(page).toHaveURL(/\/tools\/call-long-output$/);
  await expect(page.locator("#subDetail")).toContainText(
    "END-OF-LONG-TOOL-OUTPUT",
  );
});

test("system instructions and diagnostics are routed and loaded on demand", async ({
  page,
}) => {
  await page.locator("tbody tr").first().click();
  await page.locator(".turn-card").click();
  await page.locator(".process-step").first().click();
  await page.getByRole("button", { name: "原始请求/响应（排障）" }).click();
  await expect(page).toHaveURL(/\/requests\/mock-request-long-content$/);

  await page
    .getByRole("button", { name: /展开系统指令.*17730 字符/ })
    .click();
  await expect(page).toHaveURL(/\/raw\/instructions$/);
  await expect(page.locator("#subDetail")).toContainText(
    "END-OF-SYSTEM-INSTRUCTIONS",
  );
  await page.getByRole("button", { name: /返回本次请求/ }).click();
  await expect(page).toHaveURL(/\/requests\/mock-request-long-content$/);
  await page.getByRole("button", { name: "查看并复制" }).first().click();
  await expect(page).toHaveURL(/\/raw\/request$/);
  await expect(page.locator("#subDetail")).toContainText(
    "END-OF-COMPLETE-USER-COMMAND",
  );
});

test("session export exposes a reusable server-streamed JSONL download", async ({
  page,
  request,
}) => {
  await page.locator("tbody tr").first().click();
  await page.evaluate(() => {
    window.__archiveDownloadURL = "";
    window.open = () => ({
      close() {},
      location: {
        replace(url) {
          window.__archiveDownloadURL = url;
        },
      },
    });
  });
  await page.getByRole("button", { name: "下载当前 Session" }).click();
  const link = page.locator("#downloadNotice a");
  await expect(link).toHaveAttribute(
    "href",
    "http://127.0.0.1:4173/archive-api/v1/exports/mock-ticket",
  );
  await expect(link).toHaveText("mock-session-long-content.archive.jsonl");
  await expect
    .poll(() => page.evaluate(() => window.__archiveDownloadURL))
    .toBe(
      "http://127.0.0.1:4173/archive-api/v1/exports/mock-ticket",
    );

  const response = await request.get(
    "http://127.0.0.1:4173/archive-api/v1/exports/mock-ticket",
  );
  expect(response.ok()).toBeTruthy();
  expect(response.headers()["content-disposition"]).toContain(
    'filename="mock-session-long-content.archive.jsonl"',
  );
  expect(await response.text()).toContain("END-OF-LONG-TOOL-OUTPUT");
});

test("facets, i18n, deep links, and back navigation remain interactive", async ({
  page,
}) => {
  const sidebar = page.locator("#filterSidebar");
  await sidebar.locator(":scope > summary").click();
  await expect(sidebar).not.toHaveAttribute("open", "");
  await sidebar.locator(":scope > summary").click();
  await expect(sidebar).toHaveAttribute("open", "");

  await page.locator("#facetSearch").fill("project");
  await expect(page.locator('[data-filter-name*="project"]').first()).toBeVisible();
  await page.locator("#facetSearch").fill("");
  await page.locator('[data-facet="project.name"]').selectOption(
    "mock-long-project",
  );
  await expect(page.locator("#activeCount")).toHaveText("1");
  await page.getByRole("button", { name: "应用筛选" }).click();
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await page.getByRole("button", { name: "重置" }).click();
  await expect(page.locator("#activeCount")).toHaveText("0");

  await page.evaluate(() => {
    document.documentElement.lang = "en";
  });
  await expect(page.getByRole("heading", { name: "Session Archive" })).toBeVisible();
  await page.evaluate(() => {
    document.documentElement.lang = "zh-CN";
  });
  await expect(page.getByRole("heading", { name: "会话归档" })).toBeVisible();

  await page.goto(
    "/#/sessions/mock-session-long-content/turns/mock-turn-long-content",
  );
  await expect(page.getByRole("button", { name: /返回当前会话/ })).toBeVisible();
  await page.getByRole("button", { name: /返回当前会话/ }).click();
  await expect(page).toHaveURL(/\/sessions\/mock-session-long-content$/);
  await page.getByRole("button", { name: /返回会话列表/ }).click();
  await expect(page).toHaveURL(/#\/sessions$/);
  await expect(page.locator("#listView")).toBeVisible();
});
