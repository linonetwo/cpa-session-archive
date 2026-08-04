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
  await primary
    .locator(".turn-final")
    .getByRole("button", { name: "展开完整内容" })
    .click();
  await expect(primary).toContainText("END-OF-COMPLETE-ASSISTANT-ANSWER");

  await page
    .locator("#turnPagination")
    .getByRole("button", { name: "下一页" })
    .click();
  await expect(page.locator("#turnPageInfo")).toContainText("第 2 / 5 页");
  await page
    .locator("#turnPagination")
    .getByRole("button", { name: "上一页" })
    .click();
  await expect(page.locator("#turnPageInfo")).toContainText("第 1 / 5 页");

  await page.locator(".process-step").first().click();
  await expect(page.getByText("shell_command · 查看完整调用")).toBeVisible();
  await page.getByText("shell_command · 查看完整调用").click();
  await expect(page).toHaveURL(/\/tools\/call-long-output$/);
  await expect(page.locator("#subDetail")).not.toContainText(
    "END-OF-LONG-TOOL-OUTPUT",
  );
  await page
    .locator("#subDetail .tool-card")
    .last()
    .getByRole("button", { name: "展开完整内容" })
    .click();
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
}) => {
  await page.locator("tbody tr").first().click();
  const sessionDownload = page.waitForEvent("download");
  await page.getByRole("button", { name: "下载当前 Session" }).click();
  const downloaded = await sessionDownload;
  expect(downloaded.suggestedFilename()).toBe(
    "mock-session-long-content.archive.jsonl",
  );
  const downloadedPath = await downloaded.path();
  expect(downloadedPath).toBeTruthy();
  expect(require("node:fs").statSync(downloadedPath).size).toBeGreaterThan(0);
  expect(require("node:fs").readFileSync(downloadedPath, "utf8")).toContain(
    "END-OF-LONG-TOOL-OUTPUT",
  );
  const link = page.locator("#downloadNotice a");
  await expect(link).toHaveAttribute(
    "href",
    "http://127.0.0.1:4173/archive-api/v1/exports/mock-ticket",
  );
  await expect(link).toHaveText("mock-session-long-content.archive.jsonl");

  await page.getByRole("button", { name: /返回会话列表/ }).click();
  await page.locator("#allExportFormat").selectOption("sft");
  const allDownload = page.waitForEvent("download");
  await page.getByRole("button", { name: "下载全库" }).click();
  expect((await allDownload).suggestedFilename()).toBe(
    "mock-session-long-content.archive.jsonl",
  );
  await expect(page.locator("#allDownloadNotice a")).toHaveText(
    "cpa-session-archive.all.sft.jsonl",
  );
});

test("escaped environment context is readable and process content loads lazily", async ({
  page,
}) => {
  await expect(page.locator("tbody tr").first()).toContainText(
    'calibre 路径 "C:\\Books" 应显示为正常引号',
  );
  await page.locator("tbody tr").first().click();
  await expect(page.locator(".turn-card").first()).toContainText(
    "<environment_context>mock cwd</environment_context>",
  );
  await page.locator(".turn-card").click();
  const step = page.locator(".process-step").first();
  await expect(step).toContainText("展开后加载完整过程正文");
  await step.click();
  await expect(step).not.toContainText("没有可直接阅读的文本内容");
  await step.getByRole("button", { name: "原始请求/响应（排障）" }).click();
  await expect(page.locator("#requestTitle")).toContainText(
    "<environment_context>mock cwd</environment_context>",
  );
  await expect(page.locator("#requestDetail")).toContainText(
    "<environment_context>mock cwd</environment_context>",
  );
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

test("session pagination and loading SQL remain visible and operable", async ({
  page,
}) => {
  let delayed = false;
  await page.route("**/cpa-session-archive/turns?**", async (route) => {
    if (!delayed) {
      delayed = true;
      await new Promise((resolve) => setTimeout(resolve, 600));
    }
    await route.continue();
  });
  await page.locator("tbody tr").first().click();
  await expect(page.locator("#loading")).toHaveClass(/open/);
  await expect(page.locator("#loadingSQL")).toContainText(
    "SELECT request_id, key_id, summary",
  );
  await expect(page.locator(".turn-card")).toBeVisible();
  await expect(page.locator("#pageInfo")).toContainText("第 1 / 3 页");

  await page.getByRole("button", { name: "末页" }).click();
  await expect(page.locator("#pageInfo")).toContainText("第 3 / 3 页");
  await page.getByRole("button", { name: "首页" }).click();
  await expect(page.locator("#pageInfo")).toContainText("第 1 / 3 页");
  await page.locator("#pageJump").fill("2");
  await page.locator("#pageJump").press("Enter");
  await expect(page.locator("#pageInfo")).toContainText("第 2 / 3 页");
  await page.locator("#sessionOrder").selectOption("asc");
  await expect(page.locator("#pageInfo")).toContainText("第 1 / 3 页");
});
