const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

const page = fs.readFileSync(
  path.join(__dirname, "..", "internal", "archive", "web", "index.html"),
);
const sessionID = "mock-session-long-content";
const turnID = "mock-turn-long-content";
const requestID = "mock-request-long-content";
const longUser =
  "You are an expert at upholding safety and compliance standards for Codex ambient suggestions. " +
  "用户要求：详情页必须能够读取完整命令，列表可以只显示摘要。" +
  "LONG-USER-BODY ".repeat(700) +
  "END-OF-COMPLETE-USER-COMMAND";
const longSystem = (
  "系统指令必须按需加载，并允许查看全文。LONG-SYSTEM-CONTEXT ".repeat(500)
).slice(0, 17704) + "END-OF-SYSTEM-INSTRUCTIONS";
const longAssistant =
  "我会先说明处理结果，再展示必要的中间过程。" +
  "LONG-ASSISTANT-BODY ".repeat(280) +
  "END-OF-COMPLETE-ASSISTANT-ANSWER";
const longToolOutput =
  "命令执行结果：\n" +
  "TOOL-OUTPUT-LINE\n".repeat(7000) +
  "END-OF-LONG-TOOL-OUTPUT";
let turnTextPolls = 0;
const originalRequest = {
  model: "gpt-5.6-sol",
  instructions: longSystem,
  input: [
    {
      role: "user",
      content: [{ type: "input_text", text: longUser }],
    },
  ],
  tools: [
    {
      type: "function",
      name: "shell_command",
      description: "Run a command with deliberately long mock data.",
    },
  ],
};
const originalResponse = {
  output: [
    {
      type: "function_call",
      call_id: "call-long-output",
      name: "shell_command",
      arguments: JSON.stringify({
        command: "printf a deliberately long command",
      }),
    },
    {
      type: "function_call_output",
      call_id: "call-long-output",
      output: longToolOutput,
    },
    {
      type: "message",
      role: "assistant",
      content: [{ type: "output_text", text: longAssistant }],
    },
  ],
};
const encode = (value) =>
  Buffer.from(JSON.stringify(value), "utf8").toString("base64");
const requestRecord = {
  request_id: requestID,
  session_id: sessionID,
  key_id: "sha256:mock-key-hash",
  summary: longUser.slice(0, 160) + "…",
  response_preview: longAssistant.slice(0, 160) + "…",
  requested_model: "gpt-5.6-sol",
  model: "gpt-5.6-sol",
  outcome: "succeeded",
  status_code: 200,
  started_at: "2026-08-03T12:34:56+08:00",
  completed_at: "2026-08-03T12:35:56+08:00",
  original_request: encode(originalRequest),
  upstream_request: "",
  response: encode(originalResponse),
  facets: {
    "project.name": ["mock-long-project"],
    client: ["codex"],
    "request.kind": ["turn"],
    "tool.name": ["shell_command"],
    "key.id": ["sha256:mock-key-hash"],
  },
};
const timeline = {
  request_id: requestID,
  session_id: sessionID,
  key_id: "sha256:mock-key-hash",
  summary: requestRecord.summary,
  requested_model: "gpt-5.6-sol",
  model: "gpt-5.6-sol",
  outcome: "succeeded",
  status_code: 200,
  started_at: requestRecord.started_at,
  completed_at: requestRecord.completed_at,
  kind: "turn",
  input_items: 1,
  history_items: 42,
  system_chars: longSystem.length,
  tool_definitions: 1,
  entries: [
    { role: "assistant", text: longAssistant, label: "analysis" },
    {
      role: "tool_call",
      label: "shell_command",
      call_id: "call-long-output",
    },
    {
      role: "tool_result",
      label: "shell_command",
      call_id: "call-long-output",
    },
  ],
};

function json(response, value) {
  response.writeHead(200, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  response.end(JSON.stringify(value));
}

const server = http.createServer((request, response) => {
  const url = new URL(request.url, "http://127.0.0.1:4173");
  if (url.pathname === "/healthz") return json(response, { ok: true });
  if (url.pathname === "/" || url.pathname === "/management.html") {
    response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    return response.end(page);
  }
  if (url.pathname === "/v0/management/api-key-aliases") {
    return json(response, {
      items: [
        { apiKeyHash: "sha256:mock-key-hash", alias: "长文本测试 Key" },
      ],
    });
  }
  const prefix = "/v0/management/plugins/cpa-session-archive";
  if (url.pathname === prefix + "/stats") {
    return json(response, {
      compressed_bytes: 987654321,
      sessions: 1,
      records: 23,
    });
  }
  if (url.pathname === prefix + "/facets") {
    return json(response, [
      {
        name: "project.name",
        value: "mock-long-project",
        sessions: 1,
        records: 23,
      },
      { name: "client", value: "codex", sessions: 1, records: 23 },
      {
        name: "tool.name",
        value: "shell_command",
        sessions: 1,
        records: 23,
      },
      {
        name: "key.id",
        value: "sha256:mock-key-hash",
        sessions: 1,
        records: 23,
      },
    ]);
  }
  if (url.pathname === prefix + "/sessions") {
    return json(response, [
      {
        session_id: sessionID,
        summary: longUser.slice(0, 160) + "…",
        project: "mock-long-project",
        key_id: "sha256:mock-key-hash",
        model: "gpt-5.6-sol",
        requests: 23,
        last_at: "2026-08-03T12:35:56+08:00",
        thread_sources: ["user"],
        kinds: ["turn"],
      },
    ]);
  }
  if (url.pathname === prefix + "/turn-text") {
    turnTextPolls++;
    if (turnTextPolls === 1) {
      return json(response, { status: "building" });
    }
    return json(response, { status: "ok", text: longUser });
  }
  if (url.pathname === prefix + "/turns" && url.searchParams.has("turn_id")) {
    return json(response, {
      turn: {
        turn_id: turnID,
        session_id: sessionID,
        user_text: longUser.slice(0, 160) + "…",
        final_text: longAssistant,
        requests: 23,
        first_at: "2026-08-03T12:34:56+08:00",
        last_at: "2026-08-03T12:35:56+08:00",
        key_id: "sha256:mock-key-hash",
        model: "gpt-5.6-sol",
        outcome: "succeeded",
        status_code: 200,
        tool_names: ["shell_command"],
      },
      records: [timeline],
      total: 41,
      limit: 10,
      offset: Number(url.searchParams.get("offset") || 0),
    });
  }
  if (url.pathname === prefix + "/turns") {
    return json(response, {
      turns: [
        {
          turn_id: turnID,
          session_id: sessionID,
          user_text: longUser.slice(0, 160) + "…",
          final_text: longAssistant.slice(0, 160) + "…",
          requests: 23,
          first_at: "2026-08-03T12:34:56+08:00",
          last_at: "2026-08-03T12:35:56+08:00",
          key_id: "sha256:mock-key-hash",
          model: "gpt-5.6-sol",
          outcome: "succeeded",
          status_code: 200,
          tool_names: ["shell_command"],
        },
      ],
      total: 45,
      limit: 20,
      offset: Number(url.searchParams.get("offset") || 0),
    });
  }
  if (url.pathname === prefix + "/request-view") {
    return json(response, timeline);
  }
  if (url.pathname === prefix + "/requests") {
    return json(response, requestRecord);
  }
  if (url.pathname === prefix + "/request-context") {
    return json(response, [requestRecord]);
  }
  if (url.pathname === prefix + "/export") {
    const scope = url.searchParams.get("scope") || "session";
    const format = url.searchParams.get("format") || "archive";
    const filename =
      scope === "all"
        ? "cpa-session-archive.all." + format + ".jsonl"
        : sessionID + "." + format + ".jsonl";
    return json(response, {
      url: "/archive-api/v1/exports/mock-ticket",
      filename,
      content_type: "application/x-ndjson",
    });
  }
  if (url.pathname === "/archive-api/v1/exports/mock-ticket") {
    response.writeHead(200, {
      "content-type": "application/x-ndjson",
      "content-disposition":
        'attachment; filename="' + sessionID + '.archive.jsonl"',
    });
    return response.end(
      JSON.stringify({
        session_id: sessionID,
        request: originalRequest,
        response: originalResponse,
      }) + "\n",
    );
  }
  response.writeHead(404, { "content-type": "text/plain" });
  response.end("not found: " + url.pathname);
});

server.listen(4173, "127.0.0.1");
