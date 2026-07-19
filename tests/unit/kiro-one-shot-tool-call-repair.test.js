import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const fetchMock = vi.fn();
vi.mock("../../open-sse/utils/proxyFetch.js", () => ({
  proxyAwareFetch: (...args) => fetchMock(...args),
}));

const { KiroExecutor } = await import("../../open-sse/executors/kiro.js");
const { getExecutor } = await import("../../open-sse/executors/index.js");

function encodeHeader(name, value) {
  const nameBytes = new TextEncoder().encode(name);
  const valueBytes = new TextEncoder().encode(value);
  const out = new Uint8Array(1 + nameBytes.length + 1 + 2 + valueBytes.length);
  let offset = 0;
  out[offset++] = nameBytes.length;
  out.set(nameBytes, offset);
  offset += nameBytes.length;
  out[offset++] = 7;
  out[offset++] = (valueBytes.length >> 8) & 0xff;
  out[offset++] = valueBytes.length & 0xff;
  out.set(valueBytes, offset);
  return out;
}

function concatBytes(chunks) {
  const out = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.byteLength, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) {
      crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function writeFrameChecksums(frame) {
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  view.setUint32(8, crc32(frame.subarray(0, 8)), false);
  view.setUint32(frame.byteLength - 4, crc32(frame.subarray(0, frame.byteLength - 4)), false);
  return frame;
}

function encodeFrame(headerValues, payload) {
  const headers = concatBytes(Object.entries(headerValues).map(([name, value]) => encodeHeader(name, value)));
  const payloadBytes = new TextEncoder().encode(JSON.stringify(payload));
  const totalLength = 12 + headers.length + payloadBytes.length + 4;
  const frame = new Uint8Array(totalLength);
  const view = new DataView(frame.buffer);
  view.setUint32(0, totalLength, false);
  view.setUint32(4, headers.length, false);
  frame.set(headers, 12);
  frame.set(payloadBytes, 12 + headers.length);
  return writeFrameChecksums(frame);
}

function encodeEventFrame(eventType, payload) {
  return encodeFrame({ ":event-type": eventType }, payload);
}

function eventStreamResponse(frames, status = 200) {
  return new Response(new ReadableStream({
    start(controller) {
      for (const frame of frames) controller.enqueue(frame);
      controller.close();
    }
  }), { status, statusText: status === 200 ? "OK" : "Bad Gateway" });
}

function controlledEventStreamResponse(initialFrames = []) {
  let controllerRef;
  const response = new Response(new ReadableStream({
    start(controller) {
      controllerRef = controller;
      for (const frame of initialFrames) controller.enqueue(frame);
    }
  }), { status: 200, statusText: "OK" });

  return {
    response,
    enqueue(frame) {
      controllerRef.enqueue(frame);
    },
    close() {
      controllerRef.close();
    }
  };
}

async function collectText(stream) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
  }
  return text + decoder.decode();
}

function collectDataChunks(text) {
  return text
    .split("\n")
    .filter((line) => line.startsWith("data: "))
    .map((line) => line.slice(6).trim())
    .filter((data) => data && data !== "[DONE]")
    .map((data) => JSON.parse(data));
}

const credentials = {
  accessToken: "test-token",
  providerSpecificData: {
    kiroToolCallRepair: true
  }
};

beforeEach(() => {
  fetchMock.mockReset();
  delete process.env.KIRO_TOOL_CALL_REPAIR;
  delete process.env.KIRO_TOOL_CALL_REPAIR_BUFFER_MAX_BYTES;
});

afterEach(() => {
  delete process.env.KIRO_TOOL_CALL_REPAIR;
  delete process.env.KIRO_TOOL_CALL_REPAIR_BUFFER_MAX_BYTES;
  delete process.env.KIRO_TOOL_CALL_REPAIR_TTFT_TIMEOUT_MS;
  delete process.env.KIRO_TOOL_CALL_REPAIR_STALL_TIMEOUT_MS;
});

describe("Kiro one-shot tool_call repair", () => {
  it("repairs malformed wrapper output by default through the exported live executor", async () => {
    fetchMock
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("toolUseEvent", {
          toolUseId: "call_1",
          name: "tool_call",
          input: { arguments: { q: "router" } }
        }),
        encodeEventFrame("messageStopEvent", {})
      ]))
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("toolUseEvent", {
          toolUseId: "call_2",
          name: "tool_call",
          input: { name: "mcp_search", arguments: { q: "router" } }
        }),
        encodeEventFrame("messageStopEvent", {})
      ]));

    const result = await getExecutor("kiro").execute({
      model: "kr/gpt-5.6-sol",
      body: { systemPrompt: "base", conversationState: {} },
      stream: true,
      credentials: { accessToken: "test-token", providerSpecificData: {} }
    });
    const text = await collectText(result.response.body);
    const chunks = collectDataChunks(text);
    const toolChunks = chunks.flatMap((chunk) => chunk.choices?.[0]?.delta?.tool_calls || []);
    const args = toolChunks.map((toolCall) => toolCall.function?.arguments || "").join("");

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(text).not.toContain("invalid_kiro_tool_call");
    expect(JSON.parse(args)).toEqual({ name: "mcp_search", arguments: { q: "router" } });
  });

  it("releases non-ellipsis text without waiting for messageStop or EOF", async () => {
    const executor = new KiroExecutor();
    const upstream = controlledEventStreamResponse([
      encodeEventFrame("assistantResponseEvent", { content: "hello" })
    ]);
    fetchMock.mockResolvedValueOnce(upstream.response);

    const resultPromise = executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    const result = await resultPromise;
    const reader = result.response.body.getReader();
    const decoder = new TextDecoder();
    const firstRead = await reader.read();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(firstRead.done).toBe(false);
    expect(decoder.decode(firstRead.value)).toContain("hello");

    upstream.close();
    await reader.cancel("test complete").catch(() => {});
  });

  it("surfaces a malformed tool call that arrives after streamed text", async () => {
    const executor = new KiroExecutor();
    const upstream = controlledEventStreamResponse([
      encodeEventFrame("assistantResponseEvent", { content: "hello" })
    ]);
    fetchMock.mockResolvedValueOnce(upstream.response);

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const reader = result.response.body.getReader();
    const decoder = new TextDecoder();
    const firstRead = await reader.read();
    let text = decoder.decode(firstRead.value, { stream: true });

    upstream.enqueue(encodeEventFrame("toolUseEvent", {
      toolUseId: "call_late",
      name: "tool_call",
      input: { arguments: { q: "router" } }
    }));
    upstream.close();

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      text += decoder.decode(value, { stream: true });
    }
    text += decoder.decode();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(text).toContain("hello");
    expect(text).toContain("invalid_kiro_tool_call");
    expect(text).toContain("data: [DONE]");
    expect(text).not.toContain("\"finish_reason\":\"stop\"");
  });

  it("propagates client cancellation after the happy-path gate opens", async () => {
    const executor = new KiroExecutor();
    let cancelCount = 0;
    let cancelReason;
    let upstreamController;
    fetchMock.mockResolvedValueOnce(new Response(new ReadableStream({
      start(controller) {
        upstreamController = controller;
        controller.enqueue(encodeEventFrame("assistantResponseEvent", { content: "hello" }));
      },
      cancel(reason) {
        cancelCount++;
        cancelReason = reason;
      }
    }), { status: 200, statusText: "OK" }));

    const resultPromise = executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    const result = await resultPromise;
    const reader = result.response.body.getReader();
    const firstRead = await reader.read();

    expect(firstRead.done).toBe(false);
    await reader.cancel("client cancelled");
    await vi.waitFor(() => expect(cancelCount).toBe(1));
    expect(cancelReason).toBe("client cancelled");
  });

  it("releases a direct tool call without waiting for EOF", async () => {
    const executor = new KiroExecutor();
    const upstream = controlledEventStreamResponse([
      encodeEventFrame("toolUseEvent", {
        toolUseId: "call_streaming",
        name: "read_file",
        input: { path: "safe.txt" }
      })
    ]);
    fetchMock.mockResolvedValueOnce(upstream.response);

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const reader = result.response.body.getReader();
    const firstRead = await reader.read();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(firstRead.done).toBe(false);
    expect(new TextDecoder().decode(firstRead.value)).toContain('"name":"read_file"');

    upstream.close();
    await reader.cancel("test complete").catch(() => {});
  });

  it("releases a future-action prefix after the 800-character gate cap", async () => {
    const executor = new KiroExecutor();
    const longContent = `Next I'll verify ${"x".repeat(800)}`;
    const upstream = controlledEventStreamResponse([
      encodeEventFrame("assistantResponseEvent", { content: longContent })
    ]);
    fetchMock.mockResolvedValueOnce(upstream.response);

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const reader = result.response.body.getReader();
    const firstRead = await reader.read();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(firstRead.done).toBe(false);
    expect(new TextDecoder().decode(firstRead.value)).toContain("Next I'll verify");

    upstream.close();
    await reader.cancel("test complete").catch(() => {});
  });

  it("retries once on pre-output malformed wrapper output and does not leak fake tool calls", async () => {
    const executor = new KiroExecutor();
    fetchMock
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("toolUseEvent", {
          toolUseId: "call_1",
          name: "tool_call",
          input: { arguments: { q: "router" } }
        }),
        encodeEventFrame("messageStopEvent", {})
      ]))
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("toolUseEvent", {
          toolUseId: "call_2",
          name: "tool_call",
          input: { name: "mcp_search", arguments: { q: "router" } }
        }),
        encodeEventFrame("messageStopEvent", {})
      ]));

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { systemPrompt: "base", conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);
    const chunks = collectDataChunks(text);
    const toolChunks = chunks.flatMap((chunk) => chunk.choices?.[0]?.delta?.tool_calls || []);
    const args = toolChunks.map((toolCall) => toolCall.function?.arguments || "").join("");
    const retryBody = JSON.parse(fetchMock.mock.calls[1][1].body);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(text).not.toContain("invalid_kiro_tool_call");
    expect(JSON.parse(args)).toEqual({ name: "mcp_search", arguments: { q: "router" } });
    expect(retryBody.systemPrompt).toContain("Previous validation error");
  });

  it("retries an ellipsis-only final response once without leaking it downstream", async () => {
    const executor = new KiroExecutor();
    fetchMock
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "." }),
        encodeEventFrame("assistantResponseEvent", { content: ".." }),
        encodeEventFrame("messageStopEvent", {})
      ]))
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "The investigation is complete." }),
        encodeEventFrame("messageStopEvent", {})
      ]));

    const result = await executor.execute({
      model: "kr/gpt-5.6-sol",
      body: { systemPrompt: "base", conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);
    const chunks = collectDataChunks(text);
    const content = chunks.map((chunk) => chunk.choices?.[0]?.delta?.content || "").join("");
    const retryBody = JSON.parse(fetchMock.mock.calls[1][1].body);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(content).toBe("The investigation is complete.");
    expect(text).not.toContain('"content":"..."');
    expect(retryBody.systemPrompt).toContain("ended with only an ellipsis");
  });

  it("treats a unicode ellipsis-only final response as retryable", async () => {
    const executor = new KiroExecutor();
    fetchMock
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "…" }),
        encodeEventFrame("messageStopEvent", {})
      ]))
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "Recovered." }),
        encodeEventFrame("messageStopEvent", {})
      ]));

    const result = await executor.execute({
      model: "kr/gpt-5.6-sol",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(text).toContain("Recovered.");
    expect(text).not.toContain('"content":"…"');
  });

  it("retries when both reasoning and final content contain only ellipses", async () => {
    const executor = new KiroExecutor();
    fetchMock
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("reasoningContentEvent", { content: "..." }),
        encodeEventFrame("assistantResponseEvent", { content: "…" }),
        encodeEventFrame("messageStopEvent", {})
      ]))
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "Recovered after tool use." }),
        encodeEventFrame("messageStopEvent", {})
      ]));

    const result = await executor.execute({
      model: "kr/gpt-5.6-sol",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(text).toContain("Recovered after tool use.");
    expect(text).not.toContain("reasoning_content");
  });

  it("retries an exact visible ellipsis even after substantive reasoning", async () => {
    const executor = new KiroExecutor();
    fetchMock
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("reasoningContentEvent", { content: "I found the likely root cause." }),
        encodeEventFrame("assistantResponseEvent", { content: "..." })
      ]))
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "The root cause is the invalid terminal frame." })
      ]));

    const result = await executor.execute({
      model: "kr/gpt-5.6-sol",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(text).toContain("The root cause is the invalid terminal frame.");
    expect(text).not.toContain('"content":"..."');
    expect(text).not.toContain("I found the likely root cause.");
  });

  it("returns a retryable upstream error when the ellipsis retry is also invalid", async () => {
    const executor = new KiroExecutor();
    fetchMock
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "..." }),
        encodeEventFrame("messageStopEvent", {})
      ]))
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "..." }),
        encodeEventFrame("messageStopEvent", {})
      ]));

    const result = await executor.execute({
      model: "kr/gpt-5.6-sol",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const error = await result.response.json();

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(result.response.status).toBe(502);
    expect(error.error.code).toBe("kiro_ellipsis_retry_failed");
  });

  it("does not retry legitimate text that merely ends with an ellipsis", async () => {
    const executor = new KiroExecutor();
    fetchMock.mockResolvedValueOnce(eventStreamResponse([
      encodeEventFrame("assistantResponseEvent", { content: "Working..." }),
      encodeEventFrame("messageStopEvent", {})
    ]));

    const result = await executor.execute({
      model: "kr/gpt-5.6-sol",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(text).toContain("Working...");
  });

  it("emits repair retry failure code when the single repair retry is still malformed", async () => {
    const executor = new KiroExecutor();
    fetchMock
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("toolUseEvent", {
          toolUseId: "call_1",
          name: "tool_call",
          input: { arguments: { q: "router" } }
        }),
        encodeEventFrame("messageStopEvent", {})
      ]))
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("toolUseEvent", {
          toolUseId: "call_2",
          name: "tool_call",
          input: { arguments: { q: "router" } }
        }),
        encodeEventFrame("messageStopEvent", {})
      ]));

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(text).toContain("kiro_tool_call_repair_retry_failed");
    expect(text).not.toContain("invalid_kiro_tool_call");
    expect(text).toContain("missing nested MCP tool name");
    expect(text).not.toContain("\"tool_calls\"");
  });

  it("classifies invalid transformed output before treating its SSE error as stream data", async () => {
    const executor = new KiroExecutor();
    const gate = await executor.openToolCallRepairGate(eventStreamResponse([
      encodeEventFrame("toolUseEvent", {
        toolUseId: "call_1",
        name: "tool_call",
        input: { arguments: { q: "router" } }
      }),
      encodeEventFrame("messageStopEvent", {})
    ]), {
      model: "kr/claude-opus-4.8"
    }, {
      signal: undefined,
      maxBufferBytes: 1024 * 1024,
      ttftTimeoutMs: 1000,
      stallTimeoutMs: 1000,
      suppressInvalidToolCallError: false,
      invalidToolCallErrorCode: "kiro_tool_call_repair_retry_failed"
    });

    expect(gate.kind).toBe("invalid");
    expect(gate.invalidToolCall).toContain("missing nested MCP tool name");
    expect(gate.firstChunk).toBeUndefined();
  });

  it("propagates retry HTTP 429 instead of hiding it in a 200 SSE error", async () => {
    const executor = new KiroExecutor();
    executor.config = { ...executor.config, baseUrls: [executor.config.baseUrls[0]] };
    fetchMock
      .mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("toolUseEvent", {
          toolUseId: "call_1",
          name: "tool_call",
          input: { arguments: { q: "router" } }
        }),
        encodeEventFrame("messageStopEvent", {})
      ]))
      .mockResolvedValueOnce(new Response("rate limited", { status: 429, statusText: "Too Many Requests" }));

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(result.response.status).toBe(429);
    expect(await result.response.text()).toBe("rate limited");
  });

  it("allows repair to be explicitly disabled", async () => {
    const executor = new KiroExecutor();
    fetchMock.mockResolvedValueOnce(eventStreamResponse([
      encodeEventFrame("toolUseEvent", {
        toolUseId: "call_1",
        name: "tool_call",
        input: { arguments: { q: "router" } }
      }),
      encodeEventFrame("messageStopEvent", {})
    ]));

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials: {
        accessToken: "test-token",
        providerSpecificData: { kiroToolCallRepair: false }
      }
    });
    const text = await collectText(result.response.body);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(text).toContain("invalid_kiro_tool_call");
    expect(text).not.toContain("\"tool_calls\"");
  });

  it("fails cleanly if the private repair gate buffer exceeds its configured cap", async () => {
    process.env.KIRO_TOOL_CALL_REPAIR_BUFFER_MAX_BYTES = "8";
    const executor = new KiroExecutor();
    fetchMock.mockResolvedValueOnce(eventStreamResponse([
      encodeEventFrame("assistantResponseEvent", { content: "hello" })
    ]));

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(text).toContain("kiro_tool_call_repair_buffer_exceeded");
  });

  it("aborts a gated first attempt and cancels its upstream reader", async () => {
    process.env.KIRO_TOOL_CALL_REPAIR_STALL_TIMEOUT_MS = "1000";
    const executor = new KiroExecutor();
    let cancelReason;
    fetchMock.mockResolvedValueOnce(new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(encodeEventFrame("toolUseEvent", { toolUseId: "call_1", name: "tool_call" }));
      },
      cancel(reason) {
        cancelReason = reason;
      }
    }), { status: 200, statusText: "OK" }));
    const abortController = new AbortController();

    const executePromise = executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials,
      signal: abortController.signal
    });
    abortController.abort("client aborted");

    await expect(executePromise).rejects.toMatchObject({ name: "AbortError" });
    expect(cancelReason).toBeDefined();
  });

  it("uses separate TTFT and inter-chunk stall timeouts for the repair gate", async () => {
    process.env.KIRO_TOOL_CALL_REPAIR_TTFT_TIMEOUT_MS = "1000";
    process.env.KIRO_TOOL_CALL_REPAIR_STALL_TIMEOUT_MS = "1";
    const executor = new KiroExecutor();
    fetchMock.mockResolvedValueOnce(new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(encodeEventFrame("toolUseEvent", { toolUseId: "call_1", name: "tool_call" }));
      }
    }), { status: 200, statusText: "OK" }));

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);

    expect(text).toContain("Kiro tool_call repair stalled");
  });

  it("does not retry valid wrapper output and calls upstream exactly once", async () => {
    const executor = new KiroExecutor();
    fetchMock.mockResolvedValueOnce(eventStreamResponse([
      encodeEventFrame("toolUseEvent", {
        toolUseId: "call_1",
        name: "tool_call",
        input: { name: "mcp_search", arguments: { q: "router" } }
      }),
      encodeEventFrame("messageStopEvent", {})
    ]));

    const result = await executor.execute({
      model: "kr/claude-opus-4.8",
      body: { conversationState: {} },
      stream: true,
      credentials
    });
    const text = await collectText(result.response.body);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(text).not.toContain("kiro_tool_call_repair");
    expect(text).toContain("\"tool_calls\"");
  });

  describe("Kiro terminal integrity at the live executor seam", () => {
    it("accepts text plus usage/context events at clean EOF without messageStop", async () => {
      const executor = new KiroExecutor();
      const incompleteFrames = [
        encodeEventFrame("assistantResponseEvent", { content: "The deployment is healthy." }),
        encodeEventFrame("meteringEvent", { usage: 1, unit: "credit" }),
        encodeEventFrame("contextUsageEvent", { contextUsagePercentage: 12 })
      ];
      fetchMock.mockResolvedValueOnce(eventStreamResponse(incompleteFrames));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(result.response.status).toBe(200);
      expect(text).toContain("The deployment is healthy.");
      expect(text).toContain('"finish_reason":"stop"');
      expect(text).toContain('"kiro_credits":1');
    });

    it("accepts text followed directly by clean EOF", async () => {
      const executor = new KiroExecutor();
      const incomplete = eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "Partial progress that must stay private." })
      ]);
      fetchMock.mockResolvedValueOnce(incomplete);

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const body = await result.response.text();

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(result.response.status).toBe(200);
      expect(body).toContain("Partial progress that must stay private.");
      expect(body).toContain('"finish_reason":"stop"');
    });

    it("also accepts an optional genuine messageStopEvent as completion", async () => {
      const executor = new KiroExecutor();
      fetchMock.mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "Complete answer." }),
        encodeEventFrame("messageStopEvent", {})
      ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(text).toContain("Complete answer.");
      expect(text).toContain('"finish_reason":"stop"');
      expect(text).toContain("[DONE]");
    });

    it("retries a messageStop-only empty response once", async () => {
      const executor = new KiroExecutor();
      fetchMock
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("messageStopEvent", {})
        ]))
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("assistantResponseEvent", { content: "Recovered from empty response." })
        ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(result.response.status).toBe(200);
      expect(text).toContain("Recovered from empty response.");
      expect(text).not.toContain("kiro_missing_terminal");
    });

    it("accepts tool-call output at clean EOF", async () => {
      const executor = new KiroExecutor();
      fetchMock.mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("toolUseEvent", {
          toolUseId: "call_tool_eof",
          name: "read_file",
          input: { path: "safe.txt" }
        })
      ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(text).toContain('"finish_reason":"tool_calls"');
      expect(text).toContain('"name":"read_file"');
      expect(text).not.toContain("kiro_missing_terminal");
    });

    it("emits a terminal SSE error, never stop, after semantic output plus a later truncated frame", async () => {
      const executor = new KiroExecutor();
      const laterFrame = encodeEventFrame("meteringEvent", { usage: 1, unit: "credit" });
      fetchMock.mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: "Visible before transport failure." }),
        laterFrame.slice(0, laterFrame.byteLength - 3)
      ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(text).toContain("Visible before transport failure.");
      expect(text).toContain('"code":"kiro_missing_terminal"');
      expect(text).toContain('"terminal_provenance":"incomplete_eventstream_frame"');
      expect(text).not.toContain('"finish_reason":"stop"');
    });

    it.each(["exception", "error"])(
      "treats an AWS EventStream %s message after semantic output as terminal failure",
      async (messageType) => {
        const executor = new KiroExecutor();
        const upstream = controlledEventStreamResponse([
          encodeEventFrame("assistantResponseEvent", { content: "Visible before upstream exception." })
        ]);
        fetchMock.mockResolvedValueOnce(upstream.response);

        const result = await executor.execute({
          model: "kr/gpt-5.6-sol",
          body: { conversationState: {} },
          stream: true,
          credentials
        });
        upstream.enqueue(encodeFrame({
          ":message-type": messageType,
          ...(messageType === "exception" ? { ":exception-type": "InternalServerException" } : {})
        }, { message: "upstream failed" }));
        upstream.close();
        const text = await collectText(result.response.body);

        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(text).toContain("Visible before upstream exception.");
        expect(text).toContain('"code":"kiro_missing_terminal"');
        expect(text).toContain('"terminal_provenance":"upstream_eventstream_error"');
        expect(text).not.toContain('"finish_reason":"stop"');
      }
    );

    it("rejects a message CRC mismatch and retries before releasing output", async () => {
      const corrupt = encodeEventFrame("assistantResponseEvent", { content: "CRC-corrupt output." });
      corrupt[corrupt.byteLength - 1] ^= 0xff;
      const executor = new KiroExecutor();
      fetchMock
        .mockResolvedValueOnce(eventStreamResponse([corrupt]))
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("assistantResponseEvent", { content: "Recovered after CRC validation." })
        ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(text).toContain("Recovered after CRC validation.");
      expect(text).not.toContain("CRC-corrupt output.");
    });

    it("rejects a prelude CRC mismatch before parsing the frame", async () => {
      const corrupt = encodeEventFrame("assistantResponseEvent", { content: "Bad prelude CRC." });
      corrupt[8] ^= 0xff;
      const terminalStates = [];
      const executor = new KiroExecutor();
      const transformed = executor.transformEventStreamToSSE(
        eventStreamResponse([corrupt]),
        "kr/gpt-5.6-sol",
        { onTerminalState: (state) => terminalStates.push(state) }
      );
      const text = await collectText(transformed.body);

      expect(text).toContain('"terminal_provenance":"corrupt_eventstream_frame"');
      expect(text).not.toContain("Bad prelude CRC.");
      expect(text).not.toContain('"finish_reason":"stop"');
      expect(terminalStates.at(-1)).toMatchObject({
        terminal_provenance: "corrupt_eventstream_frame"
      });
    });

    it("classifies out-of-bounds EventStream headers as corrupt transport", async () => {
      const malformed = encodeEventFrame("assistantResponseEvent", { content: "must not parse" });
      const view = new DataView(malformed.buffer, malformed.byteOffset, malformed.byteLength);
      view.setUint32(4, malformed.byteLength - 15, false);
      writeFrameChecksums(malformed);
      const terminalStates = [];
      const executor = new KiroExecutor();
      const transformed = executor.transformEventStreamToSSE(
        eventStreamResponse([malformed]),
        "kr/gpt-5.6-sol",
        { onTerminalState: (state) => terminalStates.push(state) }
      );
      const text = await collectText(transformed.body);

      expect(text).toContain('"code":"kiro_missing_terminal"');
      expect(text).toContain('"terminal_provenance":"corrupt_eventstream_frame"');
      expect(text).not.toContain("must not parse");
      expect(text).not.toContain('"finish_reason":"stop"');
      expect(terminalStates).toMatchObject([{
        terminal_provenance: "corrupt_eventstream_frame"
      }]);
    });

    it("retries a truncated frame and releases only the clean second attempt", async () => {
      const executor = new KiroExecutor();
      const truncated = encodeEventFrame("assistantResponseEvent", { content: "Unproven partial." });
      fetchMock
        .mockResolvedValueOnce(eventStreamResponse([truncated.slice(0, truncated.byteLength - 3)]))
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("assistantResponseEvent", { content: "Recovered complete answer." })
        ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(result.response.status).toBe(200);
      expect(text).toContain("Recovered complete answer.");
      expect(text).not.toContain("Unproven partial.");
      expect(text).toContain('"finish_reason":"stop"');
    });

    it("bounds empty-response repair to one retry and returns fallback-compatible 502", async () => {
      const executor = new KiroExecutor();
      fetchMock
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("meteringEvent", { usage: 1, unit: "credit" })
        ]))
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("contextUsageEvent", { contextUsagePercentage: 5 })
        ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const body = await result.response.text();

      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(result.response.status).toBe(502);
      expect(result.response.headers.get("content-type")).toContain("application/json");
      expect(body).toContain("kiro_missing_terminal_retry_failed");
      expect(body).toContain("empty_response_eof");
    });

    it.each([
      "接下來我只再確認部署結果。",
      "現在我會繼續追查剩下的日誌。",
      "我會重新抓取 parent thread、指定 Sentry event 與 issue 最新分布，並以目前程式碼/git lineage 交叉驗證；以下調查會以這次查詢結果為準。",
      "Next I'll verify the deployment logs.",
      "I'll verify the deployment logs now.",
      "Let me check the remaining failures."
    ])("retries a Kiro-only short future-action final: %s", async (shortFinal) => {
      const executor = new KiroExecutor();
      fetchMock
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("assistantResponseEvent", { content: shortFinal })
        ]))
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("assistantResponseEvent", { content: "Verification completed successfully." })
        ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(text).toContain("Verification completed successfully.");
      expect(text).not.toContain(shortFinal);
    });

    it("holds the observed Chinese progress-only final until terminal classification", async () => {
      const shortFinal = "我會重新抓取 parent thread、指定 Sentry event 與 issue 最新分布，並以目前程式碼/git lineage 交叉驗證；以下調查會以這次查詢結果為準。";
      const executor = new KiroExecutor();
      const firstAttempt = controlledEventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: shortFinal })
      ]);
      fetchMock
        .mockResolvedValueOnce(firstAttempt.response)
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("assistantResponseEvent", { content: "Verification completed successfully." })
        ]));

      const resultPromise = executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const earlyResult = await Promise.race([
        resultPromise.then(() => "settled"),
        new Promise((resolve) => setTimeout(() => resolve("pending"), 25))
      ]);

      expect(earlyResult).toBe("pending");
      firstAttempt.close();
      const result = await resultPromise;
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(text).toContain("Verification completed successfully.");
      expect(text).not.toContain(shortFinal);
    });

    it("bounds short future-action repair to one retry", async () => {
      const shortFinal = "Next I'll verify the deployment logs.";
      const executor = new KiroExecutor();
      fetchMock
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("assistantResponseEvent", { content: shortFinal })
        ]))
        .mockResolvedValueOnce(eventStreamResponse([
          encodeEventFrame("assistantResponseEvent", { content: shortFinal })
        ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const error = await result.response.json();

      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(result.response.status).toBe(502);
      expect(error.error.code).toBe("kiro_short_final_retry_failed");
    });

    it.each([
      "接下來請你先批准部署，我會等待你的確認。",
      "已完成驗證，所有測試均通過。",
      "我會建議先正規化 response shape，再補上欄位驗證。",
      "我會補充兩點：第一，response shape 應正規化；第二，欄位需要驗證。",
      "我會重新抓取最新資料；調查完成，結果如下：無異常。",
      "我會重新抓取最新資料；目前結果顯示所有事件均無異常。",
      "我會重新抓取最新資料；調查已完成，以下是完整結論。",
      "Next I'll verify after you approve the deployment.",
      "The verification is complete and all tests passed."
    ])("does not retry an excluded or completed short final: %s", async (finalText) => {
      const executor = new KiroExecutor();
      fetchMock.mockResolvedValueOnce(eventStreamResponse([
        encodeEventFrame("assistantResponseEvent", { content: finalText })
      ]));

      const result = await executor.execute({
        model: "kr/gpt-5.6-sol",
        body: { conversationState: {} },
        stream: true,
        credentials
      });
      const text = await collectText(result.response.body);

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(text).toContain(finalText);
      expect(text).toContain('"finish_reason":"stop"');
    });
  });
});
