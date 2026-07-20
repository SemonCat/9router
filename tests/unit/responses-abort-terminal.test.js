import { describe, expect, it, vi } from "vitest";

vi.mock("@/lib/usageDb.js", () => ({
  appendRequestLog: vi.fn(async () => {}),
  saveRequestDetail: vi.fn(async () => {}),
  saveRequestUsage: vi.fn(async () => {}),
  trackPendingRequest: vi.fn()
}));

import { createDisconnectAwareStream } from "../../open-sse/utils/streamHandler.js";
import { buildAbortedResponsesTerminalBytes } from "../../open-sse/utils/responsesStreamHelpers.js";
import {
  createResponsesAccumulator,
  reduceResponsesEvent
} from "../../open-sse/translator/concerns/responsesAccumulator.js";
import { FORMATS } from "../../open-sse/translator/formats.js";
import {
  createPassthroughStreamWithLogger,
  createSSETransformStreamWithLogger
} from "../../open-sse/utils/stream.js";

// Minimal stream controller stub
function makeController() {
  let connected = true;
  return {
    signal: new AbortController().signal,
    startTime: Date.now(),
    isConnected: () => connected,
    handleComplete: () => { connected = false; },
    handleError: () => { connected = false; },
    handleDisconnect: () => { connected = false; },
    abort: () => { connected = false; },
  };
}

async function readAll(stream) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
  }
  text += decoder.decode();
  return text;
}

describe("Responses abort terminal synthesis", () => {
  it("emits response.failed + [DONE] when upstream errors (abort/stall)", async () => {
    // Upstream readable that errors mid-stream (simulates fetch abort on stall)
    const upstream = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("event: response.created\ndata: {}\n\n"));
        controller.error(new Error("stream stall timeout"));
      },
    });

    const out = createDisconnectAwareStream(
      { readable: upstream, writable: { getWriter: () => ({ abort: () => Promise.resolve() }) } },
      makeController(),
      buildAbortedResponsesTerminalBytes
    );

    const text = await readAll(out);
    expect(text).toContain("event: response.failed");
    expect(text).toContain("data: [DONE]");
  });

  it("does not synthesize terminal for non-Responses streams (callback null)", async () => {
    const upstream = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("data: hi\n\n"));
        controller.error(new Error("socket hang up"));
      },
    });

    const out = createDisconnectAwareStream(
      { readable: upstream, writable: { getWriter: () => ({ abort: () => Promise.resolve() }) } },
      makeController(),
      null
    );

    const text = await readAll(out);
    expect(text).not.toContain("response.failed");
    expect(text).not.toContain("[DONE]");
  });

  it("includes reconstructed partial output in the synthesized abort terminal", async () => {
    const accumulator = createResponsesAccumulator({ id: "resp_abort" });
    reduceResponsesEvent(accumulator, {
      type: "response.output_text.delta",
      output_index: 0,
      item_id: "msg_abort",
      delta: "partial before stall"
    });
    const upstream = new ReadableStream({
      start(controller) {
        controller.error(new Error("stream stall timeout"));
      },
    });

    const out = createDisconnectAwareStream(
      { readable: upstream, writable: { getWriter: () => ({ abort: () => Promise.resolve() }) } },
      makeController(),
      () => buildAbortedResponsesTerminalBytes(accumulator)
    );

    const text = await readAll(out);
    expect(text).toContain('"id":"resp_abort"');
    expect(text).toContain('"text":"partial before stall"');
    expect(text.match(/event: response\.failed/g)).toHaveLength(1);
    expect(buildAbortedResponsesTerminalBytes(accumulator)).toBeNull();
  });

  it("tracks raw Responses passthrough output for abort synthesis", async () => {
    const accumulator = createResponsesAccumulator({ model: "gpt-passthrough" });
    const transform = createPassthroughStreamWithLogger(
      "codex",
      null,
      "gpt-passthrough",
      null,
      null,
      null,
      null,
      accumulator
    );
    const encoder = new TextEncoder();
    const chunks = [
      [
        "event: response.created",
        `data: ${JSON.stringify({
          type: "response.created",
          response: { id: "resp_passthrough", status: "in_progress", output: [] }
        })}`,
        "",
        ""
      ].join("\n"),
      [
        "event: response.output_text.delta",
        `data: ${JSON.stringify({
          type: "response.output_text.delta",
          output_index: 0,
          item_id: "msg_passthrough",
          delta: "partial passthrough"
        })}`,
        "",
        ""
      ].join("\n")
    ];
    let chunkIndex = 0;
    const upstream = new ReadableStream({
      pull(controller) {
        if (chunkIndex < chunks.length) {
          controller.enqueue(encoder.encode(chunks[chunkIndex++]));
          return;
        }
        controller.error(new Error("stream stall timeout"));
      }
    }).pipeThrough(transform);

    const out = createDisconnectAwareStream(
      { readable: upstream, writable: { getWriter: () => ({ abort: () => Promise.resolve() }) } },
      makeController(),
      () => buildAbortedResponsesTerminalBytes(accumulator)
    );

    const text = await readAll(out);
    expect(text).toContain('"id":"resp_passthrough"');
    expect(text).toContain('"text":"partial passthrough"');
    expect(text.match(/event: response\.failed/g)).toHaveLength(1);
  });

  it("fails raw Responses passthrough when clean EOF arrives without a terminal", async () => {
    const accumulator = createResponsesAccumulator({ model: "gpt-passthrough" });
    const transform = createPassthroughStreamWithLogger(
      "codex",
      null,
      "gpt-passthrough",
      null,
      null,
      null,
      null,
      accumulator
    );
    const payload = [
      "event: response.created",
      `data: ${JSON.stringify({
        type: "response.created",
        response: { id: "resp_clean_eof", status: "in_progress", output: [] }
      })}`,
      "",
      "event: response.output_text.delta",
      `data: ${JSON.stringify({
        type: "response.output_text.delta",
        output_index: 0,
        item_id: "msg_clean_eof",
        delta: "partial before clean eof"
      })}`,
      "",
      ""
    ].join("\n");
    const upstream = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(payload));
        controller.close();
      }
    }).pipeThrough(transform);

    const text = await readAll(upstream);
    expect(text).toContain("event: response.failed");
    expect(text).toContain('"id":"resp_clean_eof"');
    expect(text).toContain('"text":"partial before clean eof"');
    expect(text.match(/event: response\.failed/g)).toHaveLength(1);
    expect(text.match(/data: \[DONE\]/g)).toHaveLength(1);
    expect(accumulator.doneSent).toBe(true);
  });

  it.each(["data: [DONE]", "data:[DONE]"])(
    "emits failure before a raw Responses %s sentinel when no terminal arrived",
    async doneSentinel => {
      const accumulator = createResponsesAccumulator({ model: "gpt-passthrough" });
      const transform = createPassthroughStreamWithLogger(
        "codex",
        null,
        "gpt-passthrough",
        null,
        null,
        null,
        null,
        accumulator
      );
      const payload = [
        "event: response.created",
        `data: ${JSON.stringify({
          type: "response.created",
          response: { id: "resp_done_without_terminal", status: "in_progress", output: [] }
        })}`,
        "",
        doneSentinel,
        "",
        ""
      ].join("\n");
      const upstream = new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(payload));
          controller.close();
        }
      }).pipeThrough(transform);

      const text = await readAll(upstream);
      expect(text.indexOf("event: response.failed")).toBeLessThan(text.indexOf("data: [DONE]"));
      expect(text.match(/event: response\.failed/g)).toHaveLength(1);
      expect(text.match(/data: \[DONE\]/g)).toHaveLength(1);
    }
  );

  it("accepts a final unterminated Responses terminal line", async () => {
    const accumulator = createResponsesAccumulator({ model: "gpt-passthrough" });
    const transform = createPassthroughStreamWithLogger(
      "codex",
      null,
      "gpt-passthrough",
      null,
      null,
      null,
      null,
      accumulator
    );
    const payload = [
      "event: response.completed",
      `data: ${JSON.stringify({
        type: "response.completed",
        response: { id: "resp_unterminated_terminal", status: "completed", output: [] }
      })}`
    ].join("\n");
    const upstream = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(payload));
        controller.close();
      }
    }).pipeThrough(transform);

    const text = await readAll(upstream);
    expect(text).toContain("event: response.completed");
    expect(text).not.toContain("event: response.failed");
    expect(text.match(/data: \[DONE\]/g)).toHaveLength(1);
  });

  it("fails before an unterminated no-space DONE sentinel", async () => {
    const accumulator = createResponsesAccumulator({ id: "resp_unterminated_done" });
    const transform = createPassthroughStreamWithLogger(
      "codex",
      null,
      "gpt-passthrough",
      null,
      null,
      null,
      null,
      accumulator
    );
    const upstream = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("data:[DONE]"));
        controller.close();
      }
    }).pipeThrough(transform);

    const text = await readAll(upstream);
    expect(text.indexOf("event: response.failed")).toBeLessThan(text.indexOf("data: [DONE]"));
    expect(text.match(/event: response\.failed/g)).toHaveLength(1);
    expect(text.match(/data: \[DONE\]/g)).toHaveLength(1);
  });

  it("emits only DONE when abort follows an accepted Responses terminal", () => {
    const accumulator = createResponsesAccumulator({ id: "resp_terminal" });
    reduceResponsesEvent(accumulator, {
      type: "response.completed",
      response: { id: "resp_terminal", status: "completed", output: [] }
    });

    const first = new TextDecoder().decode(buildAbortedResponsesTerminalBytes(accumulator));
    expect(first).toBe("data: [DONE]\n\n");
    expect(buildAbortedResponsesTerminalBytes(accumulator)).toBeNull();
  });

  it("finalizes a Responses-to-Chat translation when upstream aborts", async () => {
    const transform = createSSETransformStreamWithLogger(
      FORMATS.OPENAI_RESPONSES,
      FORMATS.OPENAI,
      "codex",
      null,
      null,
      "gpt-p0"
    );
    const encoder = new TextEncoder();
    let sentPartial = false;
    const upstream = new ReadableStream({
      pull(controller) {
        if (!sentPartial) {
          sentPartial = true;
          controller.enqueue(encoder.encode([
            "event: response.output_text.delta",
            `data: ${JSON.stringify({ type: "response.output_text.delta", output_index: 0, item_id: "msg_abort", delta: "partial" })}`,
            ""
          ].join("\n")));
          return;
        }
        controller.error(new Error("stream stall timeout"));
      }
    }).pipeThrough(transform);

    const out = createDisconnectAwareStream(
      { readable: upstream, writable: { getWriter: () => ({ abort: () => Promise.resolve() }) } },
      makeController(),
      () => transform.buildAbortedTerminalBytes()
    );

    const text = await readAll(out);
    expect(text).toContain('"partial"');
    expect(text).toContain('"finish_reason":"stop"');
    expect(text).toContain("stream closed before a terminal response event");
  });
});
