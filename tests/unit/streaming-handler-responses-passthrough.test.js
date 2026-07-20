import { beforeEach, describe, expect, it, vi } from "vitest";

const { passthroughMock, pipeMock, translateMock } = vi.hoisted(() => ({
  passthroughMock: vi.fn(() => new TransformStream()),
  pipeMock: vi.fn(providerResponse => providerResponse.body),
  translateMock: vi.fn(() => new TransformStream())
}));

vi.mock("../../open-sse/utils/stream.js", () => ({
  COLORS: { green: "", reset: "" },
  createPassthroughStreamWithLogger: passthroughMock,
  createSSETransformStreamWithLogger: translateMock
}));

vi.mock("../../open-sse/utils/streamHandler.js", () => ({
  pipeWithDisconnect: pipeMock
}));

vi.mock("@/lib/usageDb.js", () => ({
  appendRequestLog: vi.fn(async () => {}),
  saveRequestDetail: vi.fn(async () => {}),
  saveRequestUsage: vi.fn(async () => {})
}));

const { FORMATS } = await import("../../open-sse/translator/formats.js");
const { reduceResponsesEvent } = await import("../../open-sse/translator/concerns/responsesAccumulator.js");
const { handleStreamingResponse } = await import("../../open-sse/handlers/chatCore/streamingHandler.js");

function responsesProviderResponse() {
  return new Response("event: response.completed\ndata: {}\n\n", {
    headers: { "content-type": "text/event-stream" }
  });
}

async function handleWithUserAgent(userAgent) {
  return handleStreamingResponse({
    providerResponse: responsesProviderResponse(),
    provider: "codex",
    model: "gpt-5.3-codex",
    sourceFormat: FORMATS.OPENAI_RESPONSES,
    targetFormat: FORMATS.OPENAI_RESPONSES,
    userAgent,
    body: { model: "gpt-5.3-codex", stream: true },
    stream: true,
    requestStartTime: Date.now(),
    connectionId: "test-connection",
    clientRawRequest: { endpoint: "/v1/responses" },
    streamController: {}
  });
}

async function handleResponsesClient(targetFormat) {
  return handleStreamingResponse({
    providerResponse: responsesProviderResponse(),
    provider: "test-provider",
    model: "test-model",
    sourceFormat: FORMATS.OPENAI_RESPONSES,
    targetFormat,
    userAgent: "9router-test-client/1.0",
    body: { model: "test-model", stream: true },
    stream: true,
    requestStartTime: Date.now(),
    connectionId: "test-connection",
    clientRawRequest: { endpoint: "/v1/responses" },
    streamController: {}
  });
}

describe("Responses streaming handler CLI passthrough", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it.each(["Droid/1.2.3", "codex-cli/0.42.0"])(
    "keeps the raw Responses-to-Responses path for %s",
    async userAgent => {
      const result = await handleWithUserAgent(userAgent);

      expect(result.success).toBe(true);
      expect(passthroughMock).toHaveBeenCalledOnce();
      const accumulator = passthroughMock.mock.calls[0][7];
      expect(accumulator).toEqual(expect.objectContaining({ model: "gpt-5.3-codex" }));
      expect(translateMock).not.toHaveBeenCalled();

      reduceResponsesEvent(accumulator, {
        type: "response.created",
        response: { id: "resp_handler_passthrough", status: "in_progress", output: [] }
      });
      reduceResponsesEvent(accumulator, {
        type: "response.output_text.delta",
        output_index: 0,
        item_id: "msg_handler_passthrough",
        delta: "partial from raw passthrough"
      });
      const abortTerminal = pipeMock.mock.calls[0][3]();
      const terminalText = new TextDecoder().decode(abortTerminal);
      expect(terminalText).toContain('"id":"resp_handler_passthrough"');
      expect(terminalText).toContain('"text":"partial from raw passthrough"');
    }
  );

  it("continues repairing the Responses path for non-CLI clients", async () => {
    const result = await handleWithUserAgent("9router-test-client/1.0");

    expect(result.success).toBe(true);
    expect(translateMock).toHaveBeenCalledOnce();
    expect(passthroughMock).not.toHaveBeenCalled();
  });

  it.each([FORMATS.OPENAI, FORMATS.CLAUDE, FORMATS.GEMINI])(
    "installs a Responses abort terminal for %s provider output",
    async targetFormat => {
      await handleResponsesClient(targetFormat);

      const accumulator = translateMock.mock.calls[0][10];
      reduceResponsesEvent(accumulator, {
        type: "response.created",
        response: { id: `resp_${targetFormat}`, status: "in_progress", output: [] }
      });
      reduceResponsesEvent(accumulator, {
        type: "response.output_text.delta",
        output_index: 0,
        item_id: `msg_${targetFormat}`,
        delta: `partial ${targetFormat}`
      });

      const onAbortTerminal = pipeMock.mock.calls[0][3];
      const terminalText = new TextDecoder().decode(onAbortTerminal());
      expect(terminalText).toContain("event: response.failed");
      expect(terminalText).toContain(`\"id\":\"resp_${targetFormat}\"`);
      expect(terminalText).toContain(`\"text\":\"partial ${targetFormat}\"`);
      expect(terminalText.match(/event: response\.failed/g)).toHaveLength(1);
      expect(terminalText.match(/data: \[DONE\]/g)).toHaveLength(1);
    }
  );
});
