import { BaseExecutor } from "./base.js";
import { PROVIDERS } from "../config/providers.js";
import { resolveKiroModel } from "../config/kiroConstants.js";
import { v4 as uuidv4 } from "uuid";
import { refreshKiroToken } from "../services/tokenRefresh.js";
import { SSE_DONE, SSE_HEADERS } from "../utils/sseConstants.js";
import { getCapabilitiesForModel } from "../providers/capabilities.js";
import { STREAM_FIRST_CHUNK_TIMEOUT_MS } from "../config/runtimeConfig.js";
import { getKiroUsage } from "../services/usage/kiro.js";
import { KIRO_CREDIT_EXHAUSTION_PROBE_MS } from "../config/errorConfig.js";

const KIRO_TOOL_CALL_WRAPPER = "tool_call";
const KIRO_TOOL_CALL_REPAIR_BUFFER_MAX_BYTES_ENV = "KIRO_TOOL_CALL_REPAIR_BUFFER_MAX_BYTES";
const KIRO_TOOL_CALL_REPAIR_TIMEOUT_MS_ENV = "KIRO_TOOL_CALL_REPAIR_TIMEOUT_MS";
const KIRO_TOOL_CALL_REPAIR_TTFT_TIMEOUT_MS_ENV = "KIRO_TOOL_CALL_REPAIR_TTFT_TIMEOUT_MS";
const KIRO_TOOL_CALL_REPAIR_STALL_TIMEOUT_MS_ENV = "KIRO_TOOL_CALL_REPAIR_STALL_TIMEOUT_MS";
const KIRO_TOOL_CALL_REPAIR_BUFFER_MAX_BYTES = 8 * 1024 * 1024;
const KIRO_SHORT_FINAL_MAX_CHARS = 800;
const KIRO_TERMINAL_PROVENANCE = Object.freeze({
  MESSAGE_STOP: "message_stop_event",
  CLEAN_EOF: "clean_eventstream_eof",
  INCOMPLETE_FRAME: "incomplete_eventstream_frame",
  CORRUPT_FRAME: "corrupt_eventstream_frame",
  UPSTREAM_ERROR: "upstream_eventstream_error",
  EMPTY_RESPONSE: "empty_response_eof",
  MISSING_BODY: "missing_response_body"
});
const KIRO_DIAGNOSTIC_EVENT_TYPES = Object.freeze([
  "assistantResponseEvent",
  "reasoningContentEvent",
  "codeEvent",
  "toolUseEvent",
  "messageStopEvent",
  "contextUsageEvent",
  "meteringEvent",
  "metricsEvent"
]);
const KIRO_TOOL_CALL_REPAIR_INSTRUCTION = [
  "Retry the previous response because its Kiro tool_call wrapper was malformed.",
  "If you use the wrapper tool named tool_call, its input must be a JSON object with a non-empty string name and an arguments field.",
  "Do not emit a tool_call wrapper without input.name and input.arguments."
].join(" ");
const KIRO_ELLIPSIS_REPAIR_INSTRUCTION = [
  "Retry the previous response because it ended with only an ellipsis instead of a complete answer.",
  "Use the existing conversation and tool results to provide the full final answer.",
  "Do not answer with only ... or …."
].join(" ");
const KIRO_SHORT_FINAL_REPAIR_INSTRUCTION = [
  "Retry the previous response because its short final only announced a future action instead of reporting the result.",
  "Complete the announced check now and return the result or a concrete blocker.",
  "Do not repeat a progress update as the final answer."
].join(" ");
const KIRO_SHORT_FINAL_PREFIXES = Object.freeze([
  "現在",
  "接著",
  "接下來",
  "下一步",
  "我只再",
  "我會重新抓取",
  "next",
  "now",
  "then",
  "i'll",
  "i will",
  "i am going to",
  "i need to",
  "let me"
]);
const KIRO_SHORT_FUTURE_ACTION_PATTERN = /^(?:(?:(?:現在|接著|接下來|下一步)[，,:：\s]*(?:我(?:只)?(?:會|要|將|再)?\s*)?|我只再)(?:補|查|確認|驗證|追(?:查|蹤)?|繼續|檢查|測試)|我會重新抓取(?=[\s\S]*調查會以[\s\S]+為準[。.!]?$)|(?:(?:next|now|then)\b[\s,:-]*)?(?:i(?:'ll| will| am going to| need to)|let me)\s+(?:verify|check|confirm|validate|investigate|trace|continue|follow up|test)\b)/iu;
const KIRO_SHORT_FINAL_USER_WAIT_PATTERN = /(?:請(?:你|先)|你(?:先|需要|可以|提供|確認|批准|允許)|等待(?:你|使用者)|等你|核准|同意|授權|\b(?:after|when|once)\s+you\b|\byour\s+(?:approval|confirmation|permission|input)\b|\bwait(?:ing)?\s+for\s+you\b|\bplease\s+(?:approve|confirm|provide|send)\b)/iu;
const KIRO_SHORT_FINAL_COMPLETE_PATTERN = /(?:已(?:經)?完成|完成(?:了|驗證|確認)|修復完成|確認無誤|驗證(?:完成|通過)|測試(?:均)?通過|結論|總結|\b(?:done|completed|fixed|verified|confirmed|passed|in conclusion|summary)\b|\b(?:is|are) complete\b)/iu;
const CRC32_TABLE = Uint32Array.from({ length: 256 }, (_, index) => {
  let value = index;
  for (let bit = 0; bit < 8; bit++) {
    value = (value >>> 1) ^ ((value & 1) ? 0xedb88320 : 0);
  }
  return value >>> 0;
});
const sharedEncoder = new TextEncoder();
const sharedDecoder = new TextDecoder();

function encodeSSE(value) {
  return sharedEncoder.encode(value);
}

function closeSSEController(controller) {
  if (typeof controller.terminate === "function") {
    controller.terminate();
  } else if (typeof controller.close === "function") {
    controller.close();
  }
}

function envInt(name, fallback) {
  const raw = process.env?.[name];
  if (raw == null || raw === "") return fallback;
  const parsed = parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function buildKiroToolCallRepairBody(body, invalidMessage) {
  const repaired = JSON.parse(JSON.stringify(body || {}));
  const reason = String(invalidMessage || "invalid tool_call payload").slice(0, 300);
  const instruction = `${KIRO_TOOL_CALL_REPAIR_INSTRUCTION} Previous validation error: ${reason}`;
  repaired.systemPrompt = repaired.systemPrompt
    ? `${repaired.systemPrompt}\n\n${instruction}`
    : instruction;
  return repaired;
}

function buildKiroEllipsisRepairBody(body) {
  const repaired = JSON.parse(JSON.stringify(body || {}));
  repaired.systemPrompt = repaired.systemPrompt
    ? `${repaired.systemPrompt}\n\n${KIRO_ELLIPSIS_REPAIR_INSTRUCTION}`
    : KIRO_ELLIPSIS_REPAIR_INSTRUCTION;
  return repaired;
}

function buildKiroShortFinalRepairBody(body) {
  const repaired = JSON.parse(JSON.stringify(body || {}));
  repaired.systemPrompt = repaired.systemPrompt
    ? `${repaired.systemPrompt}\n\n${KIRO_SHORT_FINAL_REPAIR_INSTRUCTION}`
    : KIRO_SHORT_FINAL_REPAIR_INSTRUCTION;
  return repaired;
}

function makeAbortError(reason) {
  const error = new Error(reason || "Request aborted");
  error.name = "AbortError";
  return error;
}

function combineAbortSignals(signals) {
  const activeSignals = signals.filter(Boolean);
  if (activeSignals.length === 0) return { signal: undefined, cleanup: () => {} };
  if (activeSignals.length === 1) return { signal: activeSignals[0], cleanup: () => {} };

  const controller = new AbortController();
  const listeners = [];
  const abortFrom = (signal) => {
    if (!controller.signal.aborted) {
      controller.abort(signal.reason || makeAbortError("Request aborted"));
    }
  };

  for (const signal of activeSignals) {
    if (signal.aborted) {
      abortFrom(signal);
      break;
    }
    const listener = () => abortFrom(signal);
    signal.addEventListener("abort", listener, { once: true });
    listeners.push([signal, listener]);
  }

  return {
    signal: controller.signal,
    cleanup: () => {
      for (const [signal, listener] of listeners) {
        signal.removeEventListener("abort", listener);
      }
    }
  };
}

function throwIfAborted(signal) {
  if (signal?.aborted) {
    throw makeAbortError(signal.reason?.message || signal.reason || "Request aborted");
  }
}

async function readWithTimeout(reader, signal, timeoutMs, timeoutMessage) {
  throwIfAborted(signal);

  let abortHandler;
  let timeoutId;
  const abortPromise = new Promise((_, reject) => {
    abortHandler = () => reject(makeAbortError(signal?.reason?.message || signal?.reason || "Request aborted"));
    signal?.addEventListener?.("abort", abortHandler, { once: true });
  });
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(timeoutMessage)), timeoutMs);
  });

  try {
    return await Promise.race([reader.read(), abortPromise, timeoutPromise]);
  } finally {
    if (abortHandler) signal?.removeEventListener?.("abort", abortHandler);
    if (timeoutId) clearTimeout(timeoutId);
  }
}

function concatChunks(chunks, totalBytes) {
  const out = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

function isEllipsisOnly(value) {
  const normalized = String(value || "").trim();
  return normalized === "..." || normalized === "…";
}

function isPossibleEllipsisPrefix(value) {
  const normalized = String(value || "").trim();
  return normalized === "" || normalized === "." || normalized === ".." || isEllipsisOnly(normalized);
}

function normalizeKiroShortFinal(value) {
  return String(value || "").trim().replaceAll("’", "'");
}

function isPossibleShortFutureActionPrefix(value) {
  const normalized = normalizeKiroShortFinal(value).toLowerCase();
  if (!normalized || normalized.length > KIRO_SHORT_FINAL_MAX_CHARS) return false;
  return KIRO_SHORT_FINAL_PREFIXES.some((prefix) =>
    prefix.startsWith(normalized) || normalized.startsWith(prefix)
  );
}

function isShortFutureActionFinal(value) {
  const normalized = normalizeKiroShortFinal(value);
  return normalized.length > 0 &&
    normalized.length <= KIRO_SHORT_FINAL_MAX_CHARS &&
    KIRO_SHORT_FUTURE_ACTION_PATTERN.test(normalized) &&
    !KIRO_SHORT_FINAL_USER_WAIT_PATTERN.test(normalized) &&
    !KIRO_SHORT_FINAL_COMPLETE_PATTERN.test(normalized);
}

function classifyKiroGatedOutput(state) {
  if (state.hasToolCalls) return null;
  const visible = state.content.trim();
  if (visible) {
    if (isEllipsisOnly(visible)) return "ellipsis";
    if (isShortFutureActionFinal(visible)) return "short_final";
    return null;
  }
  return isEllipsisOnly(state.reasoningContent) ? "ellipsis" : null;
}

function inspectRepairSSEChunk(chunk, state) {
  const text = sharedDecoder.decode(chunk);
  let safeToStream = false;

  for (const line of text.split("\n")) {
    if (!line.startsWith("data: ")) continue;
    const data = line.slice(6).trim();
    if (!data || data === "[DONE]") continue;

    let event;
    try {
      event = JSON.parse(data);
    } catch {
      safeToStream = true;
      continue;
    }

    if (event?.error) continue;

    for (const choice of event?.choices || []) {
      const delta = choice?.delta || {};
      if (Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0) {
        state.hasToolCalls = true;
        safeToStream = true;
      }

      if (typeof delta.content === "string") {
        state.content += delta.content;
        if (!isPossibleEllipsisPrefix(state.content) &&
            !isPossibleShortFutureActionPrefix(state.content)) {
          safeToStream = true;
        }
      }

      if (typeof delta.reasoning_content === "string") {
        state.reasoningContent += delta.reasoning_content;
      }
    }
  }

  return { safeToStream };
}

function formatKiroEllipsisRetryFailure() {
  return new Response(JSON.stringify({
    error: {
      message: "Kiro returned an ellipsis-only final response after one retry",
      type: "upstream_error",
      code: "kiro_ellipsis_retry_failed"
    }
  }), {
    status: 502,
    statusText: "Bad Gateway",
    headers: { "Content-Type": "application/json" }
  });
}

function formatKiroShortFinalRetryFailure() {
  return new Response(JSON.stringify({
    error: {
      message: "Kiro returned a short future-action final after one retry",
      type: "upstream_error",
      code: "kiro_short_final_retry_failed"
    }
  }), {
    status: 502,
    statusText: "Bad Gateway",
    headers: { "Content-Type": "application/json" }
  });
}

function createKiroEventCounts() {
  return Object.fromEntries([
    ...KIRO_DIAGNOSTIC_EVENT_TYPES.map((eventType) => [eventType, 0]),
    ["other", 0]
  ]);
}

function incrementKiroEventCount(eventCounts, eventType) {
  const key = KIRO_DIAGNOSTIC_EVENT_TYPES.includes(eventType) ? eventType : "other";
  eventCounts[key]++;
}

function sanitizeKiroTerminalDiagnostics(diagnostics) {
  return {
    terminal_provenance: diagnostics?.terminal_provenance || KIRO_TERMINAL_PROVENANCE.EMPTY_RESPONSE,
    event_counts: { ...createKiroEventCounts(), ...(diagnostics?.event_counts || {}) },
    incomplete_frame_bytes: Number(diagnostics?.incomplete_frame_bytes) || 0
  };
}

function isKiroTerminalFailure(provenance) {
  return provenance === KIRO_TERMINAL_PROVENANCE.INCOMPLETE_FRAME ||
    provenance === KIRO_TERMINAL_PROVENANCE.CORRUPT_FRAME ||
    provenance === KIRO_TERMINAL_PROVENANCE.UPSTREAM_ERROR ||
    provenance === KIRO_TERMINAL_PROVENANCE.EMPTY_RESPONSE ||
    provenance === KIRO_TERMINAL_PROVENANCE.MISSING_BODY;
}

function hasKiroModelOutput(state) {
  return state.hasTextContent ||
    state.hasReasoningContent ||
    state.hasCodeContent ||
    state.hasToolCalls;
}

function logKiroTerminalDiagnostics(model, attempt, diagnostics) {
  const safe = sanitizeKiroTerminalDiagnostics(diagnostics);
  const entry = JSON.stringify({ model, attempt, ...safe });
  if (isKiroTerminalFailure(safe.terminal_provenance)) {
    console.warn(`[Kiro] Terminal integrity failure ${entry}`);
  }
  return safe;
}

function formatKiroMissingTerminalRetryFailure(attempts) {
  return new Response(JSON.stringify({
    error: {
      message: "Kiro stream ended incompletely or without model output after one bounded retry",
      type: "upstream_error",
      code: "kiro_missing_terminal_retry_failed",
      details: {
        attempts: attempts.map(sanitizeKiroTerminalDiagnostics)
      }
    }
  }), {
    status: 502,
    statusText: "Bad Gateway",
    headers: { "Content-Type": "application/json" }
  });
}

function formatKiroMissingTerminalSSE(diagnostics) {
  return encodeSSE(`data: ${JSON.stringify({
    error: {
      message: "Kiro stream ended incompletely or without model output",
      type: "upstream_error",
      code: "kiro_missing_terminal",
      details: sanitizeKiroTerminalDiagnostics(diagnostics)
    }
  })}\n\ndata: [DONE]\n\n`);
}

function formatKiroToolCallRepairError(message, code = "kiro_tool_call_repair_failed") {
  return encodeSSE(`data: ${JSON.stringify({
    error: {
      message,
      type: "invalid_request_error",
      code
    }
  })}\n\ndata: [DONE]\n\n`);
}

function once(fn) {
  let called = false;
  return () => {
    if (called) return;
    called = true;
    fn?.();
  };
}

function prependChunkToReader(firstChunk, reader, { onCancel, onDone } = {}) {
  let cancelled = false;
  const finish = once(onDone);
  return new ReadableStream({
    async start(controller) {
      try {
        if (firstChunk?.byteLength) controller.enqueue(firstChunk);
        while (!cancelled) {
          const { done, value } = await reader.read();
          if (done) break;
          if (!cancelled) controller.enqueue(value);
        }
        if (!cancelled) controller.close();
      } catch (error) {
        if (!cancelled) controller.error(error);
      } finally {
        finish();
      }
    },

    async cancel(reason) {
      cancelled = true;
      try {
        onCancel?.(reason);
      } finally {
        try {
          await reader.cancel(reason);
        } finally {
          finish();
        }
      }
    }
  });
}

function parseKiroToolInput(toolInput) {
  if (typeof toolInput === "string") {
    try {
      return JSON.parse(toolInput);
    } catch (error) {
      throw new Error(`Invalid Kiro tool_call payload: input must be valid JSON (${error.message})`);
    }
  }
  return toolInput;
}

function validateKiroToolName(toolUse) {
  const toolName = typeof toolUse?.name === "string" ? toolUse.name.trim() : "";
  if (!toolName) {
    throw new Error("Invalid Kiro toolUseEvent: missing tool name");
  }

  return toolName;
}

function getBufferedKiroToolInput(toolCall) {
  if (toolCall.inputKind === "string") return toolCall.inputText || "";
  return toolCall.inputObject;
}

function appendBufferedKiroToolInput(toolCall, toolInput) {
  if (toolInput === undefined) return;

  if (typeof toolInput === "string") {
    if (toolCall.inputKind && toolCall.inputKind !== "string") {
      throw new Error("Invalid Kiro tool_call payload: mixed input fragment types");
    }
    toolCall.inputKind = "string";
    toolCall.inputText = `${toolCall.inputText || ""}${toolInput}`;
    return;
  }

  if (toolInput && typeof toolInput === "object" && !Array.isArray(toolInput)) {
    if (toolCall.inputKind && toolCall.inputKind !== "object") {
      throw new Error("Invalid Kiro tool_call payload: mixed input fragment types");
    }
    toolCall.inputKind = "object";
    toolCall.inputObject = toolInput;
  }
}

function validateKiroToolCallWrapperInput(toolInput) {
  if (toolInput === undefined) {
    throw new Error("Invalid Kiro tool_call payload: missing input");
  }

  const input = parseKiroToolInput(toolInput);
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw new Error("Invalid Kiro tool_call payload: input must be an object with name and arguments");
  }

  const nestedName = typeof input.name === "string" ? input.name.trim() : "";
  if (!nestedName) {
    throw new Error("Invalid Kiro tool_call payload: missing nested MCP tool name at input.name");
  }

  if (!Object.prototype.hasOwnProperty.call(input, "arguments")) {
    throw new Error("Invalid Kiro tool_call payload: missing nested MCP tool arguments at input.arguments");
  }
}

/**
 * Validate a complete Kiro toolUseEvent payload. Streaming wrapper tool_call
 * fragments must be buffered first; otherwise an init/delta fragment without
 * the final nested input would be rejected as malformed.
 */
export function validateKiroToolUse(toolUse) {
  const toolName = validateKiroToolName(toolUse);
  if (toolName !== KIRO_TOOL_CALL_WRAPPER) {
    return;
  }

  validateKiroToolCallWrapperInput(toolUse.input);
}

function emitKiroToolCallValidationError(controller, state, message, options = {}) {
  const error = {
    error: {
      message,
      type: "invalid_request_error",
      code: options.invalidToolCallErrorCode || "invalid_kiro_tool_call"
    }
  };
  state.invalidToolCall = true;
  state.finishEmitted = true;
  state.doneSent = true;
  options.onInvalidToolCall?.(message);
  if (!options.suppressInvalidToolCallError) {
    controller.enqueue(encodeSSE(`data: ${JSON.stringify(error)}\n\n`));
    controller.enqueue(encodeSSE(SSE_DONE));
  }
  closeSSEController(controller);
}

/**
 * Confirmed-exhaustion signal for Kiro's monthly credit quota. Verified against a real
 * CodeWhisperer GenerateAssistantResponse 402 body (AWS ServiceQuotaExceededException):
 *   { "message": "You have reached the limit.",
 *     "cause": { "$metadata": { "httpStatusCode": 402 }, "name": "ServiceQuotaExceededException",
 *                "reason": "MONTHLY_REQUEST_COUNT" } }
 * Some surfaces flatten `cause.name`/`cause.reason` onto the top-level object instead, so both
 * shapes are checked. Any other 402 (bad payment method, suspended account, unrecognized shape)
 * is left ambiguous and keeps the existing generic 402 cooldown (open-sse/config/errorConfig.js).
 */
const KIRO_QUOTA_EXCEEDED_EXCEPTION = "ServiceQuotaExceededException";
const KIRO_QUOTA_EXCEEDED_REASON = "MONTHLY_REQUEST_COUNT";

/** Follow-up quota-lookup timeout — bounds how long a confirmed-402 error response can wait. */
const KIRO_RESET_LOOKUP_TIMEOUT_MS = 8000;

function isConfirmedKiroCreditExhaustion(bodyText) {
  if (!bodyText) return false;
  try {
    const json = JSON.parse(bodyText);
    const name = json?.name ?? json?.cause?.name;
    const reason = json?.reason ?? json?.cause?.reason;
    if (name === KIRO_QUOTA_EXCEEDED_EXCEPTION && reason === KIRO_QUOTA_EXCEEDED_REASON) return true;
  } catch { /* not JSON — fall through to the text-based check below */ }
  const lower = bodyText.toLowerCase();
  return lower.includes(KIRO_QUOTA_EXCEEDED_EXCEPTION.toLowerCase())
    && lower.includes(KIRO_QUOTA_EXCEEDED_REASON.toLowerCase());
}

/** Earliest resetAt (ms epoch) among fully-depleted quota buckets, or null if none/unknown. */
function earliestDepletedResetMs(quotas) {
  let earliest = null;
  for (const quota of Object.values(quotas || {})) {
    if (!quota || quota.unlimited || !(quota.total > 0) || quota.remaining > 0 || !quota.resetAt) continue;
    const ms = new Date(quota.resetAt).getTime();
    if (!Number.isFinite(ms)) continue;
    if (earliest === null || ms < earliest) earliest = ms;
  }
  return earliest;
}

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((resolve) => setTimeout(() => resolve(null), ms)),
  ]);
}

/**
 * KiroExecutor - Executor for Kiro AI (AWS CodeWhisperer)
 * Uses AWS CodeWhisperer streaming API with AWS EventStream binary format
 */
export class KiroExecutor extends BaseExecutor {
  constructor() {
    super("kiro", PROVIDERS.kiro);
  }

  buildHeaders(credentials, stream = true) {
    const headers = {
      ...this.config.headers,
      "Amz-Sdk-Request": "attempt=1; max=3",
      "Amz-Sdk-Invocation-Id": uuidv4()
    };

    // API-key auth: the key is stored as accessToken and sent as a bearer token
    // exactly like an OAuth access token, but with an extra `tokentype: API_KEY`
    // header so CodeWhisperer treats it as a long-lived API key rather than an
    // OIDC/social access token. Mirrors the Kiro IDE headless-auth behavior.
    // Enterprise / Microsoft Entra (external_idp) tokens are OAuth access tokens,
    // but CodeWhisperer requires TokenType=EXTERNAL_IDP to bind them to profiles.
    const authMethod = credentials?.providerSpecificData?.authMethod;
    const isApiKey = authMethod === "api_key";
    const isExternalIdp = authMethod === "external_idp";

    const apiKey = credentials?.apiKey || (isApiKey ? credentials?.accessToken : null);
    if (isApiKey && apiKey) {
      headers["Authorization"] = `Bearer ${apiKey}`;
      headers["tokentype"] = "API_KEY";
    } else if (credentials.accessToken) {
      headers["Authorization"] = `Bearer ${credentials.accessToken}`;
      if (isExternalIdp) {
        headers["TokenType"] = "EXTERNAL_IDP";
      }
    }

    return headers;
  }

  /**
   * Auth-aware endpoint ordering.
   *
   * API-key Kiro connections store a raw CodeWhisperer credential (validated
   * against codewhisperer.us-east-1.amazonaws.com via ListAvailableProfiles).
   * The Kiro IDE gateway (runtime.*.kiro.dev) expects Kiro OIDC/social tokens
   * and rejects an `tokentype: API_KEY` token with 401/403 — which
   * BaseExecutor.execute() returns immediately (only 429 / network errors fall
   * through to the next host). So for api-key auth we must try the *.amazonaws.com
   * CodeWhisperer hosts FIRST, mirroring the Kiro-Go reference fork which never
   * routes api-key traffic through kiro.dev. External IdP enterprise tokens also
   * use the CodeWhisperer surface, with the `TokenType: EXTERNAL_IDP` header.
   * Other OAuth methods keep the default order (kiro.dev first) since their
   * tokens are what that gateway accepts.
   */
  getOrderedBaseUrls(credentials) {
    const baseUrls = this.getBaseUrls();
    const authMethod = credentials?.providerSpecificData?.authMethod;
    // IAM Identity Center (idc) tokens are AWS SSO access tokens — the same
    // family as external_idp/api_key. The kiro.dev gateway rejects them with
    // 403 "bearer token invalid", so they must hit the CodeWhisperer
    // *.amazonaws.com surface, and in the region the token was minted in
    // (the baseUrls are hardcoded us-east-1).
    const isCodeWhispererSurface =
      authMethod === "api_key" || authMethod === "external_idp" || authMethod === "idc";
    if (!isCodeWhispererSurface) return baseUrls;

    const region = (credentials?.providerSpecificData?.region || "us-east-1").trim();
    const regionalize = (u) =>
      region && region !== "us-east-1" && u.includes("amazonaws.com")
        ? u.replace(/([a-z]+)\.[a-z0-9-]+\.amazonaws\.com/, `$1.${region}.amazonaws.com`)
        : u;

    const amazon = baseUrls.filter((u) => u.includes("amazonaws.com")).map(regionalize);
    const others = baseUrls.filter((u) => !u.includes("amazonaws.com"));
    return amazon.length > 0 ? [...amazon, ...others] : baseUrls;
  }

  buildUrl(model, stream, urlIndex = 0, credentials = null) {
    const baseUrls = this.getOrderedBaseUrls(credentials);
    return baseUrls[urlIndex] || baseUrls[0] || this.config.baseUrl;
  }

  transformRequest(model, body, stream, credentials) {
    return body;
  }

  /**
   * Classify a Kiro 402 as confirmed monthly-credit exhaustion vs. ambiguous (bad payment
   * method, suspended account, unrecognized upstream shape — see isConfirmedKiroCreditExhaustion
   * doc above). Only a confirmed match gets a precise cooldown; everything else falls through
   * to the base classifier and keeps the existing generic 402 cooldown unchanged.
   *
   * The 402 body itself never carries a reset time, so on a confirmed match this makes a
   * best-effort follow-up call to Kiro's own quota API (GetUsageLimits) for the trustworthy
   * `resetAt` already surfaced there (services/usage/kiro.js). If that lookup is unreachable,
   * times out, or shows nothing depleted, resetsAtMs falls back to a bounded daily-probe
   * window rather than guessing a date — markAccountUnavailable caps either case at
   * KIRO_CREDIT_EXHAUSTION_PROBE_MS (see errorConfig.js) so the account is retried at most
   * about once a day until it recovers.
   */
  async parseError(response, bodyText, credentials, proxyOptions) {
    if (response.status !== 402 || !isConfirmedKiroCreditExhaustion(bodyText)) {
      return super.parseError(response, bodyText);
    }

    let resetsAtMs = null;
    try {
      const accessToken = credentials?.apiKey || credentials?.accessToken;
      const usage = await withTimeout(
        getKiroUsage(accessToken, credentials?.providerSpecificData, proxyOptions),
        KIRO_RESET_LOOKUP_TIMEOUT_MS
      );
      resetsAtMs = earliestDepletedResetMs(usage?.quotas);
    } catch { /* best-effort only — fall back to the daily probe below */ }

    if (!resetsAtMs || resetsAtMs <= Date.now()) {
      resetsAtMs = Date.now() + KIRO_CREDIT_EXHAUSTION_PROBE_MS;
    }

    return { status: 402, message: "Kiro monthly credit limit reached", resetsAtMs };
  }

  /**
   * Kiro execute — delegate to BaseExecutor for endpoint fallback + retry, then
   * transform the binary AWS EventStream into OpenAI-shaped SSE on success.
   *
   * BaseExecutor.execute() walks config.baseUrls (runtime.us-east-1.kiro.dev →
   * codewhisperer → q) advancing to the next host on 429 (shouldRetry) and on
   * network/5xx errors, while tryRetry handles in-place retries per `retry: {429: 2}`.
   * Note: api-key connections reorder these so the *.amazonaws.com hosts come
   * first — see getOrderedBaseUrls/buildUrl above.
   * Note: the baseUrls are alternate surfaces of one regional service, so rotation
   * is edge-level failover — it does not grant fresh 429 quota. Per-account 429
   * spreading is handled upstream by account rotation in sse/handlers/chat.js.
   *
   * Errors are returned untransformed so the upstream handler can read the body,
   * classify the status, and trigger account fallback/cooldown.
   */
  async execute(args) {
    const result = await super.execute(args);
    if (result?.response?.ok) {
      return this.createToolCallRepairResult(result, args);
    }
    return result;
  }

  async createToolCallRepairResult(firstResult, args) {
    const executeRaw = (nextArgs) => BaseExecutor.prototype.execute.call(this, nextArgs);
    const repairController = new AbortController();
    const combined = combineAbortSignals([args.signal, repairController.signal]);
    let cleanupInFinally = true;
    const maxBufferBytes = envInt(
      KIRO_TOOL_CALL_REPAIR_BUFFER_MAX_BYTES_ENV,
      KIRO_TOOL_CALL_REPAIR_BUFFER_MAX_BYTES
    );
    const legacyTimeoutMs = envInt(KIRO_TOOL_CALL_REPAIR_TIMEOUT_MS_ENV, STREAM_FIRST_CHUNK_TIMEOUT_MS);
    const ttftTimeoutMs = envInt(KIRO_TOOL_CALL_REPAIR_TTFT_TIMEOUT_MS_ENV, legacyTimeoutMs);
    const stallTimeoutMs = envInt(KIRO_TOOL_CALL_REPAIR_STALL_TIMEOUT_MS_ENV, legacyTimeoutMs);
    const toolCallRepairEnabled = args.credentials?.providerSpecificData?.kiroToolCallRepair !== false &&
      process.env.KIRO_TOOL_CALL_REPAIR !== "false";

    // Hold only an ellipsis/future-action-shaped prefix or malformed output.
    // Ordinary semantic output and tool calls stream immediately; clean EOF
    // finalizes the turn, while empty/incomplete streams stay private for repair.
    try {
      const firstAttempt = await this.openToolCallRepairGate(firstResult.response, args, {
        signal: combined.signal,
        maxBufferBytes,
        ttftTimeoutMs,
        stallTimeoutMs,
        suppressInvalidToolCallError: true,
        attempt: "initial"
      });

      if (firstAttempt.kind === "stream") {
        cleanupInFinally = false;
        firstResult.response = new Response(
          prependChunkToReader(firstAttempt.firstChunk, firstAttempt.reader, {
            onCancel: (reason) => repairController.abort(reason || "cancelled"),
            onDone: combined.cleanup
          }),
          {
            status: firstResult.response.status,
            statusText: firstResult.response.statusText,
            headers: { ...SSE_HEADERS }
          }
        );
        return firstResult;
      }

      if (firstAttempt.kind === "complete") {
        firstResult.response = new Response(firstAttempt.bytes, {
          status: firstResult.response.status,
          statusText: firstResult.response.statusText,
          headers: { ...SSE_HEADERS }
        });
        return firstResult;
      }

      if (firstAttempt.kind === "buffer_exceeded") {
        firstResult.response = new Response(formatKiroToolCallRepairError(
          `Kiro tool_call repair buffer exceeded ${maxBufferBytes} bytes`,
          "kiro_tool_call_repair_buffer_exceeded"
        ), {
          status: firstResult.response.status,
          statusText: firstResult.response.statusText,
          headers: { ...SSE_HEADERS }
        });
        return firstResult;
      }

      if (firstAttempt.kind === "invalid" && !toolCallRepairEnabled) {
        firstResult.response = new Response(formatKiroToolCallRepairError(
          firstAttempt.invalidToolCall || "Invalid Kiro tool_call payload",
          "invalid_kiro_tool_call"
        ), {
          status: firstResult.response.status,
          statusText: firstResult.response.statusText,
          headers: { ...SSE_HEADERS }
        });
        return firstResult;
      }

      const repairingEllipsis = firstAttempt.kind === "ellipsis";
      const repairingShortFinal = firstAttempt.kind === "short_final";
      const repairingMissingTerminal = firstAttempt.kind === "missing_terminal";
      if (repairingEllipsis) {
        console.warn(`[Kiro] Ellipsis-only final response detected for ${args.model}; retrying once`);
      } else if (repairingShortFinal) {
        console.warn(`[Kiro] Short future-action final detected for ${args.model}; retrying once`);
      }
      const repairBody = repairingMissingTerminal
        ? JSON.parse(JSON.stringify(args.body || {}))
        : repairingEllipsis
          ? buildKiroEllipsisRepairBody(args.body)
          : repairingShortFinal
            ? buildKiroShortFinalRepairBody(args.body)
            : buildKiroToolCallRepairBody(args.body, firstAttempt.invalidToolCall);
      const retryResult = await executeRaw({
        ...args,
        body: repairBody,
        signal: combined.signal
      });

      if (!retryResult?.response?.ok) {
        return retryResult;
      }

      const retryAttempt = await this.openToolCallRepairGate(retryResult.response, args, {
        signal: combined.signal,
        maxBufferBytes,
        ttftTimeoutMs,
        stallTimeoutMs,
        suppressInvalidToolCallError: false,
        invalidToolCallErrorCode: "kiro_tool_call_repair_retry_failed",
        attempt: "retry"
      });

      if (retryAttempt.kind === "stream") {
        cleanupInFinally = false;
        retryResult.response = new Response(
          prependChunkToReader(retryAttempt.firstChunk, retryAttempt.reader, {
            onCancel: (reason) => repairController.abort(reason || "cancelled"),
            onDone: combined.cleanup
          }),
          {
            status: retryResult.response.status,
            statusText: retryResult.response.statusText,
            headers: { ...SSE_HEADERS }
          }
        );
        return retryResult;
      }

      if (retryAttempt.kind === "complete") {
        retryResult.response = new Response(retryAttempt.bytes, {
          status: retryResult.response.status,
          statusText: retryResult.response.statusText,
          headers: { ...SSE_HEADERS }
        });
        return retryResult;
      }

      if (retryAttempt.kind === "ellipsis") {
        console.warn(`[Kiro] Ellipsis-only final response persisted after retry for ${args.model}`);
        retryResult.response = formatKiroEllipsisRetryFailure();
        return retryResult;
      }

      if (retryAttempt.kind === "short_final") {
        console.warn(`[Kiro] Short future-action final persisted after retry for ${args.model}`);
        retryResult.response = formatKiroShortFinalRetryFailure();
        return retryResult;
      }

      if (retryAttempt.kind === "missing_terminal") {
        retryResult.response = formatKiroMissingTerminalRetryFailure([
          firstAttempt.terminalDiagnostics,
          retryAttempt.terminalDiagnostics
        ].filter(Boolean));
        return retryResult;
      }

      retryResult.response = new Response(formatKiroToolCallRepairError(
        retryAttempt.kind === "buffer_exceeded"
          ? `Kiro tool_call repair buffer exceeded ${maxBufferBytes} bytes`
          : retryAttempt.invalidToolCall || "Kiro tool_call repair retry failed",
        retryAttempt.kind === "buffer_exceeded"
          ? "kiro_tool_call_repair_buffer_exceeded"
          : "kiro_tool_call_repair_retry_failed"
      ), {
        status: retryResult.response.status,
        statusText: retryResult.response.statusText,
        headers: { ...SSE_HEADERS }
      });
      return retryResult;
    } catch (error) {
      if (error.name === "AbortError") throw error;
      firstResult.response = new Response(formatKiroToolCallRepairError(
        error.message || "Kiro tool_call repair failed"
      ), {
        status: firstResult.response.status,
        statusText: firstResult.response.statusText,
        headers: { ...SSE_HEADERS }
      });
      return firstResult;
    } finally {
      if (cleanupInFinally) combined.cleanup();
    }
  }

  async openToolCallRepairGate(rawResponse, args, options) {
    let invalidToolCall = null;
    let terminalDiagnostics = null;
    const transformOptions = {
      onInvalidToolCall: (message) => {
        invalidToolCall = message;
      },
      onTerminalState: (diagnostics) => {
        terminalDiagnostics = diagnostics;
      },
      suppressInvalidToolCallError: options.suppressInvalidToolCallError,
      invalidToolCallErrorCode: options.invalidToolCallErrorCode
    };
    const transformed = this.transformEventStreamToSSE(rawResponse, args.model, transformOptions);
    const reader = transformed.body.getReader();
    const bufferedChunks = [];
    let totalBytes = 0;
    let sawAnyChunk = false;
    const outputState = {
      content: "",
      reasoningContent: "",
      hasToolCalls: false
    };

    try {
      while (true) {
        const timeoutMs = sawAnyChunk ? options.stallTimeoutMs : options.ttftTimeoutMs;
        const timeoutKind = sawAnyChunk ? "stalled" : "timed out before first chunk";
        const { done, value } = await readWithTimeout(
          reader,
          options.signal,
          timeoutMs,
          `Kiro tool_call repair ${timeoutKind}`
        );

        if (done) {
          if (invalidToolCall) {
            return { kind: "invalid", invalidToolCall };
          }
          const loggedDiagnostics = logKiroTerminalDiagnostics(
            args.model,
            options.attempt,
            terminalDiagnostics
          );
          if (isKiroTerminalFailure(loggedDiagnostics.terminal_provenance)) {
            return { kind: "missing_terminal", terminalDiagnostics: loggedDiagnostics };
          }
          const gatedOutputKind = classifyKiroGatedOutput(outputState);
          if (gatedOutputKind) return { kind: gatedOutputKind };
          return { kind: "complete", bytes: concatChunks(bufferedChunks, totalBytes) };
        }

        sawAnyChunk = true;
        if (invalidToolCall) {
          await reader.cancel("invalid_kiro_tool_call").catch(() => {});
          return { kind: "invalid", invalidToolCall };
        }

        totalBytes += value.byteLength;
        if (totalBytes > options.maxBufferBytes) {
          await reader.cancel("kiro_tool_call_repair_buffer_exceeded").catch(() => {});
          return { kind: "buffer_exceeded" };
        }

        bufferedChunks.push(value);
        const inspection = inspectRepairSSEChunk(value, outputState);

        if (terminalDiagnostics) {
          const loggedDiagnostics = logKiroTerminalDiagnostics(
            args.model,
            options.attempt,
            terminalDiagnostics
          );
          if (isKiroTerminalFailure(loggedDiagnostics.terminal_provenance)) {
            await reader.cancel("kiro_missing_terminal").catch(() => {});
            return { kind: "missing_terminal", terminalDiagnostics: loggedDiagnostics };
          }

          const gatedOutputKind = classifyKiroGatedOutput(outputState);
          if (gatedOutputKind) {
            await reader.cancel(`kiro_${gatedOutputKind}_retry`).catch(() => {});
            return { kind: gatedOutputKind };
          }

          // The repair gate no longer owns validation failures after bytes are
          // released to the client. Surface any later malformed tool call as a
          // terminal SSE error instead of silently closing a partial 200 stream.
          transformOptions.suppressInvalidToolCallError = false;
          return {
            kind: "stream",
            firstChunk: concatChunks(bufferedChunks, totalBytes),
            reader
          };
        }

        // Match Kiro CLI streaming behavior: once output cannot be an
        // ellipsis-only false final, release it without waiting for EOF.
        if (inspection.safeToStream) {
          transformOptions.suppressInvalidToolCallError = false;
          return {
            kind: "stream",
            firstChunk: concatChunks(bufferedChunks, totalBytes),
            reader
          };
        }
      }
    } catch (error) {
      await reader.cancel(error.message || "kiro_tool_call_repair_failed").catch(() => {});
      throw error;
    }
  }

  /**
   * Transform AWS EventStream binary response to SSE text stream.
   * This pumps the upstream reader directly so a malformed wrapper can emit a
   * clean SSE error and then cancel the upstream HTTP body immediately.
   */
  transformEventStreamToSSE(response, model, options = {}) {
    let buffer = new Uint8Array(0);
    let chunkIndex = 0;
    const responseId = `chatcmpl-${Date.now()}`;
    const created = Math.floor(Date.now() / 1000);
    const capabilityModel = resolveKiroModel(model).upstream;
    const contextWindow = getCapabilitiesForModel("kiro", capabilityModel).contextWindow || 200000;
    const state = {
      endDetected: false,
      finishEmitted: false,
      hasToolCalls: false,
      hasReasoningContent: false,
      reasoningChunkCount: 0,
      toolCallIndex: 0,
      generatedToolIdCounter: 0,
      seenToolIds: new Map(),
      pendingWrapperToolCalls: new Map(),
      inThinking: false,
      hasTextContent: false,
      hasCodeContent: false,
      terminalFailed: false,
      terminalProvenance: null,
      eventCounts: createKiroEventCounts()
    };

    const reportTerminalState = (provenance, incompleteFrameBytes = buffer.byteLength) => {
      if (state.terminalProvenance === provenance) return;
      if (state.terminalProvenance && !isKiroTerminalFailure(provenance)) return;
      state.terminalProvenance = provenance;
      options.onTerminalState?.({
        terminal_provenance: provenance,
        event_counts: { ...state.eventCounts },
        incomplete_frame_bytes: incompleteFrameBytes
      });
    };

    const failTransport = (controller, provenance, incompleteFrameBytes) => {
      const diagnostics = {
        terminal_provenance: provenance,
        event_counts: { ...state.eventCounts },
        incomplete_frame_bytes: incompleteFrameBytes
      };
      state.terminalFailed = true;
      state.doneSent = true;
      reportTerminalState(provenance, incompleteFrameBytes);
      controller.enqueue(formatKiroMissingTerminalSSE(diagnostics));
    };

    const emitFinishChunk = (controller, finishReason) => {
      if (state.finishEmitted) return;
      state.finishEmitted = true;
      const finishChunk = {
        id: responseId,
        object: "chat.completion.chunk",
        created,
        model,
        choices: [{
          index: 0,
          delta: {},
          finish_reason: finishReason
        }]
      };
      if (state.usage) finishChunk.usage = state.usage;
      controller.enqueue(encodeSSE(`data: ${JSON.stringify(finishChunk)}\n\n`));
    };

    const getToolCallId = (toolUse) => {
      if (typeof toolUse?.toolUseId === "string" && toolUse.toolUseId) {
        return toolUse.toolUseId;
      }
      state.generatedToolIdCounter++;
      return `call_${created}_${state.generatedToolIdCounter}`;
    };

    const getOrAssignToolIndex = (toolCallId) => {
      if (state.seenToolIds.has(toolCallId)) {
        return { toolIndex: state.seenToolIds.get(toolCallId), isNewTool: false };
      }
      const toolIndex = state.toolCallIndex++;
      state.seenToolIds.set(toolCallId, toolIndex);
      return { toolIndex, isNewTool: true };
    };

    const emitToolCallStart = (controller, toolCallId, toolName, toolIndex) => {
      const startChunk = {
        id: responseId,
        object: "chat.completion.chunk",
        created,
        model,
        choices: [{
          index: 0,
          delta: {
            ...(chunkIndex === 0 ? { role: "assistant" } : {}),
            tool_calls: [{
              index: toolIndex,
              id: toolCallId,
              type: "function",
              function: {
                name: toolName,
                arguments: ""
              }
            }]
          },
          finish_reason: null
        }]
      };
      chunkIndex++;
      controller.enqueue(encodeSSE(`data: ${JSON.stringify(startChunk)}\n\n`));
    };

    const emitToolCallArguments = (controller, toolIndex, argumentsStr) => {
      const argsChunk = {
        id: responseId,
        object: "chat.completion.chunk",
        created,
        model,
        choices: [{
          index: 0,
          delta: {
            tool_calls: [{
              index: toolIndex,
              function: {
                arguments: argumentsStr
              }
            }]
          },
          finish_reason: null
        }]
      };
      chunkIndex++;
      controller.enqueue(encodeSSE(`data: ${JSON.stringify(argsChunk)}\n\n`));
    };

    const failInvalidToolCall = (controller, message) => {
      emitKiroToolCallValidationError(controller, state, message, options);
      buffer = new Uint8Array(0);
    };

    const flushPendingWrapperToolCalls = (controller) => {
      if (state.pendingWrapperToolCalls.size === 0) return true;

      for (const toolCall of state.pendingWrapperToolCalls.values()) {
        const toolInput = getBufferedKiroToolInput(toolCall);
        try {
          validateKiroToolCallWrapperInput(toolInput);
        } catch (error) {
          failInvalidToolCall(controller, error.message);
          return false;
        }

        const { toolIndex } = getOrAssignToolIndex(toolCall.toolCallId);
        const argumentsStr = typeof toolInput === "string" ? toolInput : JSON.stringify(toolInput);
        toolCall.toolIndex = toolIndex;
        emitToolCallStart(controller, toolCall.toolCallId, toolCall.toolName, toolIndex);
        if (argumentsStr) {
          emitToolCallArguments(controller, toolIndex, argumentsStr);
        }
      }

      state.pendingWrapperToolCalls.clear();
      return true;
    };

    const transformChunk = async (chunk, controller) => {
        if (state.invalidToolCall) return;
        // Track output so we can emit a keepalive if this frame yields no chunk.
        const enqueueCountBefore = chunkIndex;
        // Append to buffer
        const newBuffer = new Uint8Array(buffer.length + chunk.length);
        newBuffer.set(buffer);
        newBuffer.set(chunk, buffer.length);
        buffer = newBuffer;

        // Parse events from buffer
        let iterations = 0;
        const maxIterations = 1000;
        while (buffer.length >= 12 && iterations < maxIterations) {
          iterations++;
          const view = new DataView(buffer.buffer, buffer.byteOffset);
          const totalLength = view.getUint32(0, false);
          const headersLength = view.getUint32(4, false);

          if (totalLength < 16 || headersLength > totalLength - 16) {
            failTransport(controller, KIRO_TERMINAL_PROVENANCE.CORRUPT_FRAME, buffer.byteLength);
            return;
          }
          if (buffer.length < totalLength) break;

          const eventData = buffer.slice(0, totalLength);
          let event;
          try {
            event = parseEventFrame(eventData);
          } catch {
            failTransport(controller, KIRO_TERMINAL_PROVENANCE.CORRUPT_FRAME, eventData.byteLength);
            return;
          }
          buffer = buffer.slice(totalLength);

          const eventType = event.headers[":event-type"] || "";
          incrementKiroEventCount(state.eventCounts, eventType);
          const messageType = event.headers[":message-type"] || "";
          if (messageType === "exception" || messageType === "error") {
            failTransport(
              controller,
              KIRO_TERMINAL_PROVENANCE.UPSTREAM_ERROR,
              eventData.byteLength
            );
            return;
          }

          // Track total content length for token estimation
          if (!state.totalContentLength) state.totalContentLength = 0;
          if (!state.contextUsagePercentage) state.contextUsagePercentage = 0;

          // Handle assistantResponseEvent
          if (eventType === "assistantResponseEvent" && event.payload?.content) {
            let content = event.payload.content;

            // Kiro Claude models can leak <thinking> blocks into the content stream.
            // We strip these literal tags to prevent duplication, as the reasoning 
            // is already routed correctly via reasoningContentEvent.
            if (state.inThinking) {
              if (content.includes("</thinking>")) {
                state.inThinking = false;
                const after = content.split("</thinking>").slice(1).join("</thinking>");
                content = after.startsWith("\n") ? after.substring(1) : after;
              } else {
                content = ""; // Drop entirely while inside thinking block
              }
            } else if (content.includes("<thinking>")) {
              state.inThinking = true;
              if (content.includes("</thinking>")) {
                state.inThinking = false;
                const before = content.split("<thinking>")[0];
                const after = content.split("</thinking>").slice(1).join("</thinking>");
                content = before + (after.startsWith("\n") ? after.substring(1) : after);
              } else {
                content = content.split("<thinking>")[0];
              }
            }

            if (!content && state.hasReasoningContent) {
              // If we stripped everything, skip emitting an empty content chunk
              continue;
            }

            state.totalContentLength += content.length;
            if (content) state.hasTextContent = true;

            const chunk = {
              id: responseId,
              object: "chat.completion.chunk",
              created,
              model,
              choices: [{
                index: 0,
                delta: chunkIndex === 0
                  ? { role: "assistant", content }
                  : { content },
                finish_reason: null
              }]
            };
            chunkIndex++;
            controller.enqueue(encodeSSE(`data: ${JSON.stringify(chunk)}\n\n`));
          }

          // Handle reasoningContentEvent (Kiro thinking / reasoning)
          // Kiro returns reasoning as a separate event when the request system
          // prompt contains <thinking_mode>enabled</thinking_mode>. Surface it
          // as OpenAI delta.reasoning_content so downstream translators can map
          // it back to Claude thinking blocks / Anthropic reasoning, etc.
          if (eventType === "reasoningContentEvent") {
            const reasoning = event.payload?.reasoningContentEvent || event.payload || {};
            const reasoningText = (typeof reasoning === "string")
              ? reasoning
              : (reasoning.text || reasoning.content || "");
            if (reasoningText) {
              state.hasReasoningContent = true;
              state.totalContentLength += reasoningText.length;

              const reasoningDelta = state.reasoningChunkCount === 0 && chunkIndex === 0
                ? { role: "assistant", reasoning_content: reasoningText }
                : { reasoning_content: reasoningText };

              const chunk = {
                id: responseId,
                object: "chat.completion.chunk",
                created,
                model,
                choices: [{
                  index: 0,
                  delta: reasoningDelta,
                  finish_reason: null
                }]
              };
              chunkIndex++;
              state.reasoningChunkCount++;
              controller.enqueue(encodeSSE(`data: ${JSON.stringify(chunk)}\n\n`));
            }
          }

          // Handle codeEvent
          if (eventType === "codeEvent" && event.payload?.content) {
            state.hasCodeContent = true;
            const chunk = {
              id: responseId,
              object: "chat.completion.chunk",
              created,
              model,
              choices: [{
                index: 0,
                delta: { content: event.payload.content },
                finish_reason: null
              }]
            };
            chunkIndex++;
            controller.enqueue(encodeSSE(`data: ${JSON.stringify(chunk)}\n\n`));
          }

          // Handle toolUseEvent
          if (eventType === "toolUseEvent" && event.payload) {
            state.hasToolCalls = true;
            const toolUse = event.payload;
            const toolUses = Array.isArray(toolUse) ? toolUse : [toolUse];

            for (const singleToolUse of toolUses) {
              let toolName;
              try {
                toolName = validateKiroToolName(singleToolUse);
              } catch (error) {
                failInvalidToolCall(controller, error.message);
                return;
              }

              const toolCallId = getToolCallId(singleToolUse);
              const toolInput = singleToolUse.input;

              if (toolName === KIRO_TOOL_CALL_WRAPPER) {
                let toolCall = state.pendingWrapperToolCalls.get(toolCallId);
                if (!toolCall) {
                  if (state.seenToolIds.has(toolCallId)) {
                    failInvalidToolCall(controller, "Invalid Kiro tool_call payload: duplicate toolUseId reused by wrapper");
                    return;
                  }
                  toolCall = { toolCallId, toolName };
                  state.pendingWrapperToolCalls.set(toolCallId, toolCall);
                }
                try {
                  appendBufferedKiroToolInput(toolCall, toolInput);
                } catch (error) {
                  failInvalidToolCall(controller, error.message);
                  return;
                }
                continue;
              }

              if (state.pendingWrapperToolCalls.has(toolCallId)) {
                failInvalidToolCall(controller, "Invalid Kiro tool_call payload: mixed wrapper and direct tool fragments");
                return;
              }

              const { toolIndex, isNewTool } = getOrAssignToolIndex(toolCallId);
              if (isNewTool) {
                emitToolCallStart(controller, toolCallId, toolName, toolIndex);
              }

              if (toolInput !== undefined) {
                let argumentsStr;

                if (typeof toolInput === 'string') {
                  argumentsStr = toolInput;
                } else if (typeof toolInput === 'object') {
                  argumentsStr = JSON.stringify(toolInput);
                } else {
                  continue;
                }

                emitToolCallArguments(controller, toolIndex, argumentsStr);
              }
            }
          }

          // Handle messageStopEvent
          if (eventType === "messageStopEvent") {
            if (!flushPendingWrapperToolCalls(controller)) return;
            if (!hasKiroModelOutput(state)) {
              const diagnostics = {
                terminal_provenance: KIRO_TERMINAL_PROVENANCE.EMPTY_RESPONSE,
                event_counts: { ...state.eventCounts },
                incomplete_frame_bytes: 0
              };
              reportTerminalState(KIRO_TERMINAL_PROVENANCE.EMPTY_RESPONSE);
              state.terminalFailed = true;
              state.doneSent = true;
              controller.enqueue(formatKiroMissingTerminalSSE(diagnostics));
              return;
            }
            reportTerminalState(KIRO_TERMINAL_PROVENANCE.MESSAGE_STOP);
          }

          // Handle contextUsageEvent to extract contextUsagePercentage
          if (eventType === "contextUsageEvent" && event.payload?.contextUsagePercentage) {
            state.contextUsagePercentage = event.payload.contextUsagePercentage;
            // Mark that we received context usage event
            state.hasContextUsage = true;
          }

          // Handle meteringEvent - mark that we received it
          if (eventType === "meteringEvent") {
            state.hasMeteringEvent = true;
            const metering = event.payload?.meteringEvent || event.payload || {};
            const credits = Number(metering.usage);
            if (Number.isFinite(credits)) {
              state.usage = {
                ...(state.usage || {}),
                kiro_credits: credits,
                kiro_credit_unit: typeof metering.unit === "string" ? metering.unit : "credit"
              };

              if (state.finishEmitted) {
                const usageChunk = {
                  id: responseId,
                  object: "chat.completion.chunk",
                  created,
                  model,
                  choices: [],
                  usage: state.usage
                };
                controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(usageChunk)}\n\n`));
              }
            }
          }

          // Handle metricsEvent for token usage
          if (eventType === "metricsEvent") {
            // Extract usage data from metricsEvent payload
            const metrics = event.payload?.metricsEvent || event.payload;
            if (metrics && typeof metrics === 'object') {
              const inputTokens = metrics.inputTokens || 0;
              const outputTokens = metrics.outputTokens || 0;
              // ponytail: Amazon Q upstream does not expose cache fields today,
              // but pick up cache_read_input_tokens / cache_creation_input_tokens
              // if the event shape grows them so cost tracking stays accurate.
              const cachedTokens = metrics.cacheReadInputTokens || metrics.cache_read_input_tokens || 0;
              const cacheCreationInputTokens = metrics.cacheCreationInputTokens || metrics.cache_creation_input_tokens || 0;

              if (inputTokens > 0 || outputTokens > 0) {
                state.usage = {
                  ...(state.usage || {}),
                  prompt_tokens: inputTokens,
                  completion_tokens: outputTokens,
                  total_tokens: inputTokens + outputTokens
                };
                // Kiro is Claude-backed: inputTokens EXCLUDES cache (Claude convention),
                // not inclusive like OpenAI's cached_tokens. Emit cache_read_input_tokens
                // (not cached_tokens) so canonicalizeUsage takes the Claude fold path and
                // correctly adds cache back into prompt_tokens instead of undercharging.
                if (cachedTokens > 0) state.usage.cache_read_input_tokens = cachedTokens;
                if (cacheCreationInputTokens > 0) state.usage.cache_creation_input_tokens = cacheCreationInputTokens;
              }
            }
          }

          // contextUsageEvent, meteringEvent, and metricsEvent are usage-only.
          // They may enrich a proven terminal chunk but can never prove that the
          // model completed its message.
          if (state.hasMeteringEvent && state.hasContextUsage && !state.usage?.total_tokens) {
            const estimatedOutputTokens = state.totalContentLength > 0
              ? Math.max(1, Math.floor(state.totalContentLength / 4))
              : 0;
            const estimatedInputTokens = state.contextUsagePercentage > 0
              ? Math.floor(state.contextUsagePercentage * contextWindow / 100)
              : 0;
            state.usage = {
              ...(state.usage || {}),
              prompt_tokens: estimatedInputTokens,
              completion_tokens: estimatedOutputTokens,
              total_tokens: estimatedInputTokens + estimatedOutputTokens
            };
          }
        }

        if (iterations >= maxIterations) {
          console.warn("[Kiro] Max iterations reached in event parsing");
        }

        // No client chunk produced this frame — emit an SSE comment keepalive
                // so the stall watchdog sees upstream activity (ignored by parser/client).
                if (chunkIndex === enqueueCountBefore && !state.finishEmitted) {
                  controller.enqueue(encodeSSE(": ka\n\n"));
                }
      };

      const flushOutput = (controller) => {
        if (state.invalidToolCall) return false;
        if (state.doneSent) return true;
        if (buffer.byteLength > 0) {
          failTransport(
            controller,
            KIRO_TERMINAL_PROVENANCE.INCOMPLETE_FRAME,
            buffer.byteLength
          );
          return true;
        }

        if (!state.finishEmitted) {
          if (!hasKiroModelOutput(state)) {
            const diagnostics = {
              terminal_provenance: KIRO_TERMINAL_PROVENANCE.EMPTY_RESPONSE,
              event_counts: { ...state.eventCounts },
              incomplete_frame_bytes: 0
            };
            reportTerminalState(KIRO_TERMINAL_PROVENANCE.EMPTY_RESPONSE);
            state.doneSent = true;
            controller.enqueue(formatKiroMissingTerminalSSE(diagnostics));
            return true;
          }

          if (!flushPendingWrapperToolCalls(controller)) return false;
          reportTerminalState(KIRO_TERMINAL_PROVENANCE.CLEAN_EOF);
          emitFinishChunk(controller, state.hasToolCalls ? "tool_calls" : "stop");
        }

        // Send final done message
        if (!state.doneSent) {
          state.doneSent = true;
          controller.enqueue(encodeSSE(SSE_DONE));
        }
        return true;
    };

    if (!response.body) {
      const diagnostics = {
        terminal_provenance: KIRO_TERMINAL_PROVENANCE.MISSING_BODY,
        event_counts: createKiroEventCounts(),
        incomplete_frame_bytes: 0
      };
      options.onTerminalState?.(diagnostics);
      return new Response(formatKiroMissingTerminalSSE(diagnostics), {
        status: response.status,
        statusText: response.statusText,
        headers: { ...SSE_HEADERS }
      });
    }
    let reader;
    const transformedStream = new ReadableStream({
      async start(controller) {
        reader = response.body.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (done) break;

            await transformChunk(value, controller);
            if (state.invalidToolCall) {
              await reader.cancel("invalid_kiro_tool_call").catch(() => {});
              return;
            }
            if (state.terminalFailed) {
              await reader.cancel("kiro_terminal_failure").catch(() => {});
              closeSSEController(controller);
              return;
            }
          }

          if (flushOutput(controller)) {
            closeSSEController(controller);
          }
        } catch (error) {
          if (state.invalidToolCall) return;
          controller.error(error);
        }
      },

      cancel(reason) {
        return reader?.cancel(reason);
      }
    });

    return new Response(transformedStream, {
      status: response.status,
      statusText: response.statusText,
      headers: { ...SSE_HEADERS }
    });
  }

  async refreshCredentials(credentials, log, proxyOptions = null) {
    if (!credentials.refreshToken) return null;

    try {
      // Use centralized refreshKiroToken function (handles both AWS SSO OIDC and Social Auth)
      const result = await refreshKiroToken(
        credentials.refreshToken,
        credentials.providerSpecificData,
        log,
        proxyOptions
      );

      return result;
    } catch (error) {
      log?.error?.("TOKEN", `Kiro refresh error: ${error.message}`);
      return null;
    }
  }
}

/**
 * Parse AWS EventStream frame
 */
function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc = CRC32_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function parseEventFrame(data) {
  if (!(data instanceof Uint8Array) || data.byteLength < 16) {
    throw new Error("AWS EventStream message is shorter than its 16-byte overhead");
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const totalLength = view.getUint32(0, false);
  const headersLength = view.getUint32(4, false);
  if (totalLength !== data.byteLength) {
    throw new Error("AWS EventStream reported length does not match frame length");
  }
  if (headersLength > totalLength - 16) {
    throw new Error("AWS EventStream headers exceed the frame body bounds");
  }
  if (view.getUint32(8, false) !== crc32(data.subarray(0, 8))) {
    throw new Error("AWS EventStream prelude CRC mismatch");
  }
  if (view.getUint32(totalLength - 4, false) !== crc32(data.subarray(0, totalLength - 4))) {
    throw new Error("AWS EventStream message CRC mismatch");
  }

  const headers = {};
  let offset = 12;
  const headerEnd = offset + headersLength;
  const requireHeaderBytes = (count) => {
    if (count < 0 || offset + count > headerEnd) {
      throw new Error("AWS EventStream header exceeds declared header bounds");
    }
  };

  while (offset < headerEnd) {
    requireHeaderBytes(1);
    const nameLength = data[offset++];
    requireHeaderBytes(nameLength + 1);
    const name = sharedDecoder.decode(data.subarray(offset, offset + nameLength));
    offset += nameLength;
    const headerType = data[offset++];

    if (headerType === 0 || headerType === 1) {
      headers[name] = headerType === 0;
    } else if (headerType === 2) {
      requireHeaderBytes(1);
      headers[name] = view.getInt8(offset);
      offset += 1;
    } else if (headerType === 3) {
      requireHeaderBytes(2);
      headers[name] = view.getInt16(offset, false);
      offset += 2;
    } else if (headerType === 4) {
      requireHeaderBytes(4);
      headers[name] = view.getInt32(offset, false);
      offset += 4;
    } else if (headerType === 5 || headerType === 8) {
      requireHeaderBytes(8);
      offset += 8;
    } else if (headerType === 6 || headerType === 7) {
      requireHeaderBytes(2);
      const valueLength = view.getUint16(offset, false);
      offset += 2;
      requireHeaderBytes(valueLength);
      const valueBytes = data.subarray(offset, offset + valueLength);
      headers[name] = headerType === 7 ? sharedDecoder.decode(valueBytes) : valueBytes;
      offset += valueLength;
    } else if (headerType === 9) {
      requireHeaderBytes(16);
      offset += 16;
    } else {
      throw new Error("AWS EventStream header has an unknown type");
    }
  }

  const payloadStart = headerEnd;
  const payloadEnd = totalLength - 4;
  let payload = null;
  if (payloadEnd > payloadStart) {
    const payloadStr = sharedDecoder.decode(data.subarray(payloadStart, payloadEnd));
    if (!payloadStr.trim()) return { headers, payload: null };
    try {
      payload = JSON.parse(payloadStr);
    } catch (parseError) {
      console.warn(`[Kiro] Failed to parse payload: ${parseError.message} | payload: ${payloadStr.substring(0, 100)}`);
      payload = { raw: payloadStr };
    }
  }

  return { headers, payload };
}

export default KiroExecutor;
