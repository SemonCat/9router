# Local patch stack

`local-patches` is the maintainable source branch for the personal AWS patch
stack. It is based on `upstream/master` at
`0513bf393fb0eedd2baecdadb54b6129976a706c` (2026-07-20).

This branch is **not auto-deployed**. Committing or pushing it does not update
AWS; deployment remains a separate, explicit operation.

## Upstream pull requests

Status and head SHAs were verified read-only on 2026-07-20.

| PR | Purpose | Head | Status |
| --- | --- | --- | --- |
| [#2618](https://github.com/decolua/9router/pull/2618) | Preserve Kiro credit metering without treating credits as USD | `4db68adc54313db804fd9372de11dd4538081ad0` | Open |
| [#2639](https://github.com/decolua/9router/pull/2639) | Harden round-robin session affinity | `7ffc069f4594c407c9330362f38435f6bf36899e` | Open |
| [#2707](https://github.com/decolua/9router/pull/2707) | Map GPT-5.6 reasoning effort fields and preserve fallback behavior | `52dfbf28a3282723914c3659e7ac952279e0e4a2` | Open |
| [#2688](https://github.com/decolua/9router/pull/2688) | Validate and repair malformed nested Kiro tool calls once | `4bbba263ad29b2df793cf68cc46647a250d2287d` | Open |
| [#2713](https://github.com/decolua/9router/pull/2713) | Reconstruct reliable OpenAI Responses terminal output | `8817537ba0a7b9b0b8dc877f832827d92933de2a` | Open |
| [#2664](https://github.com/decolua/9router/pull/2664) | Aggregate account locks and cool down confirmed Kiro credit exhaustion | `3f94a5c459ed7973c8716325d60450bb575d69fe` | Open |

PR #2580 is closed and is deliberately excluded: its cache-reuse core already
exists upstream. Do not re-add that patch wholesale.

The commits on this branch were replayed from the verified live integration
snapshot onto the newer base, so their SHAs differ from the PR heads above.
The table records provenance and the exact upstream patch revisions represented.

## Local-only patches and order

Apply or resolve the groups in this order:

1. #2618 credit metering, then #2639 session affinity.
2. #2707 reasoning effort, then #2688 nested tool validation/one-shot repair.
3. #2713 Responses accumulation and terminal reconstruction (`96e317b`,
   `7888c28`).
4. Kiro terminal/EventStream integrity (`e3d53a5`, `5e91b6d`, `afd773d`):
   terminal provenance, CRC/frame validation, bounded retry, and error frames.
5. #2664 aggregation/cooldown integration (`99c816d`, `e2f794c`, `d424788`,
   `495e569`).
6. Latest #2707 unsupported-effort fallback hardening (`19d3620`).
7. Consolidation review hardening: after the private repair gate releases a
   valid text prefix, any later malformed tool call is surfaced as a terminal
   SSE error instead of silently truncating the client stream. Buffered
   malformed output still follows the existing one-shot repair path.
8. Transport-only cleanup on the deploy branch removes ellipsis/future-action
   phrase classification inherited from `a5caa7c`/`22962ff`. Valid response
   prose passes through unchanged; Hermes owns semantic completion recovery.

The tool-repair and terminal changes touch the Kiro streaming state machine;
resolve them as protocol behavior rather than accepting one side of a conflict
wholesale. Do not restore response-text classification from the historical
ellipsis/guard commits. The #2639 and #2664 changes both touch account selection;
sticky bindings must survive transient locks while permanent/aggregate lock
metadata remains paired with its owning account.

## Rebase or update

1. Fetch read-only refs: `git fetch upstream` and, when needed, the personal
   fork/PR heads. Never update this stack by blindly re-adding #2580.
2. Create a backup ref, then rebase: `git rebase upstream/master`.
3. Resolve conflicts in the dependency order above. If a PR was merged
   upstream, prove equivalent behavior and tests before dropping its local
   commits.
4. For an updated PR head, compare the old and new heads first (for example
   with `git range-diff`), then port only the new protocol delta. Do not
   reintroduce router-level semantic completion policy or overwrite later
   terminal/aggregation hardening.
5. Repeat source-parity review and all validation below. Record any intentional
   production deviation here before deployment is considered.

## Live-source parity

Read-only evidence was taken from image
`9router:internal-v0.5.35-pr2618-pr2639-pr2707-pr2688-ellipsis-p0sticky-kiroterminal-18b8ddf-guardv2-pr2664-c3debbf-20260720-0200`.
The extracted production files matched the recorded `c3debbf` integration tree
byte-for-byte. After replaying onto `0513bf3`, the expected live-source
differences are these upstream/master changes:

- `open-sse/config/appConstants.js`
- `open-sse/executors/default.js`
- `open-sse/providers/capabilities.js`
- `open-sse/providers/pricing.js`
- `open-sse/providers/registry/index.js`
- `open-sse/providers/registry/kimi-coding.js` (removed upstream)
- `open-sse/providers/registry/kimi.js`
- `open-sse/services/tokenRefresh.js`
- `open-sse/services/tokenRefresh/providers.js`

There is one intentional consolidation-only runtime deviation:
`open-sse/executors/kiro.js` disables validation-error suppression after the
repair gate hands an already-started stream to the client. This fixes a P1
terminal-integrity hole found during closeout review without changing the
buffered one-shot repair behavior. `LOCAL_PATCHES.md` and restored/expanded
regression tests are otherwise repository-only additions.

The deploy branch adds a second intentional deviation from the historical live
image: Kiro ellipsis and future-action text are no longer retried or classified
inside 9Router. Only EventStream framing, terminal provenance, upstream error
frames, empty/incomplete output, and malformed tool-call protocol remain router
concerns. Hermes installs `incomplete-final-recovery` to handle semantic
COMPLETE/BLOCKED/CONTINUE decisions with the original request and tool history.

## Validation

Run from the repository root:

```sh
npx vitest run --config tests/vitest.config.js \
  tests/unit/cached-token-usage.test.js \
  tests/unit/sse-to-json-usage.test.js \
  tests/unit/provider-session-sticky.test.js \
  tests/unit/openai-to-kiro.test.js \
  tests/translator/claude-kiro-direct.test.js \
  tests/unit/kiro-tool-call-validation.test.js \
  tests/unit/kiro-one-shot-tool-call-repair.test.js \
  tests/unit/kiro-thinking-strip.test.js \
  tests/unit/responses-accumulator.test.js \
  tests/unit/forced-responses-sse-to-json.test.js \
  tests/unit/openai-responses-terminal-event.test.js \
  tests/unit/responses-abort-terminal.test.js \
  tests/unit/streaming-handler-responses-passthrough.test.js \
  tests/unit/account-lock-aggregation.test.js \
  tests/unit/chat-all-accounts-locked.test.js \
  tests/unit/kiro-credit-cooldown.test.js \
  tests/unit/kiro-credit-exhaustion.test.js
npx eslint <touched-js-files>
git diff --check upstream/master...HEAD
npm run build
```

Run the complete no-credential test suite when practical. Real-provider tests
are intentionally separate because they require local credentials and make
network calls.
