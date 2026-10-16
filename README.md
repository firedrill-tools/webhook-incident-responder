# Webhook incident responder

A synthetic service incident worker using [LangGraph](https://docs.langchain.com/oss/python/langgraph/quickstart) and [ChatAnthropic](https://docs.langchain.com/oss/python/integrations/chat/anthropic). A signed HTTP endpoint journals service events. The worker inspects state, asks Claude for an evidence-bound assessment of degradations, then atomically updates incident state and queues a Slack-format notification. The model cannot send messages or change the event sequence.

## Run locally

Requires Python 3.12+ and `uv`:

```sh
uv sync --extra test
uv run pytest -q
uv run python -m incident_responder --db incidents.db submit fixtures/degraded-1.json
uv run python -m incident_responder --db incidents.db get api-001
```

Set `ANTHROPIC_API_KEY` and `ANTHROPIC_MODEL` to a Claude model available to your account, then process a pending event:

```sh
uv run python -m incident_responder --db incidents.db work --once --model YOUR_ENABLED_MODEL
uv run python -m incident_responder --db incidents.db state checkout-api
uv run python -m incident_responder --db incidents.db outbox
```

`work` without `--once` drains the currently pending queue and exits. `retry EVENT_ID` requeues a failed event with fewer than three attempts.

To receive a signed webhook, set `INCIDENT_WEBHOOK_SECRET` and run `uv run python -m incident_responder --db incidents.db serve`. Send JSON to `POST /events` with `X-Incident-Signature: sha256=<HMAC-SHA256 of the raw body>`. The endpoint rejects missing/invalid signatures, oversized events, conflicting IDs and reused service sequences. Intake does not call the model; run a worker separately.

To send queued messages to a configured Slack incoming webhook, set `SLACK_WEBHOOK_URL` and explicitly run `uv run python -m incident_responder --db incidents.db send --confirm`. The default outbox command only previews. Slack incoming webhooks do not supply an idempotency key; a crash after Slack accepts a message but before the local sent marker can lead to a duplicate delivery. This example does not execute that external send in tests.

## State and evidence

An event ID replay with the same payload returns its current journal row. Reusing the ID with changed content or assigning one service sequence to a different ID is rejected. A late event whose sequence is at or below the committed service sequence is ignored. A worker lease token prevents an expired worker from committing after replacement. Three attempts are allowed. Incident and outbox changes share one SQLite transaction.

43 local checks exercise actual LangGraph/ChatAnthropic/Anthropic HTTP, normal CLI success/failure/SIGINT/SIGTERM/SIGHUP, original response/usage/error retention, cancellation/deadline/closed clients, recording faults, stale source, exact supplied citations and lease expiry. The original stub/HMAC/order/retry checks remain. Controlled responses establish runtime boundaries; live Claude quality and Slack delivery remain unverified. A live proof requires provider and Slack test access. The thin Firedrill connection is pending qualification of the new release. The SQLite journal and HMAC secret are synthetic example choices; a production deployment needs its own source authenticity, secret storage and notification delivery policy.

Explore related applications in the [Firedrill example catalog](https://github.com/firedrill-tools/firedrill-example-catalog). Product and setup references: [Firedrill](https://firedrill.run/) and the [official developer documentation](https://docs.firedrill.run/). Use documentation matching the qualified release; this repository records its current integration limits above.


## Native assessment and private attempt records

Normal investigation uses the pinned real ChatAnthropic 1.7.6 / Anthropic 1.14.0 async path, with one request, 512 requested output tokens, zero SDK retries, 30-second HTTP phases and a default 60-second operation budget (configurable up to 120). `work --provider-base-url URL` supports explicit HTTPS or loopback HTTP without URL credentials/query/fragment. `--model`, `--timeout-seconds` and `--output-root` configure the run. Requested model and original reported response model stay distinct; no model is substituted. Explicit credentials and authentication-header overrides prevent borrowing token/profile credentials. The narrow owned-client accessor override preserves native SDK formatting/tool parsing and avoids library-cached transports; it must be requalified when the pinned integration changes.

Original decoded HTTP response bytes/JSON, IDs, unknown fields, errors and usage are retained before SDK interpretation. A degradation assessment needs exactly one completed IncidentAssessment tool call with stop_reason tool_use. Truncated/inconsistent/refused/unsupported content fails before release or incident mutation. The graph also restricts citations to its supplied recent context. SDK-normalized usage/defaults are separate from individually observed/unknown/invalid provider counters; missing cache values and cost are never invented as zero.

Each attempt writes a private 0700 run directory/0600 atomically flushed trace, prepared before claiming/model dispatch and finalized after owned client cleanup. By default these are adjacent to the database under `.runs/`; returned records include `receipt_path`. Recording faults prevent further dispatch/commit. A recording failure after an actual commit preserves the applied event/outbox and fails the run; replay cannot repeat that applied effect. Persistent storage faults expose failed in-memory evidence with unknown durable status. Cancellation closes native work before final publication, and normal CLI signals return 130/143/129 with a failed receipt. The worker exits nonzero on failed investigation even if the event row retains an already-applied effect.

Expiry itself fences commit/failure, including the exact deadline without a replacement worker. Claim validates finite lease parameters and uses the cooperative store clock inside its transaction. Deadline/cancellation guards run after acquiring database locks and before mutation/transaction release. Event observed_at and the application/worker clock are separate. Owned clients are closed on success/error/deadline/cancellation; injected assessor work remains caller-owned.

The 1 MiB response cap is retained decoded evidence, not a hard wire/peak-memory bound. Synchronous local database/filesystem/flush work and client cleanup are excluded from a hard whole-run wall promise. This is a local synthetic application, not hostile-code containment or live incident-response/delivery proof. [PLAN.md](PLAN.md) retains persisted-source/replay/outbox/intake/data/distribution work; [COMPATIBILITY.md](COMPATIBILITY.md) records the current qualification scope. Real Slack sending stays an explicit parked access/authorization gate.
