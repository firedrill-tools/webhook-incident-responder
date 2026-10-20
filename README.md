# Webhook incident responder with LangGraph and Anthropic

A synthetic service incident worker using [LangGraph](https://docs.langchain.com/oss/python/langgraph/quickstart) and [ChatAnthropic](https://docs.langchain.com/oss/python/integrations/chat/anthropic). A signed HTTP endpoint journals service events. The worker inspects state, asks Claude for an evidence-bound assessment of degradations, then atomically updates incident state and queues a Slack-format notification. The model cannot send messages or change the event sequence.

Simulate this LangGraph AI agent with [Firedrill](https://firedrill.run/), a simulation platform for AI agents. Start with the [Firedrill setup and simulation guide](firedrill/README.md); your coding agent can operate it while this application's runner stays in your repository or CI.

This example repository is publicly viewable. A project license has not been selected.

## Run locally

Requires Python 3.12+ and `uv`:

```sh
uv sync --locked --extra test
uv run --locked --extra test pytest -q
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

To send queued messages to a configured Slack incoming webhook, set `SLACK_WEBHOOK_URL` and explicitly run `uv run python -m incident_responder --db incidents.db send --confirm`. `--once`, `--timeout-seconds` and `--output-root` also apply. `outbox` previews messages; `deliveries` shows attempt and resolution history. Each notification is claimed atomically before POST. Concurrent processes cannot claim it twice; later notifications for its service wait until it is sent. Only HTTP 200 with the exact body `ok` acknowledges the attempt. All other responses, interrupted requests and expired dispatched claims become `delivery_unknown`, which cannot automatically retry. A prepared claim that expired before dispatch intent may be reclaimed. At most three attempts are allowed.

After inspecting the receiving system, an operator can explicitly resolve an unknown attempt with `resolve-delivery ATTEMPT_ID --decision delivered --note "Observed receiving journal entry"` or `--decision not_delivered --note "Confirmed rejection"`. A not-delivered resolution allows another attempt (or leaves `delivery_failed` after three); a delivered resolution is recorded as an operator assertion, never a fabricated HTTP acknowledgement or Slack message ID. The immutable attempt identity fences stale/repeated resolutions. Unknown/failed notifications hold later work for their own service; other services can proceed. Incoming webhooks have no delivery idempotency capability here. This policy avoids an automatic duplicate after uncertainty; the operator must establish whether retry is appropriate. Tests use owned loopback endpoints only. Real Slack delivery remains parked.

## State and evidence

An event ID replay with the same payload returns its current journal row. Reusing the ID with changed content or assigning one service sequence to a different ID is rejected. A late event whose sequence is at or below the committed service sequence is ignored. A worker lease token and its expiry fence commits even without replacement. Three attempts are allowed. Incident and outbox changes share one SQLite transaction.

43 local checks exercise actual LangGraph/ChatAnthropic/Anthropic HTTP, normal CLI success/failure/SIGINT/SIGTERM/SIGHUP, original response/usage/error retention, cancellation/deadline/closed clients, recording faults, stale source, exact supplied citations and lease expiry. The original stub/HMAC/order/retry checks remain. Controlled responses establish runtime boundaries; live Claude quality and Slack delivery remain unverified. A live proof requires provider and Slack test access. The checked-in Firedrill adapter uses the released CLI/SDK; authenticated persisted-run verification remains separate. The SQLite journal and HMAC secret are synthetic example choices; a production deployment needs its own source authenticity, secret storage and notification delivery policy.

Explore related applications in the [Firedrill example catalog](https://github.com/firedrill-tools/firedrill-example-catalog). Product and setup references: [Firedrill](https://firedrill.run/) and the [official developer documentation](https://docs.firedrill.run/). Use documentation matching the qualified release; this repository records its current integration limits above.


## Native assessment and private attempt records

Normal investigation uses the pinned real ChatAnthropic 1.7.6 / Anthropic 1.14.0 async path, with one request, 512 requested output tokens, zero SDK retries, 30-second HTTP phases and a default 60-second operation budget (configurable up to 120). `work --provider-base-url URL` supports explicit HTTPS or loopback HTTP without URL credentials/query/fragment. `--model`, `--timeout-seconds` and `--output-root` configure the run. Requested model and original reported response model stay distinct; no model is substituted. Explicit credentials and authentication-header overrides prevent borrowing token/profile credentials. The narrow owned-client accessor override preserves native SDK formatting/tool parsing and avoids library-cached transports; it must be requalified when the pinned integration changes.

Original decoded HTTP response bytes/JSON, IDs, unknown fields, errors and usage are retained before SDK interpretation. A degradation assessment needs exactly one completed IncidentAssessment tool call with stop_reason tool_use. Truncated/inconsistent/refused/unsupported content fails before release or incident mutation. The graph also restricts citations to its supplied recent context. SDK-normalized usage/defaults are separate from individually observed/unknown/invalid provider counters; missing cache values and cost are never invented as zero.

Each attempt writes a private 0700 run directory/0600 atomically flushed trace, prepared before claiming/model dispatch and finalized after owned client cleanup. By default these are adjacent to the database under `.runs/`; returned records include `receipt_path`. Recording faults prevent further dispatch/commit. A recording failure after an actual commit preserves the applied event/outbox and fails the run; replay cannot repeat that applied effect. Persistent storage faults expose failed in-memory evidence with unknown durable status. Cancellation closes native work before final publication, and normal CLI signals return 130/143/129 with a failed receipt. The worker exits nonzero on failed investigation even if the event row retains an already-applied effect.

Expiry itself fences commit/failure, including the exact deadline without a replacement worker. Claim validates finite lease parameters and uses the cooperative store clock inside its transaction. Deadline/cancellation guards run after acquiring database locks and before mutation/transaction release. Event observed_at and the application/worker clock are separate. Owned clients are closed on success/error/deadline/cancellation; injected assessor work remains caller-owned.

The 1 MiB response cap is retained decoded evidence, not a hard wire/peak-memory bound. Synchronous local database/filesystem/flush work and client cleanup are excluded from a hard whole-run wall promise. This is a local synthetic application, not hostile-code containment or live incident-response/delivery proof. [PLAN.md](PLAN.md) retains persisted-source/replay/outbox/intake/data/distribution work; [COMPATIBILITY.md](COMPATIBILITY.md) records the current qualification scope. Real Slack sending stays an explicit parked access/authorization gate.


## Journal and intake checkpoint

92 local checks now include schema 1 journal ownership, complete bounded event/state/outbox consistency, source provenance, semantic replay and bounded authenticated intake. Unknown schema objects/versions, invalid references/types/lifecycle/lease facts, contradictory service summaries/history, missing/invented notifications, edited immutable event facts and oversized records are refused without source repair. Reads and writes validate one SQLite transaction; independent connections and real processes qualify one claim/atomic notification. Active stores also fence replaced files or changed ownership identities.

New intake retains its original application-serialized event bytes/hash and a canonical business binding. Reordering JSON object keys is a replay when all typed facts agree; changed facts remain a conflict. The known original three-table journal migrates transactionally, preserving original event/state/outbox values and IDs. Its unavailable original serialization is labeled legacy_snapshot; no historical authentication/model/usage or original bytes are invented. A newly captured canonical binding detects later inconsistent edits. This is cooperative local integrity validation, not protection against a local actor rewriting every consistent record/hash.

Signed intake checks signature format before reading, caps accumulated body bytes at 65,536, rejects duplicate/nonfinite JSON and strict contract violations, and compares HMAC over the actual received bytes. It never invokes the model. Original signed wire JSON is distinct from the stored application serialization. The schema supports at most 1,000 events/notifications and 128 services, 8 MiB each of business payload and retained original serializations, and a 64 MiB regular journal file. Source placement and ASGI body caps are not hard upstream wire/peak-memory guarantees. Versioned case/distribution qualification is documented below; the live Slack gate stays parked.


## Delivery journal checkpoint

161 local checks include the original 92 source/native/intake checks and 69 delivery checks. Schema 2 adds source-bound notification provenance and fenced delivery history without rewriting original business messages. Known version-0/1 journals preserve their exact original rows/IDs/digests; preexisting sent rows retain legacy snapshot provenance, with no invented POST acknowledgement. Delivery state, ordered attempts, request payload/target hashes, leases, original acknowledgement and operator assertions are validated with the whole journal. Escaping `&`, `<` and `>` affects only the outgoing JSON, preserving stored messages.

A private prepared trace precedes claim/dispatch/acknowledgement commit. The target appears as origin plus URL hash, excluding its secret path; errors exposed by the CLI are sanitized. Original decoded response status/body/hash remain private, distinct from an operator assertion. Clients disable redirects and environment proxies, use explicit HTTP/operation budgets, and close before acknowledgement release. Recording failures prevent later dispatch/commit; an already committed sent effect survives a failed receipt and the CLI exits nonzero. `receipt_durable_status=unknown` reports a persistent final recording fault. Cooperative lease expiry after a killed sender holds the uncertain POST rather than resending. The 4 KiB cap is retained decoded acknowledgement data, not a hard network/peak-allocation bound. Local synchronous database/recording work and cleanup are outside a hard whole-run wall guarantee.

The acknowledgement and text escaping follow the official [incoming-webhook contract](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/) and [message text formatting](https://docs.slack.dev/messaging/formatting-message-text/). Controlled receiver acknowledgement proves the application boundary; it does not prove real Slack receipt, downstream visibility or paid provider quality. Versioned case/distribution qualification is documented below; [PLAN.md](PLAN.md) retains external gates and the portfolio handoff.


## Versioned synthetic workspaces

`synthetic-incident-cases` 1.0.0 contains twelve source-hash-bound cases in four profiles. All prior history is fictional imported operator source. There are no invented prior model messages, tokens, authentication measurements or real Slack receipts. The task and case identity stay outside model event/context inputs.

| Profile | Cases |
| --- | --- |
| Starter | checkout-degradation, delayed-recovery, semantic-replay |
| Busy | regional-busy: twelve services, sixty historical events, forty-eight imported notifications, twelve pending degradations |
| Ambiguous | sparse-telemetry, hostile-log |
| Boundary | recovery-before-degradation, stale-degradation, maximum-sequence, expired-worker, exhausted-failure, already-current |

List, initialize and inspect without keys or network calls:

```sh
uv run --locked incident-responder datasets
uv run --locked incident-responder init --case regional-busy --directory /tmp/incident-busy
uv run --locked incident-responder workspace /tmp/incident-busy
uv run --locked incident-responder --db /tmp/incident-busy/incidents.db outbox
```

Initialization validates the pinned index and case hashes, creates an exclusive 0700 directory/0600 files, migrates the supported imported journal without changing its original rows, and writes instance-bound `workspace.json`, `TASK.md` and replay `submissions.json`. Existing paths, files and symlinks are preserved. The source clock recorded in identity is a fixture reference; ordinary workers use their configured current store clock, never event observed_at. API callers can inject a cooperative clock for lease experiments. Model investigation and delivery remain separate explicit commands.

Reset creates a fresh copy of the original case while preserving the old workspace and evidence. Use its current database instance from `workspace`:

```sh
uv run --locked incident-responder reset /tmp/incident-busy --to /tmp/incident-busy-fresh --instance CURRENT_INSTANCE_UUID
```

`workspace` validates the identity and full current journal and refuses missing/replaced/corrupt source. Identity labels do not replace current source validation or establish hostile-tamper protection.

## Distribution qualification

183 checks pass, including all twelve cases, coherent import preservation, key-free CLI/init/readback/reset, corrupt manifest refusal, and actual pinned native investigations on busy/ambiguous/maximum-sequence source. Native controlled responses qualify source placement and runtime behavior; semantic model quality still needs a separately authorized live run.

```sh
uv run --locked python scripts/build_datasets.py --check
uv build
uv run --locked python scripts/check_distribution.py
```

The wheel includes all thirteen JSON data files. The sdist includes source, lockfile, data, original fixtures, tests/helpers, scripts, scenarios, docs and CI. The separate deterministic complete source archive adds an exact per-file SHA256/size/mode manifest and is built without Git or installed caches. `dist/webhook-incident-responder-0.2.0-source.tar.gz` is created by the distribution gate. Fresh locked wheel, sdist and complete-source environments outside the checkout exercise console/module CLI, all twelve private cases, reset, missing-key failure receipts, actual SDK over owned HTTP and acknowledged controlled delivery. The complete archived source runs the entire183-check suite and keeps every archived original file unchanged. CI uploads distributions and `qualification.json` for review; the repositories are publicly viewable and no project license has been selected.

Independent application requirements are implemented. Exact source/data/distribution Linux evidence is recorded in [COMPATIBILITY.md](COMPATIBILITY.md) and the catalog. Paid Claude quality/billing, real Slack authorization/access, authenticated Firedrill persistence verification and license remain parked.

## Current Firedrill integration

Released CLI/SDK 2.2.2, current command invocation, remote operator MCP and recovery are documented in [firedrill/README.md](firedrill/README.md). Offline checks preserve actual private case/native key behavior and existing independent authority. Authenticated persisted runs and live quality/cost remain separate parked gates.
