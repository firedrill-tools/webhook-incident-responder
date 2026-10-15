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
uv run python -m incident_responder --db incidents.db work --once
uv run python -m incident_responder --db incidents.db state checkout-api
uv run python -m incident_responder --db incidents.db outbox
```

`work` without `--once` drains the currently pending queue and exits. `retry EVENT_ID` requeues a failed event with fewer than three attempts.

To receive a signed webhook, set `INCIDENT_WEBHOOK_SECRET` and run `uv run python -m incident_responder --db incidents.db serve`. Send JSON to `POST /events` with `X-Incident-Signature: sha256=<HMAC-SHA256 of the raw body>`. The endpoint rejects missing/invalid signatures, oversized events, conflicting IDs and reused service sequences. Intake does not call the model; run a worker separately.

To send queued messages to a configured Slack incoming webhook, set `SLACK_WEBHOOK_URL` and explicitly run `uv run python -m incident_responder --db incidents.db send --confirm`. The default outbox command only previews. Slack incoming webhooks do not supply an idempotency key; a crash after Slack accepts a message but before the local sent marker can lead to a duplicate delivery. This example does not execute that external send in tests.

## State and evidence

An event ID replay with the same payload returns its current journal row. Reusing the ID with changed content or assigning one service sequence to a different ID is rejected. A late event whose sequence is at or below the committed service sequence is ignored. A worker lease token prevents an expired worker from committing after replacement. Three attempts are allowed. Incident and outbox changes share one SQLite transaction.

The local tests exercise the actual LangGraph nodes with a stub investigator, HMAC intake, state ordering, retries and lease fencing. They do not establish live Claude quality or Slack delivery. A live proof requires provider and Slack test access. The thin Firedrill connection is pending qualification of the new release. The SQLite journal and HMAC secret are synthetic example choices; a production deployment needs its own source authenticity, secret storage and notification delivery policy.

Explore related applications in the [Firedrill example catalog](https://github.com/firedrill-tools/firedrill-example-catalog). Product and setup references: [Firedrill](https://firedrill.run/) and the [official developer documentation](https://docs.firedrill.run/). Use documentation matching the qualified release; this repository records its current integration limits above.
