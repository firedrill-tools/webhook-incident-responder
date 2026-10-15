# Working rules

Keep this a standalone synthetic webhook consumer. Use only synthetic service events and no real credentials in fixtures. The model may assess degradation, but sequence checks and notification creation belong to SQLite code. Do not process a stale event after a newer sequence. Preserve event ID idempotency and lease fencing. Stub investigator tests do not prove a live Anthropic run or Slack delivery. Add Firedrill only against a qualified new release, never the retired interface.
