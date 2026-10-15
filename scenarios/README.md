# Scenario set

1. **Normal incident:** `degraded-1.json` opens a checkout API incident after a sourced Claude assessment; the outbox receives one alert. `recovered-2.json` clears it and adds one recovery notice.
2. **Duplicate delivery:** resubmit the same event ID and payload. The journal returns the existing row; no second alert appears. A changed payload for that ID is rejected.
3. **Out of order:** submit and process a recovery with sequence 2 before a delayed degradation with sequence 1. The latter becomes `ignored_stale` and cannot reopen the incident.
4. **Worker interruption:** an expired lease can be reclaimed with a new token; the old worker cannot commit a result or alert.
5. **Provider failure:** mark failed, retry up to three attempts, and preserve the original event ID.

Synthetic payloads carry explicit service sequence numbers. The upstream source must supply a monotonically increasing sequence per service; wall clock arrival order is not used as authority. Tests use a stub investigator and a local HTTP client.
