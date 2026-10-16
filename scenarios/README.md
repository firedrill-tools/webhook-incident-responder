# Scenario set

1. **Normal incident:** `degraded-1.json` opens a checkout API incident after a sourced Claude assessment; the outbox receives one alert. `recovered-2.json` clears it and adds one recovery notice.
2. **Duplicate delivery:** resubmit the same event ID and payload. The journal returns the existing row; no second alert appears. A changed payload for that ID is rejected.
3. **Out of order:** submit and process a recovery with sequence 2 before a delayed degradation with sequence 1. The latter becomes `ignored_stale` and cannot reopen the incident.
4. **Worker interruption:** an expired lease can be reclaimed with a new token; the old worker cannot commit a result or alert.
5. **Provider failure:** mark failed, retry up to three attempts, and preserve the original event ID.

Synthetic payloads carry explicit service sequence numbers. The upstream source must supply a monotonically increasing sequence per service; wall clock arrival order is not used as authority. Tests retain the original stub checks and also use the actual pinned native SDK over owned HTTP, normal CLI processes, signed intake and controlled delivery. No paid Claude or Slack message is sent.


The source-authority dataset index is [data/index.json](../data/index.json), with twelve versioned starter/busy/ambiguous/boundary cases. Use `incident-responder datasets`, then exclusive `init --case CASE --directory PATH`. The busy source has twelve services/72 events and48 imported historical notifications. Historical source is synthetic operator fiction; original model/usage/authentication/real Slack observations are unavailable. Task guidance is stored separately from model business context. Unknown notification attempts require explicit resolution and cannot automatically retry. [README.md](../README.md) documents private initialization/readback/reset and fresh complete-source qualification.
