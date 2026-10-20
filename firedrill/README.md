# Firedrill agent-first integration

The optional toolchain pins released `@firedrill-run/cli` and `@firedrill-run/sdk` **2.2.2** (October 19, 2026), Node 22+. The app retains its locked native provider/framework dependencies.

## Set up Firedrill

From the repository root, install this application's locked dependencies and the optional simulation clients:

```sh
uv sync --locked --extra test
npm ci --prefix firedrill --ignore-scripts --no-audit --no-fund
```

Sign in and choose the Firedrill project and environment once:

```sh
npx firedrill-run init
```

Complete browser authorization when prompted. Initialization writes `firedrill.config.json` in this repository; it does not run the AI agent. Installation choices are optional: skip duplicate SDK/client installation if you are using the locked `firedrill/` toolchain above. Keep this application's runtime requirements from the root README; the clients' Node.js minimum does not replace them.

Automation uses protected `FIREDRILL_TOKEN`, `FIREDRILL_PROJECT_ID`, `FIREDRILL_ENVIRONMENT_ID`. Model operations independently need `ANTHROPIC_API_KEY` and an enabled `ANTHROPIC_MODEL` or recipe model parameter. Do not put credentials in recipes.

After scope and provider budget are authorized:

```sh
node firedrill/simulate.mjs --preview
node firedrill/simulate.mjs
```

The bounded sequential recipe initializes a real private incident journal and invokes the normal foreground `work --once` CLI, LangGraph/Anthropic assessment and fenced SQLite commitment. Starter/sparse telemetry cases process at most one queued event; regional-busy inspection reads actual case identity/counts without inference. Empty queues, stale/ordered source recovery and replay can complete without a fresh model call; inspect the actual original receipt/event/native observations. Worker return/status, model measurements and notification delivery remain separate. No sender, webhook URL, retry, lease release or operator delivery decision is exposed by the adapter. Notifications remain synthetic local outbox state, with the original unknown-dispatch/no-auto-retry rules preserved. No real Slack delivery or injected assessor is added.

Every attempt saves outer run/attempt/scenario IDs in private `.runs/firedrill/attempt-*/invocation.json` before creating a distinct absent workspace leaf. The actual initializer's returned database identity is checked inside that workspace; paths/case references and prior authority stay outside model input. New attempts never reset earlier DB/WAL/SHM, decisions or receipts. Normal application failure remains nonzero with actual CLI stdout/stderr/run directory. Preserve partial effects and unknown source/cleanup/publication/billing outcomes; local cancellation does not prove remote work stopped or effects rolled back. Paths are local evidence, not uploaded platform artifacts. Current ID-only invocation accepts only reviewed explicit input/model, rejects controller assessments/unsupported Tool bindings, and removes operator/CI credentials before child dispatch.

## Offline verification

These checks are optional when getting started; they do not authorize an account or run a paid model:

```sh
node firedrill/node_modules/@firedrill-run/cli/bin/firedrill.mjs validate firedrill/scenario.json --json --non-interactive
npm test --prefix firedrill
```

The saved recipe is an observation/exploration example, not a claim of agent correctness. It has no Firedrill rubric or CI gate. Execution success and any application-owned checks are distinct from a scored assessment. Local evidence paths are not uploaded captures; the evidence and proof limits described in this guide remain in effect.

## MCP and recovery

Configure remote OAuth operator MCP at **https://mcp.firedrill.run/mcp** using [host setup](https://docs.firedrill.run/guides/coding-agents). MCP prepares, queues and inspects; the local SDK/CLI executes the command target. Host OAuth and execution login are separate. Use `--run BATCH_ID` to attach queued work, `--resume OPERATION_ID` after uncertainty, and `--result BATCH_ID` to read retained results. Preserve SDK journals and local evidence. These scripts retain public SDK diagnostics and whole-batch exit status; a new simulation starts new work.

CI and the fresh locked source archive run offline integration tests with actual private business state and the native missing-key boundary. They establish wiring and cleanup, not live model quality, platform persistence, or uploaded captures. The observation recipe defines no grader/gate. Authenticated persisted happy/negative runs, paid model quality/cost, publication and licensing remain separate gates.

References: [SDK](https://docs.firedrill.run/sdk/typescript), [command targets](https://docs.firedrill.run/guides/callbacks), [operator MCP](https://docs.firedrill.run/guides/control-mcp), [CI](https://docs.firedrill.run/guides/ci).
