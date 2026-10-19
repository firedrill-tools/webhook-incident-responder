import { spawn } from 'node:child_process';
import { mkdir, mkdtemp, chmod, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { TARGET_INVOCATION_VERSION } from '@firedrill-run/sdk';

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const identifier = value => typeof value === 'string' && value.length > 0 && value.length <= 200 && !/[\x00-\x1f\x7f]/.test(value);

export function readInvocation(value) {
  const fields = ['schemaVersion', 'input', 'scenario', 'configuration', 'runId', 'attemptId', 'tools', 'step'];
  if (!object(value) || Object.keys(value).some(key => !fields.includes(key)) ||
      value.schemaVersion !== TARGET_INVOCATION_VERSION || !object(value.input) ||
      !object(value.scenario) || Object.keys(value.scenario).join(',') !== 'id' || !identifier(value.scenario.id) ||
      !object(value.configuration) || Object.keys(value.configuration).some(key => !['id', 'parameters'].includes(key)) ||
      !identifier(value.configuration.id) || !object(value.configuration.parameters) ||
      !identifier(value.runId) || !identifier(value.attemptId) || !object(value.tools)) {
    throw new Error('Invalid current Firedrill target invocation; controller recipes and assessment data are not target input.');
  }
  if (Object.keys(value.tools).length) throw new Error('This adapter uses local synthetic data; supplied Firedrill Tool bindings are unsupported.');
  if (Object.keys(value.configuration.parameters).some(key => key !== 'model') ||
      ('model' in value.configuration.parameters && !identifier(value.configuration.parameters.model))) {
    throw new Error('Only an explicit model configuration parameter is supported.');
  }
  if (value.step !== undefined && (!object(value.step) ||
      Object.keys(value.step).some(key => !['id', 'ordinal'].includes(key)) ||
      !identifier(value.step.id) || !Number.isSafeInteger(value.step.ordinal) || value.step.ordinal < 0 || value.step.ordinal > 9999)) {
    throw new Error('Invalid sequence step identity.');
  }
  return value;
}

export async function readStdin(stream = process.stdin) {
  const chunks = [];
  let size = 0;
  for await (const chunk of stream) {
    const bytes = Buffer.from(chunk);
    size += bytes.length;
    if (size > 128 * 1024) throw new Error('Target invocation exceeds 128 KiB.');
    chunks.push(bytes);
  }
  const text = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks));
  return readInvocation(JSON.parse(text));
}

export function applicationEnvironment(source = process.env, additions = {}) {
  // Operator authority and CI identity are never application/model credentials.
  return Object.fromEntries(Object.entries({ ...source, ...additions }).filter(([key]) =>
    !key.startsWith('FIREDRILL_') && !key.startsWith('ACTIONS_ID_TOKEN_')));
}

export async function privateDirectory(root, invocation) {
  const parent = join(root, '.runs', 'firedrill');
  await mkdir(parent, { recursive: true, mode: 0o700 });
  const directory = await mkdtemp(join(parent, 'attempt-'));
  await chmod(directory, 0o700);
  // Save the outer execution identity before launching application work. This is
  // a local locator, not the SDK recovery journal or proof of platform authority.
  const identity = invocation === undefined ? null : {
    runId: invocation.runId, attemptId: invocation.attemptId, scenarioId: invocation.scenarioId,
  };
  if (identity && !Object.values(identity).every(identifier)) throw new Error('Invalid invocation identity.');
  await writeFile(join(directory, 'invocation.json'), `${JSON.stringify({ source: 'command_adapter', identity })}\n`,
    { flag: 'wx', mode: 0o600, flush: true });
  // Retain state/evidence after success or failure. A new attempt never resets an old one.
  return directory;
}

export class ApplicationError extends Error {
  constructor(message, evidence) {
    super(message);
    this.evidence = evidence;
  }
}

export async function invokeCLI(argv, { cwd, signal, env, timeoutMs = 240000, maxBytes = 4 * 1024 * 1024 } = {}) {
  if (!Array.isArray(argv) || argv.length === 0 || argv.some(part => typeof part !== 'string' || part.includes('\0'))) {
    throw new Error('Application command must be literal argv.');
  }
  signal?.throwIfAborted();
  return await new Promise((resolve, reject) => {
    const child = spawn(argv[0], argv.slice(1), {
      cwd, env: applicationEnvironment(env), shell: false, stdio: ['ignore', 'pipe', 'pipe'],
      // Stay in the Firedrill runner's owned process group, including descendants.
      detached: false,
    });
    const output = { stdout: [], stderr: [] };
    let size = 0, failure, forced, settled = false;
    const stop = () => {
      failure ??= signal?.reason instanceof Error ? signal.reason : new Error('Application execution stopped.');
      child.kill('SIGTERM');
      forced ??= setTimeout(() => child.kill('SIGKILL'), 5000);
    };
    const timer = setTimeout(() => { failure = new Error('Application deadline exceeded; remote work/billing may continue.'); stop(); }, timeoutMs);
    const finish = (error, code, childSignal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer); clearTimeout(forced);
      signal?.removeEventListener('abort', stop);
      const evidence = {
        exitCode: code ?? null, signal: childSignal ?? null,
        stdout: Buffer.concat(output.stdout).toString('utf8'),
        stderr: Buffer.concat(output.stderr).toString('utf8'),
      };
      if (error || failure || code !== 0) {
        reject(new ApplicationError(error ? `Application could not start: ${error.code ?? error.name}` : failure?.message ?? 'Application exited unsuccessfully.', evidence));
        return;
      }
      try { resolve(JSON.parse(evidence.stdout)); }
      catch { reject(new ApplicationError('Application did not return one JSON value.', evidence)); }
    };
    for (const key of ['stdout', 'stderr']) child[key].on('data', chunk => {
      size += chunk.length;
      if (size > maxBytes) {
        failure = new Error('Application output exceeded the capture bound.'); stop();
      } else output[key].push(chunk);
    });
    child.once('error', error => finish(error));
    child.once('close', (code, childSignal) => finish(null, code, childSignal));
    signal?.addEventListener('abort', stop, { once: true });
    if (signal?.aborted) stop();
  });
}

export async function commandMain(application) {
  const controller = new AbortController();
  const stop = () => controller.abort(new Error('Target process interrupted.'));
  for (const name of ['SIGINT', 'SIGTERM', 'SIGHUP']) process.on(name, stop);
  try {
    const invocation = await readStdin();
    controller.signal.throwIfAborted();
    const result = await application(invocation.input, invocation.configuration.parameters, controller.signal,
      { runId: invocation.runId, attemptId: invocation.attemptId, scenarioId: invocation.scenario.id });
    controller.signal.throwIfAborted();
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } catch (error) {
    process.stderr.write(`${JSON.stringify({ error: error.message, ...(error.evidence ? { application: error.evidence } : {}) })}\n`);
    process.exitCode = controller.signal.aborted ? 130 : 1;
  } finally {
    for (const name of ['SIGINT', 'SIGTERM', 'SIGHUP']) process.removeListener(name, stop);
  }
}
