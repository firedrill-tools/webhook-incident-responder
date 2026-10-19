import { fileURLToPath } from 'node:url';
import { resolve, join } from 'node:path';
import { ApplicationError, commandMain, invokeCLI, privateDirectory } from './adapter-runtime.mjs';
const root = fileURLToPath(new URL('../', import.meta.url));
const cases = new Set(["checkout-degradation", "delayed-recovery", "semantic-replay", "regional-busy", "sparse-telemetry", "hostile-log", "recovery-before-degradation", "stale-degradation", "maximum-sequence", "expired-worker", "exhausted-failure", "already-current"]);
export async function runApplication(input, parameters, signal, invocation) {
  if (Object.keys(input).some(key => !['caseId', 'action'].includes(key)) || !cases.has(input.caseId) || !['work', 'inspect'].includes(input.action)) throw new Error('Select a reviewed case and foreground work/inspect action; sending and delivery decisions are separate.');
  const directory = await privateDirectory(root, invocation), workspace = join(directory, 'workspace');
  const cli = ['uv', 'run', '--locked', '--no-sync', 'incident-responder'];
  try {
    const initialized = await invokeCLI([...cli, 'init', '--directory', workspace, '--case', input.caseId], { cwd: root, signal });
    const db = initialized.database;
    if (typeof db !== 'string' || !db.startsWith(workspace + '/')) throw new Error('Initialized database is outside its owned workspace.');
    const argv = input.action === 'inspect' ? [...cli, 'workspace', workspace] : [...cli, '--db', db, 'work', '--once', '--output-root', join(directory, 'receipts')];
    if (input.action === 'work' && parameters.model) argv.push('--model', parameters.model);
    return { executionKind: input.action === 'inspect' ? 'application_state_inspection' : 'foreground_worker_once',
      application: await invokeCLI(argv, { cwd: root, signal }), localState: { directory, workspaceRoot: workspace, databaseFile: db } };
  } catch (error) { if (error instanceof ApplicationError) error.evidence.runDirectory = directory; throw error; }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await commandMain(runApplication);
