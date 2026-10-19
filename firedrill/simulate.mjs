import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { Firedrill, FiredrillError, simulationExitCode } from '@firedrill-run/sdk';

const root = fileURLToPath(new URL('../', import.meta.url));
const args = process.argv.slice(2);
if (args.length === 1 && args[0] === '--help') {
  console.log('node firedrill/simulate.mjs [--preview | --resume OPERATION_ID | --result BATCH_ID | --run BATCH_ID]');
  process.exit(0);
}
const selected = args.length === 0 ? 'simulate' : args.length === 1 && args[0] === '--preview' ? 'preview'
  : args.length === 2 && ['--resume', '--result', '--run'].includes(args[0]) && args[1].trim() ? args[0].slice(2) : null;
if (!selected) {
  console.error('Invalid selection. Use --help. No work was started.');
  process.exit(2);
}
process.chdir(root);
try {
  const client = existsSync('firedrill.config.json') ? Firedrill.fromConfig() : new Firedrill();
  const recipe = process.env.FIREDRILL_RECIPE_FILE ?? 'firedrill/scenario.json';
  const result = selected === 'preview' ? await client.preview(recipe)
    : selected === 'resume' ? await client.resume(args[1])
    : selected === 'result' ? await client.result(args[1])
    : selected === 'run' ? await client.run(args[1])
    : await client.simulate(recipe);
  // Keep operation IDs and the SDK's diagnostics for exact recovery; no automatic retry.
  console.log(JSON.stringify(result));
  process.exitCode = selected === 'preview' ? 0 : simulationExitCode(result);
} catch (error) {
  const diagnostic = error instanceof FiredrillError ? error.toJSON() : { error: { message: error.message } };
  console.error(JSON.stringify(diagnostic));
  const category = diagnostic.error.category;
  process.exitCode = ['interrupted', 'uncertain'].includes(category) ? 6
    : ['validation', 'unsupported'].includes(category) ? 2
    : ['authentication', 'permission'].includes(category) ? 3
    : category === 'capacity' ? 4 : 5;
}
