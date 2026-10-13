import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const api = await import('data:text/javascript;base64,' + fs.readFileSync(new URL('./core/api.js', import.meta.url)).toString('base64'));
test('stopped/stopping/failed jobs never present stale streaming as current activity', () => {
 for (const status of ['STOPPED','STOPPING','PAUSED','FAILED','DONE']) {
  const saved={phase:'streaming',summary:'CDC streaming',sink_summary:'Committing',checkpoint_pending:true};
  const shown=api.jobRuntimeProgress({status,progress:saved});
  assert.notEqual(shown.phase,'streaming');
  assert.notEqual(shown.summary,'CDC streaming');
  assert.equal(shown.sink_summary,undefined);
  assert.equal(shown.checkpoint_pending,undefined);
  assert.equal(saved.phase,'streaming');
 }
});
test('running/recovering progress stays visible, queued does not use stale CDC', () => {
 const progress={phase:'recovering',summary:'Verifying commit'};
 assert.equal(api.jobRuntimeProgress({status:'RUNNING',progress}),progress);
 assert.equal(api.jobRuntimeProgress({status:'QUEUED',progress:{phase:'streaming',summary:'CDC streaming'}}).phase,'queued');
});
