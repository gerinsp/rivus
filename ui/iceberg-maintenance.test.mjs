import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const dataURL = (source) => 'data:text/javascript;base64,' + Buffer.from(source).toString('base64');
const apiURL = dataURL(fs.readFileSync(new URL('./core/api.js', import.meta.url), 'utf8'));
const source = fs.readFileSync(new URL('./iceberg-maintenance.js', import.meta.url), 'utf8');
const { renderIcebergMaintenance } = await import(dataURL(source.replace('./core/api.js', apiURL)));

function render(table, options = {}) {
  const panel = {
    dataset: {},
    classList: { add() {}, remove() {} },
    querySelector: () => null,
    querySelectorAll: () => [],
  };
  renderIcebergMaintenance({
    id: 'test-job', sink_type: 'iceberg_native',
    iceberg_maintenance: {
      enabled: true, state: table.state, data_files_threshold: 50,
      equality_delete_files_threshold: 25, position_delete_files_threshold: 10,
      tables_total: 1, tables_scanned: 1, tables: [table],
    },
  }, { panel, ...options });
  return panel.innerHTML;
}

const partitioned = {
  identifier: 'ga4_bronze.daily_overview', state: 'partitioned',
  active_data_files: 648, eligible_small_files: 648,
  compactable_files: 0, compaction_groups: 0,
  partition_fields: ['date'],
  applicable_equality_delete_files: 0, applicable_position_delete_files: 0,
  checked_at: '2026-10-15T04:00:00Z',
};

test('table totals keep their trigger while explaining why partitioned files are ineligible', () => {
  const html = render(partitioned);
  assert.match(html, /Small:[^\n]*648 \/ 50/);
  assert.match(html, /0 files eligible\. No eligible groups within partitions\./);
  assert.match(html, /Partitioned · date/);
  assert.match(html, /No eligible file groups/);
  assert.doesNotMatch(html, /Ready for maintenance|Eligible small files|Eligible small bytes/);
});

test('the same file ratio distinguishes partitioned and unpartitioned tables from metadata', () => {
  const partitionedHTML = render({ ...partitioned, state: 'stale', eligible_small_files: 657 });
  const unpartitionedHTML = render({ ...partitioned, state: 'ready', eligible_small_files: 657, partition_fields: [], compactable_files: 657, compaction_groups: 1 });
  assert.match(partitionedHTML, /657 \/ 50/);
  assert.match(unpartitionedHTML, /657 \/ 50/);
  assert.match(partitionedHTML, /Partitioned · date/);
  assert.match(partitionedHTML, /0 files eligible at last scan/);
  assert.match(unpartitionedHTML, /Unpartitioned/);
  assert.match(unpartitionedHTML, /657 files in 1 eligible group/);
  assert.match(unpartitionedHTML, /Ready for maintenance/);
  assert.doesNotMatch(unpartitionedHTML, /within partitions/);
});

test('partitioning is not guessed from file counts or the worker state', () => {
  for (const partition_fields of [undefined, null, [''], [42]]) {
    const html = render({ ...partitioned, partition_fields });
    assert.match(html, /Partitioning pending scan/);
    assert.doesNotMatch(html, /Partitioned ·|Unpartitioned/);
  }
  assert.match(render({ ...partitioned, partition_fields: ['day(created_at)', '<region>'] }), /Partitioned · day\(created_at\), &lt;region&gt;/);
});

test('cached eligibility remains explicitly historical when inventory is stale or refreshing', () => {
  for (const state of ['stale', 'inventory_pending', 'scanning']) {
    const html = render({ ...partitioned, state });
    assert.match(html, /0 files eligible at last scan/);
    assert.doesNotMatch(html, /Ready for maintenance/);
  }
});

test('missing planner counts or a missing inventory are not presented as zero eligibility', () => {
  for (const table of [
    { ...partitioned, compactable_files: undefined, compaction_groups: undefined },
    { ...partitioned, checked_at: undefined },
  ]) {
    const html = render(table);
    assert.match(html, /Eligibility pending scan/);
    assert.doesNotMatch(html, /0 files eligible/);
  }
});

test('eligible groups and delete triggers preserve the worker status for both panel owners', () => {
  for (const historyOwnerID of ['job-1', 'monitor:maintenance-catalogs']) {
    const html = render({ ...partitioned, state: 'ready', compactable_files: 20, compaction_groups: 2, applicable_equality_delete_files: 30 }, { historyOwnerID });
    assert.match(html, /20 files in 2 eligible groups/);
    assert.match(html, /Ready for maintenance/);
    assert.match(html, /Equality:[^\n]*30 \/ 25/);
  }
});

test('worker errors and routing reasons retain precedence and are escaped', () => {
  const errorHTML = render({ ...partitioned, state: 'error', error: '<failed scan>' });
  assert.match(errorHTML, /&lt;failed scan&gt;/);
  assert.doesNotMatch(errorHTML, /0 files eligible/);
  const routeHTML = render({ ...partitioned, operations: ['compact', 'route:Compaction skipped: no eligible group'] });
  assert.match(routeHTML, /Compaction skipped: no eligible group/);
});
