import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createService } from '../src/server.mjs';

test('结构化收件：来源记录、可查询性及覆盖身份', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'structured-check-'));
  const service = await createService({ dataDir });
  try {
    const response = await fetch(`${service.baseUrl}/v1/intakes`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'structured' }),
    });
    assert.equal(response.status, 201);
    const result = await response.json();
    assert.equal(result.state, 'complete');
    assert.ok(Array.isArray(result.records));
    assert.ok(Array.isArray(result.issues));
    assert.equal(result.records.length, 2);
    assert.equal(result.records[0].note, 'crate\nA');
    assert.equal(result.records[1].note, 'box');
    const saved = await (await fetch(`${service.baseUrl}/v1/receipts/${result.id}`)).json();
    assert.deepEqual(saved.records, result.records);
    assert.deepEqual(saved.coverage, result.coverage);
    const published = await fetch(`${service.baseUrl}/v1/receipts/${result.id}/records`);
    assert.equal(published.status, 200);
    assert.deepEqual(await published.json(), result.records);
  } finally {
    await service.close();
    await rm(dataDir, { recursive: true, force: true });
  }
});
