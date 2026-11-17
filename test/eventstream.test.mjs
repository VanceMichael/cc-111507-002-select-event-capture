import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createService } from '../src/server.mjs';
import { interpretEventStream } from '../src/event-interpreter.mjs';
import { EventStreamDecoder, crc32, parseEventStream } from '../src/eventstream.mjs';

const fixture = (name) => readFile(new URL(`../fixtures/${name}`, import.meta.url));

test('正常事件流：跨帧 CSV 记录与来源帧区间', async () => {
  const result = interpretEventStream(await fixture('query.events'));
  assert.equal(result.state, 'complete');
  assert.equal(result.complete, true);
  assert.equal(result.issues.length, 0);
  assert.equal(result.records.length, 2);
  assert.deepEqual(result.records.map(record => record.id), ['1', '2']);
  // The first CSV record starts in frame 0 and is completed by frame 2.
  assert.equal(result.records[0].note, 'crate\nA');
  assert.deepEqual(result.records[0].frames, [0, 2]);
  assert.equal(result.records[1].note, 'box');
  assert.deepEqual(result.records[1].frames, [2, 2]);
  // Source-side stats live in coverage and stay separate from received bytes.
  assert.equal(result.coverage.sourceStats.BytesScanned, 256);
  assert.equal(result.coverage.sourceStats.BytesReturned, 29);
  assert.equal(result.coverage.receivedBytes, 457);
  assert.equal(result.coverage.recordsBytes, 29);
  assert.equal(result.coverage.missingEnd, false);
});

test('逐字节分片：帧边界与 CRLF 跨分片仍能还原记录', async () => {
  const bytes = await fixture('query.events');
  const interpreter = (await import('../src/event-interpreter.mjs')).EventInterpreter;
  const streaming = new interpreter();
  for (const byte of bytes) streaming.push(Buffer.from([byte]));
  const result = streaming.finish();
  assert.equal(result.state, 'complete');
  assert.deepEqual(result.records.map(record => [record.id, record.note]), [['1', 'crate\nA'], ['2', 'box']]);
  assert.deepEqual(result.records[0].frames, [0, 2]);
});

test('message CRC 损坏不得发布完整结果', async () => {
  const result = interpretEventStream(await fixture('broken.events'));
  assert.equal(result.complete, false);
  assert.equal(result.state, 'partial');
  assert.ok(result.issues.some(issue => issue.code === 'message-crc-mismatch' && issue.frame === 4));
  assert.ok(result.issues.some(issue => issue.code === 'missing-end-event'));
  assert.equal(result.coverage.frames.invalid, 1);
  assert.equal(result.coverage.frames.end, 0);
});

test('没有 End 的连接关闭不等于查询完成', async () => {
  const result = interpretEventStream(await fixture('unfinished.events'));
  assert.equal(result.complete, false);
  assert.equal(result.state, 'partial');
  assert.ok(result.issues.some(issue => issue.code === 'missing-end-event'));
  assert.equal(result.coverage.missingEnd, true);
});

test('截断流既无记录也无 End，视为 rejected', async () => {
  const result = interpretEventStream(await fixture('truncated.events'));
  assert.equal(result.state, 'rejected');
  assert.equal(result.records.length, 0);
  assert.ok(result.issues.some(issue => issue.code === 'truncated-frame'));
  assert.ok(result.issues.some(issue => issue.code === 'missing-end-event'));
});

test('prelude CRC 损坏同样拒绝该帧', () => {
  const frame = Buffer.alloc(16);
  frame.writeUInt32BE(16, 0);
  frame.writeUInt32BE(0, 4);
  frame.writeUInt32BE(0xdeadbeef, 8);
  frame.writeUInt32BE(crc32(frame.subarray(0, 12)), 12);
  const decoder = new EventStreamDecoder();
  decoder.push(frame);
  decoder.finish();
  assert.equal(decoder.frames[0].valid, false);
  assert.ok(decoder.issues.some(issue => issue.code === 'prelude-crc-mismatch'));
});

test('帧头只支持 string(7)，其他类型显式报告', () => {
  const payload = Buffer.alloc(0);
  const headers = Buffer.from([0x01, 0x3a, 0x02, 0x00]);
  const total = 12 + headers.length + payload.length + 4;
  const frame = Buffer.alloc(total);
  frame.writeUInt32BE(total, 0);
  frame.writeUInt32BE(headers.length, 4);
  frame.writeUInt32BE(crc32(frame.subarray(0, 8)), 8);
  headers.copy(frame, 12);
  frame.writeUInt32BE(crc32(frame.subarray(0, total - 4)), total - 4);
  const parsed = parseEventStream(frame);
  assert.equal(parsed.frames[0].valid, false);
  assert.ok(parsed.issues.some(issue => issue.code === 'invalid-frame-headers'));
});

test('连接结束但来源提前关闭：保留已到达记录且不为 complete', async () => {
  // Truncate after the Stats frame: valid frames so far, but no End.
  const bytes = await fixture('query.events');
  const result = interpretEventStream(bytes.subarray(0, 401));
  assert.equal(result.state, 'partial');
  assert.equal(result.complete, false);
  assert.equal(result.records.length, 2);
  assert.ok(result.issues.some(issue => issue.code === 'missing-end-event'));
});

for (const scenario of ['standard', 'bad-crc', 'no-end', 'truncated']) {
  test(`结构化收件场景：${scenario}`, async () => {
    const dataDir = await mkdtemp(join(tmpdir(), `structured-${scenario}-`));
    const service = await createService({ dataDir, scenario });
    try {
      const response = await fetch(`${service.baseUrl}/v1/intakes`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ mode: 'structured' }),
      });
      assert.equal(response.status, 201);
      const receipt = await response.json();
      const expected = { standard: 'complete', 'bad-crc': 'partial', 'no-end': 'partial', truncated: 'rejected' }[scenario];
      assert.equal(receipt.state, expected);
      assert.equal(receipt.coverage.complete, expected === 'complete');
      assert.equal(receipt.evidence.complete, expected === 'complete');
      assert.equal(typeof receipt.coverage.receivedBytes, 'number');
      assert.equal(receipt.coverage.receivedBytes, receipt.evidence.receivedBytes);

      // Downstream result endpoint refuses unconfirmed results.
      const recordsResponse = await fetch(`${service.baseUrl}/v1/receipts/${receipt.id}/records`);
      if (expected === 'complete') {
        assert.equal(recordsResponse.status, 200);
        assert.equal((await recordsResponse.json()).length, 2);
      } else {
        assert.equal(recordsResponse.status, 409);
        assert.equal((await recordsResponse.json()).error, '查询结果尚未确认完成');
      }

      // The generic receipt view also withholds unconfirmed records, while
      // the on-disk receipt keeps them for audit.
      if (expected !== 'complete') {
        assert.equal(receipt.records.length, 0);
        assert.equal(receipt.published, false);
        assert.ok(receipt.recordsWithheld >= 0);
        const raw = JSON.parse(await readFile(join(dataDir, 'receipts', `${receipt.id}.json`), 'utf8'));
        assert.ok(Array.isArray(raw.records));
        if (scenario !== 'truncated') assert.ok(raw.records.length > 0);
        assert.equal(raw.recordsWithheld, undefined);
      }

      // The receipt is still queryable after reopening the same DATA_DIR.
      await service.close();
      const reopened = await createService({ dataDir, scenario });
      try {
        const saved = await (await fetch(`${reopened.baseUrl}/v1/receipts/${receipt.id}`)).json();
        assert.deepEqual(saved.records, receipt.records);
        assert.deepEqual(saved.coverage, receipt.coverage);
        const listed = await (await fetch(`${reopened.baseUrl}/v1/receipts`)).json();
        assert.equal(listed.length, 1);
        assert.deepEqual(listed[0].records, receipt.records);
      } finally { await reopened.close(); }
    } finally {
      await service.close().catch(() => {});
      await rm(dataDir, { recursive: true, force: true });
    }
  });
}

test('原始收件仍保持既有形状', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'raw-shape-'));
  const service = await createService({ dataDir });
  try {
    const receipt = await (await fetch(`${service.baseUrl}/v1/intakes`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ mode: 'raw' }),
    })).json();
    assert.equal(receipt.state, 'received');
    assert.equal(receipt.coverage.structured, false);
    assert.equal(receipt.records.length, 0);
  } finally { await service.close(); await rm(dataDir, { recursive: true, force: true }); }
});
