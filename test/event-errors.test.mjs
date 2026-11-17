import test from 'node:test';
import assert from 'node:assert/strict';
import { crc32 } from '../src/eventstream.mjs';
import { interpretEventStream } from '../src/event-interpreter.mjs';
import { collect } from '../src/receiver.mjs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createGateway } from '../src/gateway.mjs';
import { ReceiptStore } from '../src/store.mjs';

function encodeHeaders(headers) {
  const parts = [];
  for (const [name, value] of Object.entries(headers)) {
    const nameBytes = Buffer.from(name);
    const valueBytes = Buffer.from(value);
    const head = Buffer.alloc(1 + nameBytes.length + 1 + 2);
    head.writeUInt8(nameBytes.length, 0);
    nameBytes.copy(head, 1);
    head.writeUInt8(7, 1 + nameBytes.length);
    head.writeUInt16BE(valueBytes.length, 2 + nameBytes.length);
    parts.push(head, valueBytes);
  }
  return Buffer.concat(parts);
}

export function buildFrame({ headers, payload = Buffer.alloc(0) }) {
  const headerBytes = encodeHeaders(headers);
  const total = 12 + headerBytes.length + payload.length + 4;
  const frame = Buffer.alloc(total);
  frame.writeUInt32BE(total, 0);
  frame.writeUInt32BE(headerBytes.length, 4);
  frame.writeUInt32BE(crc32(frame.subarray(0, 8)), 8);
  headerBytes.copy(frame, 12);
  payload.copy(frame, 12 + headerBytes.length);
  frame.writeUInt32BE(crc32(frame.subarray(0, total - 4)), total - 4);
  return frame;
}

const records = (text) => buildFrame({ headers: { ':message-type': 'event', ':event-type': 'Records' }, payload: Buffer.from(text) });
const end = () => buildFrame({ headers: { ':message-type': 'event', ':event-type': 'End' } });
const stats = (xml) => buildFrame({ headers: { ':message-type': 'event', ':event-type': 'Stats' }, payload: Buffer.from(xml) });

test('来源 error 事件不能发布为完整清单', () => {
  const error = buildFrame({
    headers: { ':message-type': 'error', ':error-code': 'NoSuchKey', ':error-message': '对象不存在' },
  });
  const stream = Buffer.concat([records('id,note\r\n1,x\r\n'), error]);
  const result = interpretEventStream(stream);
  assert.equal(result.complete, false);
  assert.equal(result.state, 'partial');
  assert.equal(result.issues[0].code, 'source-error');
  assert.equal(result.issues[0].errorCode, 'NoSuchKey');
  assert.equal(result.coverage.sourceError, 'NoSuchKey');
});

test('End 之后的额外事件帧使结果不完整', () => {
  const stream = Buffer.concat([records('id,note\r\n1,x\r\n'), end(), records('2,y\r\n')]);
  const result = interpretEventStream(stream);
  assert.equal(result.complete, false);
  assert.equal(result.state, 'partial');
  assert.ok(result.issues.some(issue => issue.code === 'frames-after-end' && issue.fatal));
});

test('未授权列不进入 records，仅记录省略列数', () => {
  const stream = Buffer.concat([
    records('id,note,phone\r\n1,"a\nb",13800000000\r\n'),
    end(),
  ]);
  const result = interpretEventStream(stream);
  assert.equal(result.state, 'complete');
  assert.deepEqual(result.records, [{ id: '1', note: 'a\nb', frames: [0, 0] }]);
  assert.equal(JSON.stringify(result.records).includes('13800000000'), false);
  assert.equal(result.coverage.omittedColumnCount, 1);
  assert.deepEqual(result.coverage.projectedColumns, ['id', 'note']);
});

test('记录可跨越三个以上事件帧', () => {
  const stream = Buffer.concat([
    records('id,note\r\n1,"first'),
    records(' second'),
    records(' third"\r\n'),
    end(),
  ]);
  const result = interpretEventStream(stream);
  assert.equal(result.state, 'complete');
  assert.equal(result.records[0].note, 'first second third');
  assert.deepEqual(result.records[0].frames, [0, 2]);
});

test('Stats 只留在 coverage，且与本地接收量分开', () => {
  const xml = '<Stats><BytesScanned>999</BytesScanned><BytesReturned>42</BytesReturned></Stats>';
  const stream = Buffer.concat([records('id,note\r\n1,x\r\n'), stats(xml), end()]);
  const result = interpretEventStream(stream);
  assert.equal(result.state, 'complete');
  assert.deepEqual(result.coverage.sourceStats, { BytesScanned: 999, BytesReturned: 42 });
  assert.equal(result.coverage.receivedBytes, stream.length);
  assert.notEqual(result.coverage.receivedBytes, 999);
  assert.deepEqual(result.records[0], { id: '1', note: 'x', frames: [0, 0] });
});

test('来源 HTTP 错误形成 rejected 收件而不是异常丢失', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'structured-transport-'));
  const gateway = await createGateway('standard');
  const store = new ReceiptStore(dataDir);
  await store.open();
  try {
    const result = await collect({
      input: { mode: 'structured' },
      descriptor: { ...gateway.descriptor, entry: 'not-provided.events' },
      sourceBase: gateway.baseUrl,
      store,
    });
    assert.equal(result.receipt.state, 'rejected');
    assert.equal(result.receipt.records.length, 0);
    assert.ok(result.receipt.issues.some(issue => issue.code === 'source-http-error'));
    assert.equal(result.receipt.evidence.complete, false);
    assert.equal(result.receipt.evidence.transportError.status, 404);
  } finally {
    await gateway.close();
    await rm(dataDir, { recursive: true, force: true });
  }
});
