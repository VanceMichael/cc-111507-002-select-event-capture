const CRC_TABLE = new Uint32Array(256);
for (let index = 0; index < 256; index += 1) {
  let value = index;
  for (let bit = 0; bit < 8; bit += 1) {
    value = value & 1 ? (0xedb88320 ^ (value >>> 1)) >>> 0 : value >>> 1;
  }
  CRC_TABLE[index] = value;
}

export const EVENT_STREAM_CONTENT_TYPE = 'application/vnd.amazon.eventstream';
export const PRELUDE_BYTES = 12;
export const MIN_FRAME_BYTES = 16;
export const MAX_FRAME_BYTES = 16 * 1024 * 1024;
export const MAX_STREAM_BYTES = 64 * 1024 * 1024;

export function crc32(bytes) {
  let value = 0xffffffff;
  for (let index = 0; index < bytes.length; index += 1) {
    value = CRC_TABLE[(value ^ bytes[index]) & 0xff] ^ (value >>> 8);
  }
  return (value ^ 0xffffffff) >>> 0;
}

function issue(code, message, frame, offset, fatal = true, extra = {}) {
  return { code, message, frame, offset, severity: fatal ? 'error' : 'warning', fatal, ...extra };
}

function readHeaders(bytes) {
  const headers = Object.create(null);
  let pointer = 0;
  while (pointer < bytes.length) {
    const nameLength = bytes[pointer];
    pointer += 1;
    if (pointer + nameLength > bytes.length) return { headers, error: '头部名称长度越界' };
    const name = bytes.subarray(pointer, pointer + nameLength).toString('utf8');
    pointer += nameLength;
    if (pointer >= bytes.length) return { headers, error: `头部 ${name} 缺少类型` };
    const type = bytes[pointer];
    pointer += 1;
    // The supported source profile only promises string headers (type 7).
    // Anything else is preserved as raw source bytes and reported, never guessed.
    if (type !== 7) return { headers, error: `不支持的头部类型 ${type}` };
    if (pointer + 2 > bytes.length) return { headers, error: `头部 ${name} 长度越界` };
    const length = bytes.readUInt16BE(pointer);
    pointer += 2;
    if (pointer + length > bytes.length) return { headers, error: `头部 ${name} 长度越界` };
    headers[name] = bytes.subarray(pointer, pointer + length).toString('utf8');
    pointer += length;
  }
  return { headers, error: null };
}

function parseFrameBuffer(bytes, offset, index) {
  const totalLength = bytes.readUInt32BE(0);
  const headersLength = bytes.readUInt32BE(4);
  const declaredPreludeCrc = bytes.readUInt32BE(8);
  const computedPreludeCrc = crc32(bytes.subarray(0, 8));
  const declaredMessageCrc = bytes.readUInt32BE(totalLength - 4);
  const computedMessageCrc = crc32(bytes.subarray(0, totalLength - 4));
  const preludeOk = declaredPreludeCrc === computedPreludeCrc;
  const messageOk = declaredMessageCrc === computedMessageCrc;
  const issues = [];
  if (!preludeOk) {
    issues.push(issue('prelude-crc-mismatch', '事件帧 prelude CRC32 校验失败', index, offset, true,
      { declared: declaredPreludeCrc, computed: computedPreludeCrc }));
  }
  if (!messageOk) {
    issues.push(issue('message-crc-mismatch', '事件帧 message CRC32 校验失败', index, offset, true,
      { declared: declaredMessageCrc, computed: computedMessageCrc }));
  }

  let headers = Object.create(null);
  let headerError = null;
  let payload = Buffer.alloc(0);
  if (headersLength > totalLength - MIN_FRAME_BYTES) {
    headerError = `头部长度 ${headersLength} 超出帧范围`;
  } else {
    const decoded = readHeaders(bytes.subarray(PRELUDE_BYTES, PRELUDE_BYTES + headersLength));
    headers = decoded.headers;
    headerError = decoded.error;
    payload = bytes.subarray(PRELUDE_BYTES + headersLength, totalLength - 4);
  }
  if (headerError) {
    issues.push(issue('invalid-frame-headers', headerError, index, offset, true, { headersLength }));
  }
  const valid = preludeOk && messageOk && !headerError;
  const messageType = headers[':message-type'] ?? null;
  const eventType = headers[':event-type'] ?? null;
  if (valid && messageType === null) {
    issues.push(issue('missing-message-type', '事件帧缺少 :message-type 头部', index, offset));
  }
  if (valid && messageType === 'event' && eventType === null) {
    issues.push(issue('missing-event-type', '事件帧缺少 :event-type 头部', index, offset));
  }
  return {
    frame: {
      index,
      offset,
      totalLength,
      headersLength,
      headers,
      payload,
      messageType,
      eventType,
      valid,
      truncated: false,
      crc: {
        prelude: { declared: declaredPreludeCrc, computed: computedPreludeCrc, ok: preludeOk },
        message: { declared: declaredMessageCrc, computed: computedMessageCrc, ok: messageOk },
      },
    },
    issues,
  };
}

function invalidFrame(index, offset, totalLength, headersLength, truncated, available) {
  return {
    index,
    offset,
    totalLength,
    headersLength,
    headers: Object.create(null),
    payload: Buffer.alloc(0),
    messageType: null,
    eventType: null,
    valid: false,
    truncated,
    available,
    crc: null,
  };
}

/**
 * Incremental AWS EventStream decoder. A frame is returned only once all of
 * its bytes have arrived, so callers can process Records payloads while the
 * source response is still streaming. Both CRC32 values are checked before a
 * frame may be interpreted.
 */
export class EventStreamDecoder {
  constructor({ maxFrameBytes = MAX_FRAME_BYTES, maxStreamBytes = MAX_STREAM_BYTES } = {}) {
    this.maxFrameBytes = maxFrameBytes;
    this.maxStreamBytes = maxStreamBytes;
    this.buffer = Buffer.alloc(0);
    this.bufferOffset = 0;
    this.receivedBytes = 0;
    this.frames = [];
    this.issues = [];
    this.stopped = false;
  }

  push(chunk) {
    if (this.stopped) return { frames: [], issues: [] };
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    this.receivedBytes += bytes.length;
    if (this.receivedBytes > this.maxStreamBytes) {
      this.stopped = true;
      const entry = issue('stream-limit-exceeded', `事件流超出收件限额 ${this.maxStreamBytes} 字节`, null, this.bufferOffset);
      this.issues.push(entry);
      return { frames: [], issues: [entry] };
    }
    this.buffer = this.buffer.length === 0 ? bytes : Buffer.concat([this.buffer, bytes]);
    const newFrames = [];
    const newIssues = [];
    while (!this.stopped && this.buffer.length >= PRELUDE_BYTES) {
      const totalLength = this.buffer.readUInt32BE(0);
      const headersLength = this.buffer.readUInt32BE(4);
      if (totalLength < MIN_FRAME_BYTES) {
        const entry = issue('invalid-frame-length', `事件帧长度 ${totalLength} 小于最小值 ${MIN_FRAME_BYTES}`, this.frames.length, this.bufferOffset, true, { totalLength });
        this.issues.push(entry); newIssues.push(entry);
        this.frames.push(invalidFrame(this.frames.length, this.bufferOffset, totalLength, headersLength, false, this.buffer.length));
        this.stopped = true;
        break;
      }
      if (totalLength > this.maxFrameBytes) {
        const entry = issue('invalid-frame-length', `事件帧长度 ${totalLength} 超出单帧限额 ${this.maxFrameBytes}`, this.frames.length, this.bufferOffset, true, { totalLength });
        this.issues.push(entry); newIssues.push(entry);
        this.frames.push(invalidFrame(this.frames.length, this.bufferOffset, totalLength, headersLength, false, this.buffer.length));
        this.stopped = true;
        break;
      }
      if (this.buffer.length < totalLength) break;
      const { frame, issues } = parseFrameBuffer(this.buffer.subarray(0, totalLength), this.bufferOffset, this.frames.length);
      this.frames.push(frame);
      newFrames.push(frame);
      for (const entry of issues) { this.issues.push(entry); newIssues.push(entry); }
      this.buffer = this.buffer.subarray(totalLength);
      this.bufferOffset += totalLength;
    }
    return { frames: newFrames, issues: newIssues };
  }

  finish() {
    if (this.stopped || this.buffer.length === 0) return { issues: [] };
    const available = this.buffer.length;
    const totalLength = available >= 4 ? this.buffer.readUInt32BE(0) : null;
    const headersLength = available >= 8 ? this.buffer.readUInt32BE(4) : null;
    const entry = issue('truncated-frame',
      totalLength === null ? `事件帧头部不完整：仅剩 ${available} 字节` : `事件帧不完整：声明 ${totalLength} 字节，仅剩 ${available} 字节`,
      this.frames.length, this.bufferOffset, true, { totalLength, available });
    this.issues.push(entry);
    this.frames.push(invalidFrame(this.frames.length, this.bufferOffset, totalLength, headersLength, true, available));
    this.buffer = Buffer.alloc(0);
    return { issues: [entry] };
  }
}

export function parseEventStream(bytes, options = {}) {
  const decoder = new EventStreamDecoder(options);
  decoder.push(bytes);
  decoder.finish();
  return {
    frames: decoder.frames,
    issues: decoder.issues,
    consumedBytes: decoder.bufferOffset,
    trailingBytes: decoder.buffer.length,
    receivedBytes: decoder.receivedBytes,
  };
}
