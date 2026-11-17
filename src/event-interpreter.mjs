import { XMLParser, XMLValidator } from 'fast-xml-parser';
import { EventStreamDecoder } from './eventstream.mjs';
import { CsvAssembler } from './csv.mjs';

const xml = new XMLParser({ ignoreAttributes: false, parseTagValue: false, trimValues: true });

const KNOWN_EVENT_TYPES = new Set(['Records', 'Progress', 'Stats', 'End']);
const PROGRESS_TAGS = new Set(['BytesScanned', 'BytesProcessed', 'BytesReturned']);
const STATS_TAGS = new Set(['BytesScanned', 'BytesProcessed', 'BytesReturned']);
export const DOMAIN_COLUMNS = ['id', 'note'];

function diagnostic(code, message, fatal, extra = {}) {
  return { code, message, severity: fatal ? 'error' : 'warning', fatal, frame: null, offset: null, ...extra };
}

function numberLike(value) {
  if (typeof value === 'number') return value;
  if (typeof value === 'string' && /^\d+$/.test(value.trim())) return Number(value.trim());
  return String(value);
}

function parseXmlPayload(payload, rootName, frameIndex, allowedTags, codeSuffix, label) {
  const text = payload.toString('utf8');
  const validation = XMLValidator.validate(text);
  if (validation !== true) {
    return {
      value: null,
      issue: diagnostic(`invalid-${codeSuffix}`, `${label} 事件不是合法 XML`, false,
        { frame: frameIndex, detail: validation.err?.msg }),
    };
  }
  const parsed = xml.parse(text);
  const root = parsed?.[rootName];
  if (!root || typeof root !== 'object') {
    return {
      value: null,
      issue: diagnostic(`unexpected-${codeSuffix}-shape`, `${label} 事件缺少 ${rootName} 根元素`, false, { frame: frameIndex }),
    };
  }
  const value = { frame: frameIndex };
  for (const key of Object.keys(root)) {
    if (!allowedTags.has(key)) continue;
    value[key] = numberLike(root[key]);
  }
  return { value, issue: null };
}

function parseProgress(payload, frameIndex) {
  return parseXmlPayload(payload, 'Progress', frameIndex, PROGRESS_TAGS, 'progress-xml', 'Progress');
}

function parseStats(payload, frameIndex) {
  const parsed = parseXmlPayload(payload, 'Stats', frameIndex, STATS_TAGS, 'stats-xml', 'Stats');
  if (parsed.value && [...STATS_TAGS].every(key => parsed.value[key] === undefined)) {
    return {
      value: parsed.value,
      issue: diagnostic('unexpected-stats-shape', 'Stats 事件没有可识别的字节统计', false, { frame: frameIndex }),
    };
  }
  return parsed;
}

function parseErrorFrame(frame) {
  const code = frame.headers[':error-code'] ?? frame.headers['error-code'] ?? null;
  const message = frame.headers[':error-message'] ?? frame.headers['error-message'] ?? null;
  const payload = frame.payload.length > 0 ? frame.payload.toString('utf8') : '';
  return diagnostic('source-error',
    `来源返回错误：${code ?? '未知错误码'}${message ? `（${message}）` : ''}`,
    true,
    { frame: frame.index, offset: frame.offset, errorCode: code, errorMessage: message, payload });
}

function buildRecord(row, headerValues, projection) {
  const record = {};
  for (const column of projection) {
    const index = headerValues.indexOf(column);
    record[column] = index >= 0 ? (row.values[index] ?? '') : null;
  }
  // Source frame interval for this record; a record may span several frames.
  record.frames = row.frames;
  return record;
}

/**
 * Streaming interpreter for the source event protocol.
 *
 * Records payloads are fed into the CSV assembler as frames arrive, so a CSV
 * record may span several frames without the whole source being buffered.
 * Progress/Stats metadata is kept apart from the number of bytes actually
 * received. A result becomes complete only after a valid End event and with no
 * fatal framing/CSV issue; closing the connection is never sufficient alone.
 */
export class EventInterpreter {
  constructor({
    source = null,
    sourceVersion = null,
    projection = DOMAIN_COLUMNS,
    maxFrameBytes,
    maxStreamBytes,
  } = {}) {
    this.decoder = new EventStreamDecoder({ maxFrameBytes, maxStreamBytes });
    this.csv = new CsvAssembler();
    this.source = source;
    this.sourceVersion = sourceVersion;
    this.projection = [...projection];
    this.issues = [];
    this.events = [];
    this.progress = [];
    this.statsEvents = [];
    this.recordsFrameIndices = [];
    this.recordsBytes = 0;
    this.ended = false;
    this.endFrame = null;
    this.sourceError = null;
    this.framesAfterEnd = 0;
    this.processCursor = 0;
    this.finished = false;
  }

  push(chunk) {
    if (this.finished) throw new Error('事件解释已结束');
    this.decoder.push(chunk);
    this.#drain();
  }

  #drain() {
    while (this.processCursor < this.decoder.frames.length) {
      this.#handleFrame(this.decoder.frames[this.processCursor]);
      this.processCursor += 1;
    }
  }

  /** Record a transport-level failure without discarding frames already seen. */
  fail(code, message, extra = {}) {
    const entry = diagnostic(code, message, true, extra);
    this.issues.push(entry);
    return entry;
  }

  #handleFrame(frame) {
    if (frame.truncated) return;
    this.events.push({
      frame: frame.index,
      offset: frame.offset,
      bytes: frame.totalLength,
      messageType: frame.messageType,
      eventType: frame.eventType,
      valid: frame.valid,
    });
    if (!frame.valid) return;
    if (this.ended) {
      this.framesAfterEnd += 1;
      return;
    }
    if (frame.messageType === 'error') {
      this.sourceError = parseErrorFrame(frame);
      this.issues.push(this.sourceError);
      return;
    }
    if (frame.messageType !== 'event') {
      this.issues.push(diagnostic('unsupported-message-type', `不支持的消息类型 ${frame.messageType}`, true,
        { frame: frame.index, offset: frame.offset }));
      return;
    }
    if (!KNOWN_EVENT_TYPES.has(frame.eventType)) {
      // Unknown event kinds are preserved as evidence and reported; they are
      // not silently interpreted as records.
      this.issues.push(diagnostic('unsupported-event-type', `不支持的事件类型 ${frame.eventType}`, true,
        { frame: frame.index, offset: frame.offset }));
      return;
    }
    if (frame.eventType === 'Records') {
      this.recordsFrameIndices.push(frame.index);
      this.recordsBytes += frame.payload.length;
      this.csv.push(frame.payload, frame.index);
      return;
    }
    if (frame.eventType === 'Progress') {
      const parsed = parseProgress(frame.payload, frame.index);
      if (parsed.issue) this.issues.push(parsed.issue);
      else this.progress.push(parsed.value);
      return;
    }
    if (frame.eventType === 'Stats') {
      const parsed = parseStats(frame.payload, frame.index);
      if (parsed.issue) this.issues.push(parsed.issue);
      if (parsed.value && [...STATS_TAGS].some(key => parsed.value[key] !== undefined)) {
        this.statsEvents.push(parsed.value);
      }
      return;
    }
    if (frame.eventType === 'End') {
      this.ended = true;
      this.endFrame = frame.index;
      if (frame.payload.length > 0) {
        this.issues.push(diagnostic('end-payload-unexpected', 'End 事件携带了额外 payload，已保留原文', false,
          { frame: frame.index, offset: frame.offset }));
      }
    }
  }

  finish() {
    if (this.finished) return this.#result();
    this.finished = true;

    // Frames emitted only after the final push still need interpretation.
    this.#drain();
    this.decoder.finish();
    this.#drain();
    this.issues.push(...this.decoder.issues);

    const csvResult = this.csv.finish();
    this.issues.push(...csvResult.issues);

    if (this.framesAfterEnd > 0) {
      this.issues.push(diagnostic('frames-after-end', `End 事件之后仍有 ${this.framesAfterEnd} 个有效事件帧`, true,
        { frame: this.endFrame, offset: null }));
    }
    if (!this.ended) {
      this.issues.push(diagnostic('missing-end-event', '事件流未以合法的 End 事件结束，不能视为完整结果', true,
        { frame: null, offset: this.decoder.bufferOffset }));
    }
    if (this.decoder.buffer.length > 0) {
      this.issues.push(diagnostic('trailing-bytes', `事件流末尾有 ${this.decoder.buffer.length} 字节无法组成完整帧`, true,
        { frame: null, offset: this.decoder.bufferOffset }));
    }

    const headerValues = csvResult.header?.values ?? [];
    if (csvResult.header !== null) {
      for (const column of this.projection) {
        if (!headerValues.includes(column)) {
          this.issues.push(diagnostic('missing-domain-column', `CSV 首行缺少必需列 ${column}`, true,
            { frame: csvResult.header.frames[0] ?? null, offset: csvResult.header.bytes[0] }));
        }
      }
    } else if (csvResult.rows.length === 0) {
      this.issues.push(diagnostic('missing-header', 'Records 事件没有给出 CSV 首行', true,
        { frame: this.recordsFrameIndices[0] ?? null, offset: null }));
    }

    this.csvResult = csvResult;
    this.headerValues = headerValues;
    return this.#result();
  }

  #result() {
    const csvResult = this.csvResult ?? this.csv.result();
    const headerValues = this.headerValues ?? csvResult.header?.values ?? [];
    const records = csvResult.rows.map(row => buildRecord(row, headerValues, this.projection));
    const fatal = this.issues.some(issue => issue.fatal);
    const state = fatal ? (records.length > 0 ? 'partial' : 'rejected') : 'complete';
    const complete = state === 'complete';
    const lastStats = this.statsEvents.at(-1) ?? null;
    const sourceStats = lastStats === null ? null : Object.fromEntries(
      [...STATS_TAGS]
        .filter(key => lastStats[key] !== undefined)
        .map(key => [key, lastStats[key]]),
    );
    const receivedBytes = this.decoder.receivedBytes;
    const omittedColumnCount = Math.max(0, headerValues.length - this.projection.length);

    return {
      state,
      complete,
      records,
      issues: this.issues,
      coverage: {
        structured: true,
        complete,
        // Bytes this service actually read from the source connection. This is
        // deliberately separate from the source-side Stats counters below.
        receivedBytes,
        downloadedBytes: receivedBytes,
        recordsBytes: this.recordsBytes,
        recordsFrames: this.recordsFrameIndices,
        recordsFrameCount: this.recordsFrameIndices.length,
        recordsReturned: records.length,
        frames: {
          total: this.decoder.frames.length,
          valid: this.decoder.frames.filter(frame => frame.valid).length,
          invalid: this.decoder.frames.filter(frame => !frame.valid).length,
          records: this.recordsFrameIndices.length,
          progress: this.progress.length,
          stats: this.statsEvents.length,
          end: this.ended ? 1 : 0,
        },
        // Source-side processing statistics (BytesScanned/BytesReturned...).
        sourceStats,
        progressEvents: this.progress,
        endFrame: this.endFrame,
        missingEnd: !this.ended,
        sourceError: this.sourceError ? this.sourceError.errorCode ?? '未知' : null,
        // Only the authorized waybill fields are exposed; other source columns
        // are counted but their names and values are not published.
        projection: [...this.projection],
        projectedColumns: headerValues.filter(name => this.projection.includes(name)),
        omittedColumnCount,
        csvBytes: csvResult.bytes,
      },
    };
  }
}

export function interpretEventStream(bytes, options = {}) {
  const interpreter = new EventInterpreter(options);
  interpreter.push(bytes);
  return interpreter.finish();
}
