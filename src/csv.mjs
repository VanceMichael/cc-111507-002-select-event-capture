import { StringDecoder } from 'node:string_decoder';

/**
 * Assemble RFC4180 CSV records from an arbitrary sequence of byte chunks.
 * Records may span event frames, so parsing happens on the concatenated
 * Records payload while byte offsets still map back to source frames.
 */
export class CsvAssembler {
  constructor() {
    this.decoder = new StringDecoder('utf8');
    this.pending = '';
    this.mappings = [];
    this.rawOffset = 0;
    this.decodedBytes = 0;
    this.processedBytes = 0;
    this.field = '';
    this.fields = [];
    this.inQuotes = false;
    this.afterQuote = false;
    this.atFieldStart = true;
    this.recordStart = null;
    this.header = null;
    this.rows = [];
    this.issues = [];
    this.bomChecked = false;
    this.finished = false;
  }

  push(buffer, frame = null) {
    if (this.finished) throw new Error('CSV 组装已结束');
    if (!buffer || buffer.length === 0) return;
    this.rawOffset += buffer.length;
    // Mappings live in decoded-byte space, matching processedBytes. A UTF-8
    // character split across chunks is held back by StringDecoder, so the
    // decoded span can be shorter than the raw chunk.
    const text = this.decoder.write(buffer);
    const start = this.decodedBytes;
    this.decodedBytes += Buffer.byteLength(text);
    this.mappings.push({ start, end: this.decodedBytes, frame });
    this.pending += text;
    this.#process(false);
  }

  finish() {
    if (this.finished) return this.result();
    this.finished = true;
    const tail = this.decoder.end();
    if (tail.length > 0) {
      const start = this.decodedBytes;
      this.decodedBytes += Buffer.byteLength(tail);
      this.mappings.push({ start, end: this.decodedBytes, frame: this.mappings.at(-1)?.frame ?? null });
      this.pending += tail;
    }
    this.#process(true);
    if (this.inQuotes) {
      this.#issue('unterminated-quoted-field', 'CSV 引号字段在数据结束时仍未闭合', this.processedBytes, true);
    } else if (this.recordStart !== null) {
      this.#endRecord();
    }
    return this.result();
  }

  result() {
    return {
      header: this.header,
      rows: this.rows,
      issues: this.issues,
      bytes: this.processedBytes,
      rawBytes: this.rawOffset,
      mappings: this.mappings,
    };
  }

  #process(final) {
    const chars = Array.from(this.pending);
    let index = 0;
    while (index < chars.length) {
      const char = chars[index];

      if (!this.bomChecked && this.processedBytes === 0) {
        this.bomChecked = true;
        if (char === '﻿') {
          index += 1;
          this.processedBytes += 3;
          continue;
        }
      }

      // Blank lines carry no record. A CRLF terminator may be split across
      // chunks, in which case processing waits for the next chunk.
      if (this.recordStart === null && !this.inQuotes && !this.afterQuote) {
        if (char === '\r') {
          if (index + 1 >= chars.length && !final) break;
          if (chars[index + 1] === '\n') {
            index += 2;
            this.processedBytes += 2;
          } else {
            index += 1;
            this.processedBytes += 1;
          }
          continue;
        }
        if (char === '\n') {
          index += 1;
          this.processedBytes += 1;
          continue;
        }
        this.recordStart = this.processedBytes;
      }

      if (this.inQuotes) {
        if (char === '"') {
          if (index + 1 >= chars.length && !final) break;
          if (chars[index + 1] === '"') {
            this.field += '"';
            index += 2;
            this.processedBytes += 2;
          } else {
            this.inQuotes = false;
            this.afterQuote = true;
            this.atFieldStart = false;
            index += 1;
            this.processedBytes += 1;
          }
          continue;
        }
        this.field += char;
        index += 1;
        this.processedBytes += Buffer.byteLength(char);
        continue;
      }

      if (char === '\r') {
        if (index + 1 >= chars.length && !final) break;
        if (chars[index + 1] === '\n') {
          index += 2;
          this.processedBytes += 2;
        } else {
          index += 1;
          this.processedBytes += 1;
        }
        this.#endRecord();
        continue;
      }
      if (char === '\n') {
        index += 1;
        this.processedBytes += 1;
        this.#endRecord();
        continue;
      }

      if (this.afterQuote) {
        if (char === ',') {
          this.#endField();
          this.afterQuote = false;
          this.atFieldStart = true;
          index += 1;
          this.processedBytes += 1;
          continue;
        }
        this.#issue('characters-after-closing-quote', '引号字段闭合后出现额外字符', this.processedBytes, true);
        this.afterQuote = false;
      }

      if (char === ',') {
        this.#endField();
        this.atFieldStart = true;
        index += 1;
        this.processedBytes += 1;
        continue;
      }
      if (char === '"') {
        if (this.atFieldStart && this.field === '') {
          this.inQuotes = true;
          this.atFieldStart = false;
          index += 1;
          this.processedBytes += 1;
          continue;
        }
        this.#issue('unexpected-quote', '未加引号的字段中出现引号', this.processedBytes, false);
      }
      this.field += char;
      this.atFieldStart = false;
      index += 1;
      this.processedBytes += Buffer.byteLength(char);
    }
    this.pending = chars.slice(index).join('');
  }

  #endField() {
    this.fields.push(this.field);
    this.field = '';
  }

  #endRecord() {
    this.#endField();
    const start = this.recordStart ?? this.processedBytes;
    const end = this.processedBytes;
    const mapping = this.#framesFor(start, end);
    if (this.header === null) {
      const values = this.fields;
      this.header = { values, bytes: [start, end], frames: mapping.range, frameList: mapping.list };
      if (values.length === 0 || (values.length === 1 && values[0] === '')) {
        this.#issue('empty-header', 'CSV 首行没有列名', start, true);
      }
      const seen = new Set();
      for (const name of values) {
        if (seen.has(name)) this.#issue('duplicate-column', `CSV 列名重复：${name}`, start, true, { column: name });
        seen.add(name);
      }
    } else {
      const values = this.fields;
      this.rows.push({ values, bytes: [start, end], frames: mapping.range, frameList: mapping.list });
      if (values.length !== this.header.values.length) {
        this.issues.push({
          code: 'column-count-mismatch',
          message: `CSV 记录有 ${values.length} 列，首行有 ${this.header.values.length} 列`,
          frame: mapping.range[0] ?? null,
          offset: start,
          severity: 'warning',
          fatal: false,
        });
      }
    }
    this.fields = [];
    this.field = '';
    this.recordStart = null;
    this.afterQuote = false;
    this.atFieldStart = true;
  }

  #framesFor(start, end) {
    let first = null;
    let last = null;
    const list = [];
    for (const mapping of this.mappings) {
      if (mapping.end <= start || mapping.start >= end) continue;
      if (first === null) first = mapping.frame;
      last = mapping.frame;
      if (mapping.frame !== null && !list.includes(mapping.frame)) list.push(mapping.frame);
    }
    return { range: first === null ? [] : [first, last], list };
  }

  #issue(code, message, offset, fatal, extra = {}) {
    const mapping = this.mappings.find(item => offset >= item.start && offset < item.end)
      ?? this.mappings.at(-1);
    this.issues.push({
      code,
      message,
      frame: mapping?.frame ?? null,
      offset,
      severity: fatal ? 'error' : 'warning',
      fatal,
      ...extra,
    });
  }
}
