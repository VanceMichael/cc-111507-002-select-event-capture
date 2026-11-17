import { mkdir, readdir, readFile, writeFile, rename, rm } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { finished } from 'node:stream/promises';
import { join } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { once } from 'node:events';

export class ReceiptStore {
  constructor(root = './var') { this.root = root; }

  async open() {
    await mkdir(join(this.root, 'blobs'), { recursive: true });
    await mkdir(join(this.root, 'receipts'), { recursive: true });
  }

  async blob(bytes) {
    const digest = createHash('sha256').update(bytes).digest('hex');
    await writeFile(join(this.root, 'blobs', digest), bytes);
    return { digest, bytes: bytes.length };
  }

  /**
   * Write a blob incrementally so a streamed source is never buffered whole in
   * memory. The digest names the file only after a successful close.
   */
  beginBlob() {
    const temp = join(this.root, 'blobs', `${randomUUID()}.tmp`);
    const hash = createHash('sha256');
    const stream = createWriteStream(temp);
    let bytes = 0;
    let closed = false;
    return {
      write: async (chunk) => {
        if (closed) throw new Error('来源写入已结束');
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        hash.update(buffer);
        bytes += buffer.length;
        if (!stream.write(buffer)) await once(stream, 'drain');
      },
      finish: async () => {
        if (closed) throw new Error('来源写入已结束');
        closed = true;
        stream.end();
        await finished(stream);
        const digest = hash.digest('hex');
        await rename(temp, join(this.root, 'blobs', digest));
        return { digest, bytes };
      },
      abort: async () => {
        if (closed) return;
        closed = true;
        stream.destroy();
        await finished(stream).catch(() => {});
        await rm(temp, { force: true });
      },
    };
  }

  async save(receipt) {
    const id = receipt.id || randomUUID();
    if (!/^[a-zA-Z0-9-]+$/.test(id)) throw new Error('收件标识无效');
    const result = { ...receipt, id };
    const temp = join(this.root, 'receipts', `${id}.${randomUUID()}.tmp`);
    await writeFile(temp, JSON.stringify(result, null, 2));
    await rename(temp, join(this.root, 'receipts', `${id}.json`));
    return result;
  }

  async get(id) {
    if (!/^[a-zA-Z0-9-]+$/.test(id)) return null;
    try { return JSON.parse(await readFile(join(this.root, 'receipts', `${id}.json`), 'utf8')); }
    catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  }

  async list() {
    const names = (await readdir(join(this.root, 'receipts'))).filter(name => name.endsWith('.json')).sort();
    return Promise.all(names.map(name => this.get(name.slice(0, -5))));
  }
}
