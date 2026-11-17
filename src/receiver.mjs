import { receiveBytes, streamBytes } from './http-client.mjs';
import { inspect } from './source-metadata.mjs';
import { EventInterpreter } from './event-interpreter.mjs';

function sourceUrl(sourceBase, entry) {
  return `${sourceBase}/files/${encodeURIComponent(entry)}`;
}

export async function collect({ input, descriptor, sourceBase, store }) {
  if (input.mode === 'raw') return collectRaw({ descriptor, sourceBase, store });
  if (input.mode === 'structured') return collectStructured({ input, descriptor, sourceBase, store });
  return { httpStatus: 501, error: '当前仅开放原始资料与结构化查询接收' };
}

async function collectRaw({ descriptor, sourceBase, store }) {
  const source = await receiveBytes(sourceUrl(sourceBase, descriptor.entry));
  const blob = await store.blob(source.bytes);
  const receipt = await store.save({
    state: 'received',
    mode: 'raw',
    product: descriptor.product,
    source: descriptor.entry,
    sourceVersion: source.etag,
    metadata: inspect(source.bytes),
    blobs: [blob],
    records: [],
    issues: [],
    coverage: { structured: false, downloadedBytes: source.bytes.length },
  });
  return { receipt };
}

/**
 * Structured query intake.
 *
 * The source object is streamed: event frames are decoded and CSV records are
 * assembled while bytes arrive, and the original source bytes are written
 * straight to a blob. The whole object is never downloaded first and filtered
 * afterwards; only the projected waybill fields are exposed as records.
 */
async function collectStructured({ input, descriptor, sourceBase, store }) {
  const interpreter = new EventInterpreter({
    source: descriptor.entry,
    sourceVersion: descriptor.version,
  });
  const sink = store.beginBlob();
  let sourceInfo = null;
  let transportError = null;
  try {
    sourceInfo = await streamBytes(sourceUrl(sourceBase, descriptor.entry), {
      onChunk: async (chunk) => {
        try {
          await sink.write(chunk);
        } catch (error) {
          // A local evidence write failure is not a source failure; label it
          // precisely so diagnostics do not blame the gateway.
          const wrapped = new Error(`来源证据写入失败：${error.message}`);
          wrapped.code = 'evidence-write-failed';
          wrapped.cause = error;
          throw wrapped;
        }
        interpreter.push(chunk);
      },
    });
  } catch (error) {
    // A transport/HTTP failure is never a completed query. Frames that did
    // arrive are still retained as partial evidence.
    transportError = error;
    interpreter.fail(error.code || 'source-transport-error', error.message, {
      status: error.status ?? null,
      cause: error.cause?.message ?? null,
    });
  }

  const result = interpreter.finish();
  let blob = null;
  try {
    blob = await sink.finish();
  } catch (error) {
    await sink.abort();
    result.issues.push({
      code: 'evidence-write-failed',
      message: `来源字节保存失败：${error.message}`,
      severity: 'warning',
      fatal: false,
      frame: null,
      offset: null,
    });
  }

  const receivedBytes = sourceInfo?.receivedBytes ?? interpreter.decoder.receivedBytes;
  const sourceVersion = sourceInfo?.etag ?? null;
  const coverage = {
    ...result.coverage,
    // Source-side processing statistics (sourceStats) stay separate from the
    // bytes this service actually read (receivedBytes).
    receivedBytes,
    downloadedBytes: receivedBytes,
    responseStatus: sourceInfo?.status ?? null,
    responseContentType: sourceInfo?.contentType ?? null,
    sourceVersion,
  };
  const receipt = await store.save({
    state: result.state,
    mode: 'structured',
    product: descriptor.product,
    source: descriptor.entry,
    sourceVersion,
    records: result.records,
    issues: result.issues,
    coverage,
    evidence: {
      source: descriptor.entry,
      sourceVersion,
      digest: sourceInfo?.digest ?? blob?.digest ?? null,
      receivedBytes,
      blobs: blob ? [blob] : [],
      complete: result.complete,
      transportError: transportError
        ? {
            code: transportError.code ?? null,
            message: transportError.message,
            status: transportError.status ?? null,
            receivedBytes: transportError.received ?? null,
          }
        : null,
    },
    request: input,
  });
  return { httpStatus: 201, receipt };
}
