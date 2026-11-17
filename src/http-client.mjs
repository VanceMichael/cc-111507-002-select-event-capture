import { createHash } from 'node:crypto';

function codedError(code, message, extra = {}) {
  const error = new Error(message);
  error.code = code;
  Object.assign(error, extra);
  return error;
}

/**
 * Stream a source response without buffering the whole object when the caller
 * supplies onChunk. Used by structured queries so records can be interpreted
 * while bytes are still arriving.
 */
export async function streamBytes(url, { headers = {}, limit = 16777216, timeout = 15000, onChunk = null } = {}) {
  let response;
  try {
    response = await fetch(url, { headers, signal: AbortSignal.timeout(timeout), redirect: 'error' });
  } catch (error) {
    if (error.code === 'source-transport-error' || error.code === 'source-timeout') throw error;
    throw codedError('source-transport-error', `无法连接来源（${error.cause?.code ?? error.name}）`, { cause: error });
  }
  if (!response.ok) {
    throw codedError('source-http-error', `来源返回 ${response.status}`, { status: response.status });
  }

  const hash = createHash('sha256');
  const parts = [];
  let received = 0;
  try {
    for await (const chunk of response.body) {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      received += bytes.length;
      if (received > limit) {
        throw codedError('receive-limit-exceeded', `来源超出收件限额 ${limit} 字节`, { received });
      }
      hash.update(bytes);
      if (onChunk) await onChunk(bytes);
      else parts.push(bytes);
    }
  } catch (error) {
    if (error.code) throw error;
    if (error.name === 'AbortError') {
      throw codedError('source-timeout', '来源读取超时', { cause: error, received });
    }
    throw codedError('source-stream-interrupted', '来源连接中断', { cause: error, received });
  }

  return {
    bytes: onChunk ? null : Buffer.concat(parts),
    receivedBytes: received,
    digest: hash.digest('hex'),
    status: response.status,
    etag: response.headers.get('etag'),
    contentType: response.headers.get('content-type'),
    contentRange: response.headers.get('content-range'),
  };
}

export async function receiveBytes(url, options = {}) {
  const result = await streamBytes(url, options);
  return { ...result, bytes: result.bytes ?? Buffer.alloc(0) };
}
