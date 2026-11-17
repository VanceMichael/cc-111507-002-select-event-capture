import http from 'node:http';
import { fileURLToPath } from 'node:url';
import { collect } from './receiver.mjs';
import { ReceiptStore } from './store.mjs';
import { createGateway } from './gateway.mjs';

export async function createService({ dataDir, scenario = 'standard', port = 0 } = {}) {
  const store = new ReceiptStore(dataDir);
  await store.open();
  const gateway = await createGateway(scenario);

  // Downstream publication boundary: a structured query result may be read
  // only once it is confirmed complete. Partial/rejected receipts still keep
  // their records on disk for audit, but the API never hands out an
  // unconfirmed record list.
  const publish = (receipt) => {
    if (receipt.mode !== 'structured') return receipt;
    if (receipt.state === 'complete') return { ...receipt, recordsWithheld: 0, published: true };
    return { ...receipt, records: [], recordsWithheld: receipt.records.length, published: false };
  };

  const server = http.createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      response.setHeader('Content-Type', 'application/json; charset=utf-8');
      if (url.pathname === '/health') return response.end(JSON.stringify({ status: 'ok' }));
      if (url.pathname === '/v1/source') return response.end(JSON.stringify(gateway.descriptor));
      if (url.pathname === '/v1/receipts' && request.method === 'GET') {
        return response.end(JSON.stringify((await store.list()).map(publish)));
      }
      if (url.pathname.startsWith('/v1/receipts/') && request.method === 'GET') {
        const parts = url.pathname.split('/').filter(Boolean);
        const receipt = await store.get(parts[2]);
        if (!receipt) {
          response.statusCode = 404;
          return response.end(JSON.stringify({ error: '收件不存在' }));
        }
        // A query result is only published downstream once it is confirmed
        // complete. Partial or rejected receipts remain readable for
        // diagnostics, but never through the result endpoint.
        if (parts[3] === 'records') {
          if (receipt.mode === 'structured' && receipt.state !== 'complete') {
            response.statusCode = 409;
            return response.end(JSON.stringify({
              error: '查询结果尚未确认完成',
              state: receipt.state,
              issues: receipt.issues,
            }));
          }
          return response.end(JSON.stringify(receipt.records ?? []));
        }
        if (parts[3] !== undefined) {
          response.statusCode = 404;
          return response.end(JSON.stringify({ error: '接口不存在' }));
        }
        return response.end(JSON.stringify(publish(receipt)));
      }
      if (url.pathname === '/v1/intakes' && request.method === 'POST') {
        const pieces = [];
        let size = 0;
        for await (const piece of request) {
          size += piece.length;
          if (size > 65536) throw new Error('请求体超过限制');
          pieces.push(piece);
        }
        const input = JSON.parse(Buffer.concat(pieces).toString());
        if (!input || typeof input !== 'object' || Array.isArray(input)) {
          response.statusCode = 400;
          return response.end(JSON.stringify({ error: '请求体必须是对象' }));
        }
        if (!['raw', 'structured'].includes(input.mode)) {
          response.statusCode = 400;
          return response.end(JSON.stringify({ error: '接收方式无效' }));
        }
        const result = await collect({ input, descriptor: gateway.descriptor, sourceBase: gateway.baseUrl, store });
        response.statusCode = result.httpStatus || 201;
        return response.end(JSON.stringify(result.receipt ? publish(result.receipt) : result));
      }
      response.statusCode = 404;
      response.end(JSON.stringify({ error: '接口不存在' }));
    } catch (error) {
      response.statusCode = 400;
      response.end(JSON.stringify({ error: error.message }));
    }
  });
  await new Promise(resolve => server.listen(port, '127.0.0.1', resolve));
  return { server, store, gateway, baseUrl: `http://127.0.0.1:${server.address().port}`,
    close: async () => { await new Promise(resolve => server.close(resolve)); await gateway.close(); } };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const service = await createService({ dataDir: process.env.DATA_DIR || './var', scenario: process.env.SCENARIO || 'standard', port: Number(process.env.PORT || 0) });
  console.log(JSON.stringify({ message: '接收服务已启动', url: service.baseUrl, source: service.gateway.descriptor.product }));
  const stop = async () => { await service.close(); process.exit(0); };
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
}
