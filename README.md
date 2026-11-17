# 运单对象查询

该服务用于保留已获授权的来源材料及其收件证据。提供原始资料接收、字节摘要、来源元信息检查，以及事件流来源的结构化查询收件。来源服务和样例均在本机运行；资料中的单位、账号及标识都是合成值。

## 环境与运行

使用 Node.js 22.23.4。依赖通过 package-lock.json 锁定。准备环境时执行 `npm ci --ignore-scripts --no-audit --no-fund`，之后运行、构建、测试和演示均不下载资源。

```sh
npm run build
npm test
npm run demo
npm start
```

`npm start` 输出本次监听地址；服务绑定回环地址，来源网关与接收端在同一进程内使用独立端口。`DATA_DIR` 指定收件目录，默认 `./var`；`SCENARIO` 选择 fixtures/scenarios.json 中的来源场景。`npm run demo` 从临时空目录启动、实际领取来源字节、查询收件并退出，不回放旧日志。 `PORT` 可指定接收服务端口；未设置时仍自动选择空闲端口。

## 容器

```sh
docker build -t select-event-capture:local .
docker run --rm --network none select-event-capture:local npm test
docker run --rm --network none select-event-capture:local npm run demo
```

镜像构建准备阶段完成依赖安装，运行不需要公网服务。接口约定见 `docs/接入约定.md`，来源格式范围见 `docs/来源格式.md`。`src/source-metadata.mjs` 只识别文件元信息，不代表已提取结构化记录。文件收件与完整业务覆盖是不同状态。

## 结构化查询收件

`POST /v1/intakes` 使用 `{"mode":"structured"}` 触发运单对象查询。服务流式读取来源事件流，不先整份下载再筛选：

- 按大端长度切分事件帧，校验 prelude 与 message 两处 CRC32。
- 将跨帧的 Records payload 按 RFC4180 CSV 语义组装，保留引号与内嵌换行。
- 只投影授权字段 `id`、`note`，每条记录附来源帧区间 `frames`。
- 来源 Stats 统计记录在 `coverage.sourceStats`，本服务实际接收量记录在 `coverage.receivedBytes`，二者分列。

只有收到合法 `End` 且没有致命帧/CSV 问题时才写 `complete`。连接关闭、缺少 `End`、CRC 失败、帧截断或来源 error 都会得到 `partial` 或 `rejected`。下游通过 `GET /v1/receipts/{id}/records` 读取结果；该接口对未完成的查询返回 409，只发布已确认完成的清单。收件视图 `GET /v1/receipts/{id}` 对未完成的结构化收件也不返回 `records`（以 `published: false`、`recordsWithheld` 标明），本地文件仍保留记录用于审计。

## 结构化方式检查

`npm test` 默认执行结构化检查，使用 fixtures 中的事件流样本。`STRUCTURED=1 npm test` 与默认执行内容一致，保留该变量用于兼容旧脚本。领域字段约定见接入文档，不以测试代替支持范围说明。

