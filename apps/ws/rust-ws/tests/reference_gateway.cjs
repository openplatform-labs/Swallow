// Execute the pinned original gateway and online service with a real Socket.IO
// server and a disposable in-memory subscriber repository. This is not the
// full original WS service, Redis adapter, or MongoDB repository.
const fs = require('fs');
const http = require('http');
const Module = require('module');
const path = require('path');
const root = process.env.NOVU_REFERENCE_SOURCE;
const ts = require(path.join(root, 'node_modules/.pnpm/typescript@5.9.3/node_modules/typescript'));
const { Server } = require(path.join(root, 'node_modules/.pnpm/socket.io@4.8.3/node_modules/socket.io'));
const jwt = require(path.join(root, 'node_modules/.pnpm/jsonwebtoken@9.0.3/node_modules/jsonwebtoken'));
const logger = { debug() {}, warn() {}, log() {}, error() {} };
const decorator = () => () => {};
const mocks = {
  '@nestjs/common': { Logger: logger, Injectable: decorator },
  '@nestjs/websockets': { WebSocketGateway: decorator, WebSocketServer: decorator },
  '@novu/shared': { ObservabilityBackgroundTransactionEnum: {} },
  '@socket.io/admin-ui': { instrument() {} },
  newrelic: { startBackgroundTransaction(_name, _group, cb) { cb(); }, getTransaction() { return { end() {} }; } },
};
function original(file) {
  const code = ts.transpileModule(fs.readFileSync(file, 'utf8'), {compilerOptions: {module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, experimentalDecorators:true, emitDecoratorMetadata:false}}).outputText;
  const mod = new Module(file, module);
  mod.filename = file;
  mod.paths = Module._nodeModulePaths(path.dirname(file));
  mod.require = (name) => Object.hasOwn(mocks, name) ? mocks[name] : require(name);
  mod._compile(code, file);
  return mod.exports;
}
const { WSGateway } = original(path.join(root, 'apps/ws/src/socket/ws.gateway.ts'));
const { SubscriberOnlineService } = original(path.join(root, 'apps/ws/src/shared/subscriber-online/subscriber-online.service.ts'));
const state = { isOnline:false, lastOnlineAt:null };
const repo = { async update(filter, { $set }) { if (filter._id === '64a000000000000000000010' && filter._environmentId === '64a000000000000000000002') Object.assign(state, $set); } };
const gateway = new WSGateway({ verify: (token) => jwt.verify(token, process.env.JWT_SECRET) }, new SubscriberOnlineService(repo));
const httpServer = http.createServer((req,res) => {
  if (req.url === '/status') { res.writeHead(200,{'content-type':'application/json'}); res.end(JSON.stringify(state)); }
  else if (req.url === '/emit' && req.method === 'POST') {
    let body = ''; req.on('data', chunk => body += chunk); req.on('end', async () => {
      try { const item = JSON.parse(body); await gateway.sendMessage(item.userId,item.event,item.data,item.contextKeys); res.writeHead(204); res.end(); }
      catch (error) { res.writeHead(500); res.end(String(error)); }
    });
  }
  else { res.writeHead(404); res.end(); }
});
const io = new Server(httpServer);
gateway.server = io;
io.on('connection', socket => { gateway.handleConnection(socket).catch(() => socket.disconnect()); socket.on('disconnect', () => gateway.handleDisconnect(socket).catch(() => {})); });
httpServer.listen(Number(process.env.REF_WS_PORT), '127.0.0.1');
