import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';
import type { ExtWebSocket, WsMessage } from './types.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PUBLIC_DIR = path.resolve(__dirname, '../public');

const port = Number(process.env.PORT) || 8080;

// 1. Create HTTP server for health checking & static testbench UI
const server = http.createServer((req, res) => {
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ status: 'healthy', activeClients: wss.clients.size }));
  }

  // Serve testbench HTML
  const filePath = req.url === '/' ? path.join(PUBLIC_DIR, 'index.html') : path.join(PUBLIC_DIR, req.url || '');
  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath);
    const contentType = ext === '.html' ? 'text/html' : ext === '.svg' ? 'image/svg+xml' : 'text/plain';
    res.writeHead(200, { 'Content-Type': contentType });
    return fs.createReadStream(filePath).pipe(res);
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

// 2. Attach RFC 6455 WebSocket Server
export const wss = new WebSocketServer({ server });

// Heartbeat interval to drop ghost connections
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((client) => {
    const extWs = client as ExtWebSocket;
    if (!extWs.isAlive) {
      console.log(`[WebSocket] Terminating inactive client: ${extWs.clientId}`);
      return extWs.terminate();
    }
    extWs.isAlive = false;
    extWs.ping();
  });
}, 30000);

wss.on('close', () => {
  clearInterval(heartbeatInterval);
});

wss.on('connection', (ws: WebSocket, req) => {
  const extWs = ws as ExtWebSocket;
  extWs.isAlive = true;
  extWs.clientId = `client_${Math.random().toString(36).slice(2, 9)}`;
  extWs.room = 'general';

  console.log(`[WebSocket] New client connected: ${extWs.clientId} from ${req.socket.remoteAddress}`);

  extWs.on('pong', () => {
    extWs.isAlive = true;
  });

  // Welcome message
  const welcomeMsg: WsMessage = {
    type: 'system',
    sender: 'Server',
    room: extWs.room,
    payload: `Welcome ${extWs.clientId}! Connected to RFC 6455 Masterclass Server.`,
    timestamp: Date.now()
  };
  extWs.send(JSON.stringify(welcomeMsg));

  extWs.on('message', (data: Buffer | string) => {
    try {
      const msg: WsMessage = JSON.parse(data.toString());

      switch (msg.type) {
        case 'ping':
          extWs.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
          break;

        case 'join':
          if (msg.room) {
            const oldRoom = extWs.room;
            extWs.room = msg.room;
            extWs.send(JSON.stringify({
              type: 'system',
              payload: `Switched from room #${oldRoom} to #${extWs.room}`,
              timestamp: Date.now()
            }));
          }
          break;

        case 'chat':
          // Broadcast to everyone in the same room
          wss.clients.forEach((client) => {
            const recipient = client as ExtWebSocket;
            if (recipient.readyState === WebSocket.OPEN && recipient.room === extWs.room) {
              recipient.send(JSON.stringify({
                type: 'chat',
                sender: extWs.clientId,
                room: extWs.room,
                payload: msg.payload,
                timestamp: Date.now()
              }));
            }
          });
          break;

        case 'broadcast':
          // Global broadcast across all rooms
          wss.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
              client.send(JSON.stringify({
                type: 'broadcast',
                sender: extWs.clientId,
                payload: msg.payload,
                timestamp: Date.now()
              }));
            }
          });
          break;

        default:
          extWs.send(JSON.stringify({ type: 'system', payload: `Unknown action: ${msg.type}`, timestamp: Date.now() }));
      }
    } catch {
      extWs.send(JSON.stringify({ type: 'system', payload: 'Error: Message must be valid JSON', timestamp: Date.now() }));
    }
  });

  extWs.on('close', (code, reason) => {
    console.log(`[WebSocket] Client disconnected: ${extWs.clientId} (Code: ${code}, Reason: ${reason})`);
  });
});

if (process.argv[1] === __filename) {
  server.listen(port, () => {
    console.log(`[learn-websockets] Server running on http://localhost:${port}`);
    console.log(`[learn-websockets] WebSocket endpoint active at ws://localhost:${port}`);
  });
}
