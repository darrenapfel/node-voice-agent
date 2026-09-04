const assert = require('node:assert/strict');
const { once } = require('node:events');
const { createServer } = require('node:http');
const net = require('node:net');
const path = require('node:path');
const { spawn } = require('node:child_process');
const test = require('node:test');
const { WebSocketServer, WebSocket } = require('ws');

const repoRoot = path.resolve(__dirname, '..');

async function getOpenPort() {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function startBackend(t, upstreamPort) {
  const port = await getOpenPort();
  const child = spawn('node', ['server.js'], {
    cwd: repoRoot,
    env: {
      ...globalThis.process.env,
      DEEPGRAM_API_KEY: 'not-a-real-deepgram-key',
      DEEPGRAM_BASE_URL: `ws://127.0.0.1:${upstreamPort}`,
      PORT: String(port),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (chunk) => { output += chunk; });
  child.stderr.on('data', (chunk) => { output += chunk; });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('backend did not start')), 5000);
    child.stdout.on('data', () => {
      if (output.includes(`http://localhost:${port}`)) { clearTimeout(timer); resolve(); }
    });
  });
  t.after(async () => {
    if (child.exitCode === null) { child.kill('SIGTERM'); await once(child, 'exit'); }
  });
  return { port };
}

async function connectBrowser(port) {
  const session = await (await fetch(`http://127.0.0.1:${port}/api/session`)).json();
  const ws = new WebSocket(`ws://127.0.0.1:${port}/api/voice-agent`, `access_token.${session.token}`);
  ws.binaryType = 'arraybuffer';
  return ws;
}

test('delivers every agent audio frame before AgentAudioDone, each event once', async (t) => {
  const upstream = createServer();
  const upstreamWss = new WebSocketServer({ server: upstream });
  upstreamWss.on('connection', (socket) => {
    socket.send(JSON.stringify({ type: 'Welcome', request_id: 'test' }));
    socket.on('message', (data, isBinary) => {
      if (isBinary) return;
      const msg = JSON.parse(data.toString());
      if (msg.type !== 'Settings') return;
      socket.send(JSON.stringify({ type: 'SettingsApplied' }));
      // Emit the exact sequence that exposed the tail clip: N binary frames
      // followed synchronously by the terminal JSON event.
      for (let i = 0; i < 6; i++) socket.send(Buffer.alloc(3200, i + 1), { binary: true });
      socket.send(JSON.stringify({ type: 'AgentAudioDone' }));
    });
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  t.after(() => { upstreamWss.close(); upstream.close(); });

  const backend = await startBackend(t, upstream.address().port);
  const browser = await connectBrowser(backend.port);
  const received = [];
  browser.on('message', (data, isBinary) => {
    received.push(isBinary ? 'AUDIO' : JSON.parse(data.toString()).type);
  });
  await once(browser, 'open');
  browser.send(JSON.stringify({ type: 'Settings', audio: {}, agent: {} }));

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out; got ${received.join(',')}`)), 5000);
    const tick = () => (received.includes('AgentAudioDone') ? (clearTimeout(timer), resolve()) : setTimeout(tick, 10));
    tick();
  });
  browser.close();

  assert.deepEqual(received, [
    'Welcome', 'SettingsApplied',
    'AUDIO', 'AUDIO', 'AUDIO', 'AUDIO', 'AUDIO', 'AUDIO',
    'AgentAudioDone',
  ]);
});
