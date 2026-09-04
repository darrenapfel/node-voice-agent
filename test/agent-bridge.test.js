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

async function startBackend(t, upstreamPort, apiKey = 'not-a-real-deepgram-key') {
  const port = await getOpenPort();
  const child = spawn('node', ['server.js'], {
    cwd: repoRoot,
    env: {
      ...globalThis.process.env,
      DEEPGRAM_API_KEY: apiKey,
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
      if (output.includes(`http://localhost:${port}`)) {
        clearTimeout(timer);
        resolve();
      }
    });
  });

  t.after(async () => {
    if (child.exitCode === null) {
      child.kill('SIGTERM');
      await once(child, 'exit');
    }
  });
  return { port, getOutput: () => output };
}

async function connectBrowser(port) {
  const session = await (await fetch(`http://127.0.0.1:${port}/api/session`)).json();
  return new WebSocket(`ws://127.0.0.1:${port}/api/voice-agent`, `access_token.${session.token}`);
}

test('reports a pre-open upstream failure without logging the API key', async (t) => {
  const upstream = createServer();
  upstream.on('upgrade', (request, socket) => {
    socket.end('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  t.after(() => upstream.close());

  const apiKey = 'not-a-real-deepgram-key';
  const backend = await startBackend(t, upstream.address().port, apiKey);
  const browser = await connectBrowser(backend.port);
  const messages = [];
  browser.on('message', (message) => messages.push(JSON.parse(message)));
  const [code, reason] = await once(browser, 'close');

  assert.deepEqual(messages, [{
    type: 'Error',
    description: 'Deepgram connection failed to open',
    code: 'CONNECTION_FAILED',
  }]);
  assert.equal(code, 1011);
  assert.equal(reason.toString(), 'Deepgram connection failed to open');
  assert.equal(backend.getOutput().includes(apiKey), false);
});

test('preserves valid upstream close details after opening', async (t) => {
  const upstream = createServer();
  const upstreamWss = new WebSocketServer({ server: upstream });
  upstreamWss.on('connection', (socket) => socket.close(1011, 'upstream failure'));
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  t.after(() => {
    upstreamWss.close();
    upstream.close();
  });

  const backend = await startBackend(t, upstream.address().port);
  const browser = await connectBrowser(backend.port);
  const [code, reason] = await once(browser, 'close');

  assert.equal(code, 1011);
  assert.equal(reason.toString(), 'upstream failure');
});
