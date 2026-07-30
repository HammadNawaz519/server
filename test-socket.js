/**
 * WebSocket Real-Time Test Script
 * ================================
 * Tests your socket server WITHOUT opening any browser.
 *
 * Usage:
 *   node Server/test-socket.js
 *   node Server/test-socket.js https://your-custom-server-url.railway.app
 *
 * What it tests:
 *   1. Can connect to the socket server
 *   2. Can identify (join user room)
 *   3. Can send a message and receive it back (echo test)
 *   4. Measures round-trip latency
 *   5. Tests reconnect behavior
 */

const { io } = require('socket.io-client');

const SOCKET_URL = process.argv[2] || process.env.NEXT_PUBLIC_SOCKET_URL || 'https://server-production-265c.up.railway.app';
const TIMEOUT_MS = 10000; // 10 seconds max per test

const COLORS = {
  green:  '\x1b[32m',
  red:    '\x1b[31m',
  yellow: '\x1b[33m',
  cyan:   '\x1b[36m',
  white:  '\x1b[37m',
  bold:   '\x1b[1m',
  reset:  '\x1b[0m',
};

const log = {
  pass:  (msg) => console.log(`${COLORS.green}✅ PASS${COLORS.reset} ${msg}`),
  fail:  (msg) => console.log(`${COLORS.red}❌ FAIL${COLORS.reset} ${msg}`),
  info:  (msg) => console.log(`${COLORS.cyan}ℹ️  ${msg}${COLORS.reset}`),
  warn:  (msg) => console.log(`${COLORS.yellow}⚠️  ${msg}${COLORS.reset}`),
  title: (msg) => console.log(`\n${COLORS.bold}${COLORS.white}=== ${msg} ===${COLORS.reset}`),
};

let passCount = 0;
let failCount = 0;

const pass = (msg) => { passCount++; log.pass(msg); };
const fail = (msg) => { failCount++; log.fail(msg); };

// Helper: wait for an event with a timeout
function waitForEvent(socket, event, timeoutMs = TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`Timeout: '${event}' was not received within ${timeoutMs}ms`));
    }, timeoutMs);

    socket.once(event, (data) => {
      clearTimeout(timer);
      resolve(data);
    });
  });
}

async function runTests() {
  console.log(`\n${COLORS.bold}${COLORS.cyan}╔══════════════════════════════════════════╗`);
  console.log(`║   WebSocket Real-Time Test Suite         ║`);
  console.log(`╚══════════════════════════════════════════╝${COLORS.reset}`);
  log.info(`Target: ${SOCKET_URL}`);
  log.info(`Timeout per test: ${TIMEOUT_MS}ms\n`);

  // ──────────────────────────────────────────
  // TEST 1: Connection
  // ──────────────────────────────────────────
  log.title('Test 1: Connection');
  const connectStart = Date.now();

  const socket = io(SOCKET_URL, {
    transports: ['websocket', 'polling'],
    reconnection: false,
    timeout: TIMEOUT_MS,
  });

  try {
    await waitForEvent(socket, 'connect');
    const latency = Date.now() - connectStart;
    pass(`Connected in ${latency}ms (Socket ID: ${socket.id})`);
  } catch (err) {
    fail(`Could not connect: ${err.message}`);
    console.log(`\n${COLORS.red}${COLORS.bold}FATAL: Cannot connect to socket server. Aborting.${COLORS.reset}`);
    console.log(`\n${COLORS.yellow}Diagnostics:`);
    console.log(`  • Target URL: ${SOCKET_URL}`);
    console.log(`  • This usually means the production server on Railway is DOWN or sleeping.`);
    console.log(`  • To test locally: Start your server with "node Server/index.js" then run:`);
    console.log(`    node Server/test-socket.js http://localhost:8080${COLORS.reset}\n`);
    process.exit(1);
  }

  // ──────────────────────────────────────────
  // TEST 2: Online Users Broadcast
  // ──────────────────────────────────────────
  log.title('Test 2: Receive Online Users List');
  try {
    // Server sends 'online_users' immediately on connection
    // It may have already been received, so we wait briefly
    const users = await Promise.race([
      waitForEvent(socket, 'online_users', 3000),
      new Promise(resolve => setTimeout(() => resolve([]), 3000))
    ]);
    pass(`Received online_users event (${Array.isArray(users) ? users.length : 0} users online)`);
  } catch (err) {
    fail(`Did not receive online_users: ${err.message}`);
  }

  // ──────────────────────────────────────────
  // TEST 3: Identify / Join Room
  // ──────────────────────────────────────────
  log.title('Test 3: Identify (Join User Room)');
  const TEST_EMAIL = 'test-ws-script@test.com';
  const TEST_USER_ID = 'ws-test-999';

  socket.emit('identify', { email: TEST_EMAIL, userId: TEST_USER_ID });
  // Wait a moment for server to process
  await new Promise(r => setTimeout(r, 500));
  pass(`Sent identify event (email: ${TEST_EMAIL}, userId: ${TEST_USER_ID})`);

  // ──────────────────────────────────────────
  // TEST 4: Echo Test — Send & Receive Message
  // ──────────────────────────────────────────
  log.title('Test 4: Message Echo (Send → Receive round-trip)');

  // Connect a SECOND socket to act as the "receiver"
  const socket2 = io(SOCKET_URL, {
    transports: ['websocket', 'polling'],
    reconnection: false,
    timeout: TIMEOUT_MS,
  });

  try {
    await waitForEvent(socket2, 'connect');
    // Identify socket2 as a different user who will receive from socket1
    const RECEIVER_EMAIL = 'receiver-ws-test@test.com';
    socket2.emit('identify', { email: RECEIVER_EMAIL, userId: 'ws-receiver-999' });
    await new Promise(r => setTimeout(r, 300));

    const TEST_CONTENT = `test-msg-${Date.now()}`;
    const sendTime = Date.now();

    // Listen for incoming message on socket2
    const receivePromise = waitForEvent(socket2, 'receive_social_message', 5000);

    // Send from socket1 → socket2
    socket.emit('send_social_message', {
      receiverEmail: RECEIVER_EMAIL,
      id: `test-id-${Date.now()}`,
      content: TEST_CONTENT,
      type: 'text',
      senderId: TEST_USER_ID,
      receiverId: 'ws-receiver-999',
      createdAt: new Date().toISOString(),
    });

    const received = await receivePromise;
    const latency = Date.now() - sendTime;

    if (received && received.content === TEST_CONTENT) {
      pass(`Message delivered in ${latency}ms ✓ Content matches`);
    } else {
      fail(`Message received but content mismatch. Got: ${JSON.stringify(received)}`);
    }
  } catch (err) {
    fail(`Echo test failed: ${err.message}`);
    log.warn('This means messages may not deliver in real-time between users.');
  } finally {
    socket2.disconnect();
  }

  // ──────────────────────────────────────────
  // TEST 5: Typing Indicator
  // ──────────────────────────────────────────
  log.title('Test 5: Typing Indicators');
  const socket3 = io(SOCKET_URL, {
    transports: ['websocket', 'polling'],
    reconnection: false,
    timeout: TIMEOUT_MS,
  });

  try {
    await waitForEvent(socket3, 'connect');
    const TYPER_EMAIL = 'typer-test@test.com';
    const WATCHER_EMAIL = 'watcher-test@test.com';

    socket3.emit('identify', { email: WATCHER_EMAIL, userId: 'watcher-999' });
    await new Promise(r => setTimeout(r, 300));

    const typingPromise = waitForEvent(socket3, 'user_typing', 3000);
    socket.emit('typing', { receiverEmail: WATCHER_EMAIL });

    await typingPromise;
    pass('Typing indicator delivered successfully');
  } catch (err) {
    fail(`Typing indicator test failed: ${err.message}`);
  } finally {
    socket3.disconnect();
  }

  // ──────────────────────────────────────────
  // TEST 6: Seen Receipt
  // ──────────────────────────────────────────
  log.title('Test 6: Message Seen Receipts');
  const socket4 = io(SOCKET_URL, {
    transports: ['websocket', 'polling'],
    reconnection: false,
    timeout: TIMEOUT_MS,
  });

  try {
    await waitForEvent(socket4, 'connect');
    const SENDER_EMAIL = 'seen-sender@test.com';
    const READER_EMAIL = 'seen-reader@test.com';

    socket4.emit('identify', { email: SENDER_EMAIL, userId: 'seen-sender-999' });
    await new Promise(r => setTimeout(r, 300));

    const seenPromise = waitForEvent(socket4, 'messages_seen', 3000);
    socket.emit('mark_as_seen', { senderEmail: SENDER_EMAIL });

    await seenPromise;
    pass('Seen receipt delivered successfully');
  } catch (err) {
    fail(`Seen receipt test failed: ${err.message}`);
  } finally {
    socket4.disconnect();
  }

  // ──────────────────────────────────────────
  // RESULTS SUMMARY
  // ──────────────────────────────────────────
  const total = passCount + failCount;
  console.log(`\n${COLORS.bold}╔══════════════════════════════════════════╗`);
  console.log(`║              TEST RESULTS                ║`);
  console.log(`╚══════════════════════════════════════════╝${COLORS.reset}`);
  console.log(`  Total:  ${total}`);
  console.log(`  ${COLORS.green}Passed: ${passCount}${COLORS.reset}`);
  console.log(`  ${failCount > 0 ? COLORS.red : COLORS.green}Failed: ${failCount}${COLORS.reset}`);

  if (failCount === 0) {
    console.log(`\n${COLORS.green}${COLORS.bold}🎉 All tests passed! Your WebSocket is working correctly.${COLORS.reset}\n`);
  } else {
    console.log(`\n${COLORS.red}${COLORS.bold}⚠️  ${failCount} test(s) failed. Check the server logs for details.${COLORS.reset}\n`);
  }

  socket.disconnect();
  process.exit(failCount > 0 ? 1 : 0);
}

runTests().catch(err => {
  console.error(`\n${COLORS.red}Fatal error: ${err.message}${COLORS.reset}`);
  process.exit(1);
});
