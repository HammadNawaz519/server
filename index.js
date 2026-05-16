require('dotenv').config();
const { createServer } = require('http');
const { Server } = require('socket.io');

const httpServer = createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Socket.io server is running');
});

const allowedOrigins = process.env.CLIENT_URL 
  ? process.env.CLIENT_URL.split(',') 
  : '*';

const io = new Server(httpServer, {
  cors: {
    origin: allowedOrigins,
    methods: ['GET', 'POST']
  }
});

// Track online users: email -> Set of socket IDs (multiple tabs)
const onlineUsers = new Map(); // email -> Set<socketId>
// Track which socket is in an active call
const activeCalls = new Set(); // socketIds currently in a call

// Helper: get all socket IDs for an email
function getRoomSockets(email) {
  return io.sockets.adapter.rooms.get(email) || new Set();
}

// Helper: broadcast updated online users list to everyone
function broadcastOnlineUsers() {
  const onlineList = Array.from(onlineUsers.keys());
  io.emit('online_users', onlineList);
}

io.on('connection', (socket) => {
  console.log('Socket connected:', socket.id);

  // ─── IDENTIFY ────────────────────────────────────────────────────────────────
  socket.on('identify', ({ email }) => {
    if (!email) return;
    const room = email.toLowerCase().trim();

    socket.join(room);
    socket.userEmail = room;

    // Track online presence
    if (!onlineUsers.has(room)) {
      onlineUsers.set(room, new Set());
    }
    onlineUsers.get(room).add(socket.id);
    broadcastOnlineUsers();

    console.log(`User identified: ${room} (${socket.id})`);
  });

  // ─── MESSAGING ───────────────────────────────────────────────────────────────
  socket.on('send_social_message', (data) => {
    const { receiverEmail, ...msgData } = data;
    const target = receiverEmail.toLowerCase().trim();
    socket.to(target).emit('receive_social_message', msgData);
    // Echo to sender's other tabs
    if (socket.userEmail) {
      socket.to(socket.userEmail).emit('receive_social_message', msgData);
    }
  });

  socket.on('delete_social_message', (data) => {
    const { receiverEmail, ...deleteData } = data;
    socket.to(receiverEmail.toLowerCase().trim()).emit('receive_social_delete', deleteData);
  });

  // ─── TYPING ──────────────────────────────────────────────────────────────────
  socket.on('typing', ({ receiverEmail }) => {
    socket.to(receiverEmail.toLowerCase().trim()).emit('user_typing', { email: socket.userEmail });
  });

  socket.on('stop_typing', ({ receiverEmail }) => {
    socket.to(receiverEmail.toLowerCase().trim()).emit('user_stop_typing', { email: socket.userEmail });
  });

  // ─── SEEN ────────────────────────────────────────────────────────────────────
  socket.on('mark_as_seen', ({ senderEmail }) => {
    if (senderEmail) {
      socket.to(senderEmail.toLowerCase().trim()).emit('messages_seen');
    }
  });

  // ─── CALL EVENTS ─────────────────────────────────────────────────────────────

  // Caller initiates a call
  socket.on('call_user', (data) => {
    const target = data.to.toLowerCase().trim();

    // Check if the target is already in an active call
    const targetSockets = getRoomSockets(target);
    const isTargetBusy = [...targetSockets].some(sid => activeCalls.has(sid));

    if (isTargetBusy) {
      // Notify caller that the target is busy
      socket.emit('call_busy', { email: target });
      console.log(`Call to ${target} rejected - user is busy`);
      return;
    }

    socket.to(target).emit('incoming_call', {
      from: data.from,
      type: data.type
    });
    console.log(`Call from ${socket.userEmail} to ${target} (${data.type})`);
  });

  // Receiver accepts the call
  socket.on('accept_call', (data) => {
    const target = data.to.toLowerCase().trim();
    // Mark both parties as in a call
    activeCalls.add(socket.id);
    const targetSockets = getRoomSockets(target);
    targetSockets.forEach(sid => activeCalls.add(sid));

    socket.to(target).emit('call_accepted', { from: data.from });
    console.log(`Call accepted between ${socket.userEmail} and ${target}`);
  });

  // Receiver rejects the call
  socket.on('reject_call', (data) => {
    const target = data.to.toLowerCase().trim();
    socket.to(target).emit('call_rejected', { by: socket.userEmail });
    console.log(`Call rejected by ${socket.userEmail}`);
  });

  // Either party ends the call
  socket.on('end_call', (data) => {
    const target = data.to.toLowerCase().trim();

    // Remove both from active calls
    activeCalls.delete(socket.id);
    const targetSockets = getRoomSockets(target);
    targetSockets.forEach(sid => activeCalls.delete(sid));

    socket.to(target).emit('call_ended');
    console.log(`Call ended by ${socket.userEmail} with ${target}`);
  });

  // WebRTC Signaling relay (SDP offers/answers + ICE candidates)
  socket.on('webrtc_signal', (data) => {
    const target = data.to.toLowerCase().trim();
    // Relay signal with sender info for multi-party edge cases
    socket.to(target).emit('webrtc_signal', {
      ...data.signal,
      from: socket.userEmail
    });
  });

  // ─── DISCONNECT ──────────────────────────────────────────────────────────────
  socket.on('disconnect', () => {
    console.log('Socket disconnected:', socket.id);

    // Remove from active calls
    activeCalls.delete(socket.id);

    // Remove from online tracking
    if (socket.userEmail) {
      const sockets = onlineUsers.get(socket.userEmail);
      if (sockets) {
        sockets.delete(socket.id);
        if (sockets.size === 0) {
          // No more tabs open — user is offline
          onlineUsers.delete(socket.userEmail);
          broadcastOnlineUsers();
        }
      }
    }
  });
});

const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => {
  console.log(`Socket.io server listening on port ${PORT}`);
});
