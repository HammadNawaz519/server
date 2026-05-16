const { createServer } = require('http');
const { Server } = require('socket.io');

const httpServer = createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Socket.io server is running');
});

const io = new Server(httpServer, {
  cors: {
    origin: '*', // Change to your Vercel URL in production e.g. 'https://your-app.vercel.app'
    methods: ['GET', 'POST']
  }
});

io.on('connection', (socket) => {
  console.log('Socket connected:', socket.id);

  socket.on('identify', ({ email }) => {
    if (!email) return;
    const room = email.toLowerCase().trim();
    socket.join(room);
    socket.userEmail = room;
    console.log(`User identified: ${room}`);
  });

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

  socket.on('typing', ({ receiverEmail }) => {
    socket.to(receiverEmail.toLowerCase().trim()).emit('user_typing', { email: socket.userEmail });
  });

  socket.on('stop_typing', ({ receiverEmail }) => {
    socket.to(receiverEmail.toLowerCase().trim()).emit('user_stop_typing', { email: socket.userEmail });
  });

  socket.on('mark_as_seen', ({ senderEmail }) => {
    if (senderEmail) {
      socket.to(senderEmail.toLowerCase().trim()).emit('messages_seen');
    }
  });

  // --- CALL EVENTS ---
  socket.on('call_user', (data) => {
    socket.to(data.to.toLowerCase().trim()).emit('incoming_call', { from: data.from, type: data.type });
  });

  socket.on('accept_call', (data) => {
    socket.to(data.to.toLowerCase().trim()).emit('call_accepted', { from: data.from });
  });

  socket.on('reject_call', (data) => {
    socket.to(data.to.toLowerCase().trim()).emit('call_rejected');
  });

  socket.on('end_call', (data) => {
    socket.to(data.to.toLowerCase().trim()).emit('call_ended');
  });

  socket.on('webrtc_signal', (data) => {
    socket.to(data.to.toLowerCase().trim()).emit('webrtc_signal', data.signal);
  });

  socket.on('disconnect', () => {
    console.log('Socket disconnected:', socket.id);
  });
});

const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => {
  console.log(`Socket.io server listening on port ${PORT}`);
});
