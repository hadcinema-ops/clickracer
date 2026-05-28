const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const queue = [];
const matches = {};

function genId() { return crypto.randomBytes(3).toString('hex').toUpperCase(); }
function dequeue(socketId) {
  const i = queue.findIndex(q => q.socketId === socketId);
  if (i !== -1) queue.splice(i, 1);
}

io.on('connection', (socket) => {

  socket.on('find_match', ({ name }) => {
    socket.data.name = name || 'Player';
    socket.data.roomId = null;
    dequeue(socket.id);

    if (queue.length > 0) {
      const opponent = queue.shift();
      const oppSocket = io.sockets.sockets.get(opponent.socketId);
      if (!oppSocket) {
        queue.push({ socketId: socket.id, name: socket.data.name });
        socket.emit('queued', { position: queue.length });
        return;
      }
      const roomId = genId();
      matches[roomId] = {
        players: {
          [socket.id]:        { id: socket.id,        name: socket.data.name, times: [], clickedThisRound: false },
          [opponent.socketId]:{ id: opponent.socketId, name: opponent.name,   times: [], clickedThisRound: false },
        },
        round: 0, totalRounds: 5,
        phase: 'waiting', goTimeout: null,
      };
      socket.data.roomId = roomId;
      oppSocket.data.roomId = roomId;
      socket.join(roomId);
      oppSocket.join(roomId);
      const players = Object.values(matches[roomId].players);
      io.to(roomId).emit('match_found', { roomId, players });
      setTimeout(() => startRound(roomId), 2200);
    } else {
      queue.push({ socketId: socket.id, name: socket.data.name });
      socket.emit('queued', { position: queue.length });
    }
  });

  socket.on('cancel_search', () => { dequeue(socket.id); socket.emit('search_cancelled'); });

  socket.on('clicked', ({ reactionMs }) => {
    const roomId = socket.data.roomId;
    if (!roomId || !matches[roomId]) return;
    const match = matches[roomId];
    const player = match.players[socket.id];
    if (!player || player.clickedThisRound) return;

    if (match.phase !== 'go') {
      // Clicked too early — instant round loss (9999 = DNF penalty)
      player.clickedThisRound = true;
      player.times.push(9999);
      socket.emit('too_early');
      socket.to(roomId).emit('opponent_clicked', { time: 9999 });
      // If other player already clicked, end round
      if (Object.values(match.players).every(p => p.clickedThisRound)) {
        clearTimeout(match.goTimeout);
        endRound(roomId);
      }
      // Otherwise wait for them — but end soon
      else {
        match.earlyPenaltyTimeout = setTimeout(() => {
          if (!matches[roomId]) return;
          Object.values(match.players).forEach(p => {
            if (!p.clickedThisRound) { p.times.push(9999); p.clickedThisRound = true; }
          });
          clearTimeout(match.goTimeout);
          endRound(roomId);
        }, 4000);
      }
      return;
    }

    // Normal click during GO phase
    player.clickedThisRound = true;
    player.times.push(reactionMs);
    socket.to(roomId).emit('opponent_clicked', { time: reactionMs });

    if (Object.values(match.players).every(p => p.clickedThisRound)) {
      clearTimeout(match.goTimeout);
      endRound(roomId);
    }
  });

  // WebRTC signaling
  socket.on('webrtc_offer',  ({ offer })     => { const r=socket.data.roomId; if(r) socket.to(r).emit('webrtc_offer',  { offer }); });
  socket.on('webrtc_answer', ({ answer })    => { const r=socket.data.roomId; if(r) socket.to(r).emit('webrtc_answer', { answer }); });
  socket.on('webrtc_ice',    ({ candidate }) => { const r=socket.data.roomId; if(r) socket.to(r).emit('webrtc_ice',    { candidate }); });

  socket.on('play_again', () => {
    const roomId = socket.data.roomId;
    if (roomId && matches[roomId]) {
      clearTimeout(matches[roomId].goTimeout);
      socket.leave(roomId);
      delete matches[roomId].players[socket.id];
      if (Object.keys(matches[roomId].players).length === 0) delete matches[roomId];
      else socket.to(roomId).emit('opponent_left');
    }
    socket.data.roomId = null;
  });

  socket.on('disconnect', () => {
    dequeue(socket.id);
    const roomId = socket.data.roomId;
    if (!roomId || !matches[roomId]) return;
    clearTimeout(matches[roomId].goTimeout);
    socket.to(roomId).emit('opponent_left');
    delete matches[roomId];
  });
});

function startRound(roomId) {
  const match = matches[roomId];
  if (!match) return;
  match.round++;
  match.phase = 'wait';
  Object.values(match.players).forEach(p => { p.clickedThisRound = false; });
  io.to(roomId).emit('round_start', { round: match.round, total: match.totalRounds });
  const delay = 2000 + Math.random() * 4000;
  match.goTimeout = setTimeout(() => {
    if (!matches[roomId]) return;
    match.phase = 'go';
    io.to(roomId).emit('go', { serverTime: Date.now() });
    // Auto-end if nobody clicks after 5s
    match.goTimeout = setTimeout(() => {
      if (!matches[roomId]) return;
      Object.values(match.players).forEach(p => {
        if (!p.clickedThisRound) { p.times.push(9999); p.clickedThisRound = true; }
      });
      endRound(roomId);
    }, 5000);
  }, delay);
}

function endRound(roomId) {
  const match = matches[roomId];
  if (!match) return;
  match.phase = 'roundover';
  clearTimeout(match.goTimeout);
  const results = Object.values(match.players).map(p => ({
    id: p.id, name: p.name,
    time: p.times[p.times.length - 1],
    allTimes: [...p.times],
  }));
  io.to(roomId).emit('round_result', { round: match.round, total: match.totalRounds, results });
  if (match.round >= match.totalRounds) setTimeout(() => endMatch(roomId), 2800);
  else setTimeout(() => startRound(roomId), 3200);
}

function endMatch(roomId) {
  const match = matches[roomId];
  if (!match) return;
  const summary = Object.values(match.players).map(p => {
    const valid = p.times.filter(t => t < 9999);
    const avg = valid.length ? Math.round(valid.reduce((a,b)=>a+b,0)/valid.length) : 9999;
    const best = valid.length ? Math.min(...valid) : 9999;
    return { id: p.id, name: p.name, times: p.times, avg, best };
  });
  summary.sort((a,b) => a.avg - b.avg);
  const winnerId = summary[0].avg < summary[1].avg ? summary[0].id
                 : summary[1].avg < summary[0].avg ? summary[1].id : 'tie';
  io.to(roomId).emit('match_over', { summary, winnerId });
  delete matches[roomId];
}

setInterval(() => io.emit('queue_size', { count: queue.length }), 3000);
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`ReactionRacer on http://localhost:${PORT}`));
