const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const crypto = require('crypto');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

app.use(express.static(path.join(__dirname, 'public')));

// ─── 常量 ───
const STARTING_BALANCE = 1000;
const STATE_FILE = path.join(__dirname, 'state.json');

// ─── 持久化 ───
function saveState() {
  const data = {
    players: [],
    potTotal,
    potHistory,
    savedAt: new Date().toISOString(),
  };
  for (const p of playerRegistry.values()) {
    data.players.push({
      id: p.id, name: p.name, avatarSeed: p.avatarSeed,
      balance: p.balance, totalWinLoss: p.totalWinLoss, potBet: p.potBet, folded: p.folded,
    });
  }
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(data, null, 2));
  } catch (e) {
    console.error('saveState error:', e.message);
  }
}

function loadState() {
  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf-8');
    if (!raw) return;
    const data = JSON.parse(raw);
    potTotal = data.potTotal || 0;
    potHistory = data.potHistory || [];
    for (const ps of (data.players || [])) {
      playerRegistry.set(ps.id, {
        id: ps.id,
        name: ps.name,
        avatarSeed: ps.avatarSeed || ps.id,
        balance: ps.balance,
        totalWinLoss: ps.totalWinLoss || 0,
        potBet: ps.potBet || 0,
        folded: ps.folded || false,
        ws: null,
        online: false,
      });
    }
    console.log(`Loaded state: ${data.players?.length || 0} players, pot=${potTotal}, history=${potHistory.length}`);
  } catch (e) {
    console.log('No saved state, starting fresh.');
  }
}

// ─── 玩家注册表 ───
const playerRegistry = new Map();

function makePlayer(name, ws, existingId = null) {
  const id = existingId || crypto.randomUUID().slice(0, 8);
  const existing = playerRegistry.get(id);
  if (existing) {
    existing.ws = ws;
    existing.online = true;
    return existing;
  }
  const p = {
    id,
    name: name || '玩家' + id.slice(0, 4),
    avatarSeed: id,
    balance: STARTING_BALANCE,
    totalWinLoss: 0,
    potBet: 0,           // 当前轮投入底池的筹码
    folded: false,       // 本轮是否已弃牌
    ws,
    online: true,
  };
  playerRegistry.set(id, p);
  return p;
}

function playerPublic(p) {
  return {
    id: p.id,
    name: p.name,
    avatarSeed: p.avatarSeed,
    balance: p.balance,
    totalWinLoss: p.totalWinLoss,
    potBet: p.potBet,
    folded: p.folded || false,
    online: p.online,
  };
}

// ─── 底池状态 ───
let potTotal = 0;       // 底池总筹码
let potHistory = [];    // 最近几轮收池记录

function maxPotBet() {
  let m = 0;
  for (const p of playerRegistry.values()) {
    if (p.online && !p.folded && p.potBet > m) m = p.potBet;
  }
  return m;
}

// ─── 启动时加载存档 ───
loadState();

function potState() {
  return { potTotal, history: potHistory.slice(0, 5) };
}

function roomState() {
  const allPlayers = [];
  for (const p of playerRegistry.values()) {
    // 在线玩家全部展示；离线玩家如果本轮有下注也展示（标注离线）
    if (p.online || p.potBet > 0) {
      allPlayers.push(playerPublic(p));
    }
  }
  return {
    type: 'state',
    potTotal,
    potHistory: potHistory.slice(0, 5),
    players: allPlayers,
  };
}

function broadcast(msg, exceptWs = null) {
  const data = JSON.stringify(msg);
  for (const p of playerRegistry.values()) {
    if (p.online && p.ws && p.ws !== exceptWs && p.ws.readyState === 1) {
      p.ws.send(data);
    }
  }
}

function sendTo(ws, msg) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
}

// ─── WebSocket ───
wss.on('connection', (ws) => {
  let player = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'join': {
        const name = (msg.name || '').toString().trim().slice(0, 12);
        const existingId = msg.playerId || null;
        player = makePlayer(name || undefined, ws, existingId);
        if (name) player.name = name;

        sendTo(ws, {
          type: 'joined',
          playerId: player.id,
          name: player.name,
          avatarSeed: player.avatarSeed,
          balance: player.balance,
          totalWinLoss: player.totalWinLoss,
          potBet: player.potBet,
          potTotal,
          potHistory: potHistory.slice(0, 5),
        });
        sendTo(ws, roomState());
        broadcast({ type: 'playerJoined', player: playerPublic(player) }, ws);
        console.log(`[${new Date().toISOString()}] ${player.name} (${player.id}) joined. Online: ${[...playerRegistry.values()].filter(p => p.online).length}`);
        break;
      }

      case 'rename': {
        if (!player) return;
        const newName = (msg.name || '').toString().trim().slice(0, 12);
        if (!newName) return;
        const oldName = player.name;
        player.name = newName;
        const renameMsg = { type: 'renamed', playerId: player.id, oldName, newName };
        sendTo(ws, renameMsg);
        broadcast(renameMsg, ws);
        broadcast(roomState(), ws);
        saveState();
        break;
      }

      // ── 下注：把筹码投入底池 ──
      case 'bet': {
        if (!player) return;
        if (player.folded) { sendTo(ws, { type: 'error', message: '已弃牌，不能下注' }); return; }
        const amount = Math.floor(msg.amount || 0);
        if (amount <= 0) return;
        if (player.balance < amount) {
          sendTo(ws, { type: 'error', message: '余额不足！' });
          return;
        }
        player.balance -= amount;
        player.potBet += amount;
        potTotal += amount;

        const betMsg = {
          type: 'bet',
          playerId: player.id,
          name: player.name,
          amount,
          balance: player.balance,
          potBet: player.potBet,
          potTotal,
        };
        sendTo(ws, betMsg);
        broadcast(betMsg, ws);
        broadcast(roomState(), ws);
        saveState();
        break;
      }

      // ── 收回自己的下注（只拿回自己投入的筹码） ──
      case 'collect': {
        if (!player) return;
        if (player.potBet === 0) {
          sendTo(ws, { type: 'error', message: '没有可收回的筹码' });
          return;
        }
        const refund = player.potBet;
        player.balance += refund;
        potTotal -= refund;
        player.potBet = 0;

        const collectMsg = {
          type: 'collect',
          playerId: player.id,
          name: player.name,
          amount: refund,
          balance: player.balance,
          potBet: 0,
          potTotal,
        };
        sendTo(ws, collectMsg);
        broadcast(collectMsg, ws);
        broadcast(roomState(), ws);
        saveState();
        break;
      }

      // ── 跟注：补到与当前最高下注持平 ──
      case 'call': {
        if (!player) return;
        if (player.folded) { sendTo(ws, { type: 'error', message: '已弃牌，不能跟注' }); return; }
        const target = maxPotBet();
        const need = target - player.potBet;
        if (need <= 0) {
          sendTo(ws, { type: 'error', message: '你已是最高下注，无需跟注' });
          return;
        }
        const actual = Math.min(need, player.balance);  // 余额不足则all-in跟注
        if (actual <= 0) { sendTo(ws, { type: 'error', message: '余额不足' }); return; }
        player.balance -= actual;
        player.potBet += actual;
        potTotal += actual;

        const callMsg = {
          type: 'bet',  // 复用bet消息让前端统一处理
          playerId: player.id,
          name: player.name,
          amount: actual,
          balance: player.balance,
          potBet: player.potBet,
          potTotal,
          isCall: true,
        };
        sendTo(ws, callMsg);
        broadcast(callMsg, ws);
        broadcast(roomState(), ws);
        saveState();
        break;
      }

      // ── 弃牌：退出本轮，投入的筹码留在池里 ──
      case 'fold': {
        if (!player) return;
        if (player.folded) { sendTo(ws, { type: 'error', message: '已经弃牌了' }); return; }
        player.folded = true;

        const foldMsg = {
          type: 'fold',
          playerId: player.id,
          name: player.name,
          potBet: player.potBet,
        };
        sendTo(ws, foldMsg);
        broadcast(foldMsg, ws);
        broadcast(roomState(), ws);
        saveState();
        console.log(`[${new Date().toISOString()}] ${player.name} 弃牌`);
        break;
      }

      // ── 收池：赢家庄收整个底池 ──
      case 'winPot': {
        if (!player) return;
        if (player.folded) { sendTo(ws, { type: 'error', message: '已弃牌，不能收池' }); return; }
        if (potTotal === 0) {
          sendTo(ws, { type: 'error', message: '底池是空的' });
          return;
        }

        const wonAmount = potTotal;
        const myContribution = player.potBet;
        const net = wonAmount - myContribution;

        // 记录每个玩家的本轮净输赢
        const results = {};
        for (const p of playerRegistry.values()) {
          if (!p.online) continue;
          const contribution = p.potBet;
          const pNet = (p.id === player.id) ? net : -contribution;
          p.totalWinLoss += pNet;
          results[p.id] = {
            name: p.name,
            contribution,
            net: pNet,
            balance: p.id === player.id ? (p.balance + wonAmount) : p.balance,
            totalWinLoss: p.totalWinLoss,
          };
        }

        // 赢家拿走底池
        player.balance += wonAmount;

        // 记录历史
        potHistory.unshift({
          winner: player.name,
          winnerId: player.id,
          amount: wonAmount,
          results,
        });
        if (potHistory.length > 20) potHistory.pop();

        // 重置底池
        potTotal = 0;
        for (const p of playerRegistry.values()) {
          if (p.online) { p.potBet = 0; p.folded = false; }
        }

        const winMsg = {
          type: 'winPot',
          winnerId: player.id,
          winnerName: player.name,
          amount: wonAmount,
          results,
        };
        broadcast(winMsg);
        broadcast(roomState());
        saveState();  // 结算落盘
        console.log(`[${new Date().toISOString()}] ${player.name} 收池 ${wonAmount}`);
        break;
      }

      // ── 庄家补筹码 ──
      case 'addChips': {
        if (!player) return;
        const amount = Math.min(Math.floor(msg.amount || 0), 10000);
        if (amount <= 0) return;
        player.balance += amount;
        const addMsg = { type: 'chipsAdded', playerId: player.id, name: player.name, amount, balance: player.balance };
        sendTo(ws, addMsg);
        broadcast(addMsg, ws);
        broadcast(roomState(), ws);
        saveState();
        break;
      }
    }
  });

  ws.on('close', () => {
    if (player) {
      player.online = false;
      player.ws = null;
      broadcast({ type: 'playerLeft', playerId: player.id, name: player.name });
      broadcast(roomState());
      console.log(`[${new Date().toISOString()}] ${player.name} (${player.id}) left. Online: ${[...playerRegistry.values()].filter(p => p.online).length}`);
    }
  });

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
});

const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => clearInterval(interval));

const PORT = process.env.PORT || 3002;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`chips-betting server running on 0.0.0.0:${PORT}`);
});
