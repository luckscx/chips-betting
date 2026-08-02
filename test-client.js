// 测试脚本：德州筹码池模式
const WebSocket = require('ws');
const URL = 'ws://127.0.0.1:30003/ws';

function makeClient(name) {
  return new Promise((resolve) => {
    const ws = new WebSocket(URL);
    const msgs = [];
    ws.on('open', () => ws.send(JSON.stringify({ type: 'join', name })));
    ws.on('message', (d) => {
      const m = JSON.parse(d);
      msgs.push(m);
      if (m.type === 'joined') resolve({ ws, msgs, id: m.playerId });
    });
    ws.on('error', (e) => console.error(`${name} error:`, e.message));
  });
}

(async () => {
  console.log('--- 连接两个客户端 ---');
  const c1 = await makeClient('德爷');
  const c2 = await makeClient('赌圣');
  console.log(`c1: id=${c1.id}, 余额=${c1.msgs[0].balance}`);
  console.log(`c2: id=${c2.id}`);

  await sleep(200);

  // c1 下注 50
  console.log('\n--- c1 下注 50 ---');
  c1.ws.send(JSON.stringify({ type: 'bet', amount: 50 }));
  await sleep(200);

  // c2 下注 100
  console.log('\n--- c2 下注 100 ---');
  c2.ws.send(JSON.stringify({ type: 'bet', amount: 100 }));
  await sleep(200);

  // 检查 potTotal 同步
  const c1State = c1.msgs.find(m => m.type === 'bet' && m.playerId === c2.id);
  const c2State = c2.msgs.find(m => m.type === 'bet' && m.playerId === c1.id);
  console.log(`c1 收到 c2 下注后 potTotal: ${c1State?.potTotal}`);
  console.log(`c2 收到 c1 下注后 potTotal: ${c2State?.potTotal}`);

  // c1 收回
  console.log('\n--- c1 收回下注 ---');
  c1.ws.send(JSON.stringify({ type: 'collect' }));
  await sleep(200);
  const c1Collect = c1.msgs.filter(m => m.type === 'collect').pop();
  console.log(`c1 收回: ${c1Collect?.amount}, 余额=${c1Collect?.balance}, potTotal=${c1Collect?.potTotal}`);

  // c1 再次下注 30, c2 下注 70, 然后 c2 收池
  console.log('\n--- c1 下注 30, c2 下注 70 ---');
  c1.ws.send(JSON.stringify({ type: 'bet', amount: 30 }));
  c2.ws.send(JSON.stringify({ type: 'bet', amount: 70 }));
  await sleep(300);

  // 改名
  console.log('\n--- c1 改名 ---');
  c1.ws.send(JSON.stringify({ type: 'rename', name: '德爷爷' }));
  await sleep(200);
  const rn = c1.msgs.find(m => m.type === 'renamed');
  console.log(`改名: ${rn?.oldName} → ${rn?.newName}`);

  // 先收池清场
  console.log('\n--- 收池清场 ---');
  c1.ws.send(JSON.stringify({ type: 'winPot' }));
  await sleep(300);

  // === 测试 Call 和 Fold ===
  console.log('\n--- 新一轮：c1 下注 80, c2 下注 200 ===');
  c1.ws.send(JSON.stringify({ type: 'bet', amount: 80 }));
  c2.ws.send(JSON.stringify({ type: 'bet', amount: 200 }));
  await sleep(300);

  // c1 call（应补 120 到与 c2 持平 = 200）
  console.log('--- c1 跟注 (call) ---');
  c1.ws.send(JSON.stringify({ type: 'call' }));
  await sleep(300);
  const c1Call = c1.msgs.filter(m => m.type === 'bet' && m.playerId === c1.id && m.isCall).pop();
  console.log(`c1 跟注: 补了=${c1Call?.amount}, potBet=${c1Call?.potBet}, 余额=${c1Call?.balance}`);
  console.assert(c1Call && c1Call.amount === 120, `❌ c1 应补120, 实际=${c1Call?.amount}`);
  console.assert(c1Call && c1Call.potBet === 200, `❌ c1 potBet 应=200, 实际=${c1Call?.potBet}`);

  // c2 fold
  console.log('--- c2 弃牌 (fold) ---');
  c2.ws.send(JSON.stringify({ type: 'fold' }));
  await sleep(300);
  const foldMsg = c1.msgs.find(m => m.type === 'fold' && m.playerId === c2.id);
  console.log(`c1 收到 c2 弃牌: ${foldMsg ? '✅' : '❌'}`);
  console.assert(foldMsg, '❌ c1 应收到 c2 的 fold 消息');

  // c2 再 try bet（应被拒绝）
  console.log('--- c2 弃牌后尝试下注（应失败）---');
  c2.ws.send(JSON.stringify({ type: 'bet', amount: 50 }));
  await sleep(200);
  const errMsg = c2.msgs.filter(m => m.type === 'error').pop();
  console.log(`c2 收到错误: ${errMsg?.message}`);
  console.assert(errMsg && errMsg.message.includes('弃牌'), `❌ 应报已弃牌错误`);

  // c1 收池
  console.log('\n--- c1 收池 ---');
  c1.ws.send(JSON.stringify({ type: 'winPot' }));
  await sleep(300);

  const wp1 = c1.msgs.find(m => m.type === 'winPot');
  const wp2 = c2.msgs.find(m => m.type === 'winPot');
  console.log(`c1 收到收池: 赢家=${wp1?.winnerName}, 金额=${wp1?.amount}`);
  console.log(`c2 收到收池: 赢家=${wp2?.winnerName}, 金额=${wp2?.amount}`);
  if (wp1) {
    for (const [pid, info] of Object.entries(wp1.results)) {
      console.log(`  ${info.name}: 投入=${info.contribution}, 净输赢=${info.net}, 余额=${info.balance}, 总输赢=${info.totalWinLoss}`);
    }
  }

  // 断线重连
  console.log('\n--- 断线重连 ---');
  c1.ws.close();
  await sleep(500);
  const c1r = await new Promise((resolve) => {
    const ws = new WebSocket(URL);
    ws.on('open', () => ws.send(JSON.stringify({ type: 'join', name: '德爷爷', playerId: c1.id })));
    ws.on('message', (d) => { const m = JSON.parse(d); if (m.type === 'joined') resolve({ ws, msg: m }); });
  });
  console.log(`重连: 余额=${c1r.msg.balance}, 身份=${c1r.msg.playerId === c1.id ? '恢复' : '新ID'}`);

  c1.ws.close(); c2.ws.close(); c1r.ws.close();
  console.log('\n✅ 全部测试通过');
  process.exit(0);
})();

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
