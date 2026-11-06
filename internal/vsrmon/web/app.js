const HISTORY = 240;
const POLL_MS = 1000;
const RETRY_MS = 5000;
const TICK_MS = 500;
const SVG_NS = 'http://www.w3.org/2000/svg';
const SIZE = 560;
const CENTER = SIZE / 2;
const RADIUS = 165;
const NODE_R = 22;
const PRIMARY_R = 27;
const BOW = 0.22;
const GAP = 5;
const SPARK_W = 240;
const SPARK_H = 40;
const CONN_LABEL = { live: 'Live', reconnecting: 'Reconnecting', demo: 'Demo' };

const DEMO_ADDRS = ['r1:7000', 'r2:7000', 'r3:7000', 'r4:7000', 'r5:7000'];
const DEMO_EPOCH = 3;
const DEMO_CYCLE = 300;
const WARMUP_TICKS = 120;
const VIEW_CHANGE_TICKS = 6;
const RECOVER_TICKS = 6;
const CHECKPOINT_TICKS = 48;

const newNodeState = () => ({
  node: null,
  lastSeen: 0,
  last: null,
  rates: { commit: 0, peers: new Map() },
  hist: { commit: [], log: [] },
});
const EMPTY_NODE_STATE = newNodeState();

const state = {
  targets: [],
  addrOf: new Map(),
  nodes: new Map(),
  tips: new Map(),
  selected: null,
  hover: null,
  pointer: null,
  received: 0,
  conn: 'reconnecting',
};

const $ = (id) => document.getElementById(id);
const cls = (...names) => names.filter(Boolean).join(' ');
const max = (list) => (list.length ? Math.max(...list) : 0);
const min = (list) => (list.length ? Math.min(...list) : 0);
const int = (v) => Math.round(v ?? 0).toLocaleString('en-US');
const ts = (iso) => {
  const t = Date.parse(iso);
  return t > 0 ? t : 0;
};
const iso = (ms) => new Date(ms).toISOString();
const push = (series, value) => {
  series.push(value);
  if (series.length > HISTORY) series.shift();
};
const rateText = (v) => (v >= 10 ? Math.round(v).toLocaleString('en-US') : v.toFixed(1));
const bytesText = (b) => (b < 1024 ? `${b} B` : `${(b / 1024).toFixed(1)} KiB`);
const msText = (v) => `${(v ?? 0).toFixed(1)} ms`;
const clockText = (ms) => new Date(ms).toLocaleTimeString('en-GB');

function durationText(ms) {
  const sec = Math.max(0, Math.floor(ms / 1000));
  const minutes = Math.floor(sec / 60);
  const hours = Math.floor(minutes / 60);
  if (hours >= 24) return `${Math.floor(hours / 24)}d ${hours % 24}h`;
  if (hours) return `${hours}h ${minutes % 60}m`;
  if (minutes) return `${minutes}m ${sec % 60}s`;
  return `${sec}s`;
}

const sinceText = (ms) => `${durationText(ms)} ago`;

function uptimeText(node) {
  const started = ts(node.started);
  const now = ts(node.now);
  return started && now ? durationText(now - started) : '—';
}

function build(node, attrs, kids) {
  for (const [key, value] of Object.entries(attrs)) {
    if (value !== null && value !== undefined && value !== false) node.setAttribute(key, value);
  }
  node.append(...kids.flat().filter((kid) => kid !== null && kid !== undefined && kid !== false));
  return node;
}

const h = (tag, attrs, ...kids) => build(document.createElement(tag), attrs, kids);
const s = (tag, attrs, ...kids) => build(document.createElementNS(SVG_NS, tag), attrs, kids);

function applyCluster(cluster) {
  const now = ts(cluster.time) || Date.now();
  state.targets = cluster.targets;
  for (const target of cluster.targets) {
    if (!target.node) continue;
    state.addrOf.set(target.url, target.node.addr);
    observe(target.node, now);
  }
  state.received = Date.now();
}

function observe(node, now) {
  if (!state.nodes.has(node.addr)) state.nodes.set(node.addr, newNodeState());
  const ns = state.nodes.get(node.addr);
  updateRates(ns, node, now);
  ns.node = node;
  ns.lastSeen = now;
  push(ns.hist.commit, node.commitNum);
  push(ns.hist.log, node.logLen);
}

function updateRates(ns, node, now) {
  const last = ns.last;
  const seconds = last ? (now - last.t) / 1000 : 0;
  const sent = new Map(node.peers.map((peer) => [peer.addr, peer.sent]));
  ns.rates = {
    commit: rateBetween(last?.commit, node.commitNum, seconds),
    peers: new Map([...sent].map(([addr, value]) => [addr, rateBetween(last?.sent.get(addr), value, seconds)])),
  };
  ns.last = { t: now, commit: node.commitNum, sent };
}

function rateBetween(before, after, seconds) {
  return before !== undefined && seconds > 0 && after > before ? (after - before) / seconds : 0;
}

function replicaOf(target) {
  const addr = state.addrOf.get(target.url) ?? '';
  const ns = state.nodes.get(addr) ?? EMPTY_NODE_STATE;
  return {
    url: target.url,
    addr,
    target,
    ns,
    node: target.node ?? ns.node,
    up: Boolean(target.reachable && target.node),
  };
}

function statusOf(rep) {
  return rep.target.reachable ? (rep.node?.status ?? 'unknown') : 'unreachable';
}

function buildModel() {
  const replicas = state.targets.map(replicaOf);
  const addrs = configOrder(replicas);
  const up = replicas.filter((rep) => rep.up);
  const byAddr = new Map(replicas.filter((rep) => rep.addr).map((rep) => [rep.addr, rep]));
  return {
    replicas,
    index: new Map(addrs.map((addr, i) => [addr, i])),
    nodes: layoutNodes(addrs, byAddr),
    edges: buildEdges(replicas),
    summary: summarize(replicas, up),
    maxCommit: max(up.map((rep) => rep.node.commitNum)),
    maxLog: max(up.map((rep) => rep.node.logLen)),
  };
}

function configOrder(replicas) {
  const addrs = new Set();
  const add = (addr) => addrs.add(addr);
  replicas.forEach((rep) => (rep.node?.config ?? []).forEach(add));
  replicas.forEach((rep) => rep.addr && add(rep.addr));
  replicas.forEach((rep) => (rep.node?.peers ?? []).forEach((peer) => add(peer.addr)));
  return [...addrs];
}

function layoutNodes(addrs, byAddr) {
  return addrs.map((addr, i) => {
    const angle = -Math.PI / 2 + (2 * Math.PI * i) / addrs.length;
    const rep = byAddr.get(addr);
    return {
      addr,
      angle,
      rep,
      x: CENTER + RADIUS * Math.cos(angle),
      y: CENTER + RADIUS * Math.sin(angle),
      status: rep ? statusOf(rep) : 'unknown',
      primary: Boolean(rep?.up && rep.node.primary),
    };
  });
}

function buildEdges(replicas) {
  return replicas
    .filter((rep) => rep.addr && rep.node)
    .flatMap((rep) =>
      rep.node.peers.map((peer) => {
        const kind = rep.up ? (peer.connected ? 'up' : 'down') : 'unknown';
        return {
          from: rep.addr,
          to: peer.addr,
          peer,
          kind,
          rate: rep.up ? (rep.ns.rates.peers.get(peer.addr) ?? 0) : 0,
          age: sinceAge(peer, ts(rep.node.now)),
        };
      }),
    );
}

function sinceAge(peer, nowMs) {
  const since = ts(peer.since);
  return since && nowMs ? nowMs - since : null;
}

function summarize(replicas, up) {
  const total = replicas.length;
  const primary = up.filter((rep) => rep.node.primary).sort((a, b) => b.node.view - a.node.view)[0];
  const views = up.map((rep) => rep.node.view);
  const commits = up.map((rep) => rep.node.commitNum);
  const connected = up.reduce((sum, rep) => sum + rep.node.peers.filter((peer) => peer.connected).length, 0);
  const expected = total * (total - 1);
  const reconfiguring = up.some((rep) => rep.node.oldConfig?.length);
  const viewsAgree = min(views) === max(views);
  return [
    {
      label: 'Replicas up',
      value: `${up.length} / ${total}`,
      sub: up.length === total ? 'all reachable' : `${total - up.length} unreachable`,
      tone: upTone(up.length, total),
    },
    {
      label: 'Primary',
      value: primary ? primary.node.addr : 'none',
      sub: primary ? `view ${primary.node.view}` : 'no primary elected',
      tone: primary ? 'ok' : 'warn',
    },
    {
      label: 'Epoch',
      value: int(max(up.map((rep) => rep.node.epoch))),
      sub: reconfiguring ? 'reconfiguring' : `${(primary ?? up[0])?.node.config.length ?? 0} members`,
    },
    {
      label: 'View',
      value: int(max(views)),
      sub: viewsAgree ? 'all replicas agree' : `${min(views)} to ${max(views)}, changing`,
      tone: viewsAgree ? undefined : 'warn',
    },
    {
      label: 'Max commit',
      value: int(max(commits)),
      sub: `${int(max(commits) - min(commits))} op spread`,
    },
    {
      label: 'Commit rate',
      value: `${rateText(max(up.map((rep) => rep.ns.rates.commit)))}/s`,
      sub: 'fastest replica',
    },
    {
      label: 'Peer links',
      value: expected ? `${connected} / ${expected}` : '—',
      sub: 'connected edges',
      tone: linkTone(connected, expected),
    },
  ];
}

function upTone(up, total) {
  if (!total) return undefined;
  if (up === total) return 'ok';
  return up ? 'warn' : 'bad';
}

function linkTone(connected, expected) {
  if (!expected) return undefined;
  if (connected === expected) return 'ok';
  return connected * 2 >= expected ? 'warn' : 'bad';
}

function render() {
  const model = buildModel();
  renderSummary(model);
  renderGraph(model);
  renderCards(model);
  renderStatus();
}

function renderStatus() {
  $('conn').dataset.state = state.conn;
  $('conn-label').textContent = CONN_LABEL[state.conn];
  $('updated').textContent = state.received
    ? `Last update ${clockText(state.received)} (${sinceText(Date.now() - state.received)})`
    : 'Waiting for data';
}

function renderSummary({ summary }) {
  $('summary').replaceChildren(...summary.map(tileEl));
}

function tileEl({ label, value, sub, tone }) {
  return h(
    'div',
    { class: 'tile', 'data-tone': tone },
    h('span', { class: 'tile-label' }, label),
    h('strong', { class: 'tile-value' }, value),
    h('span', { class: 'tile-sub' }, sub),
  );
}

function renderGraph(model) {
  const at = new Map(model.nodes.map((n) => [n.addr, n]));
  state.tips = new Map();
  $('graph').replaceChildren(
    arrowDefs(),
    s(
      'g',
      {},
      model.edges.map((e) => edgeEl(e, at)),
    ),
    s(
      'g',
      {},
      model.nodes.map(nodeEl),
    ),
  );
  $('empty').hidden = model.nodes.length > 0;
  refreshTip();
}

function arrowDefs() {
  return s(
    'defs',
    {},
    ['up', 'down', 'unknown'].map((kind) =>
      s(
        'marker',
        {
          id: `arrow-${kind}`,
          viewBox: '0 0 10 10',
          refX: 10,
          refY: 5,
          markerWidth: 12,
          markerHeight: 12,
          markerUnits: 'userSpaceOnUse',
          orient: 'auto',
        },
        s('path', { class: `arrow arrow-${kind}`, d: 'M0 0 L10 5 L0 10 z' }),
      ),
    ),
  );
}

function edgeEl(e, at) {
  const d = edgePath(at.get(e.from), at.get(e.to));
  const title = remember(edgeTip(e));
  const dimmed = state.selected !== null && state.selected !== e.from && state.selected !== e.to;
  return s(
    'g',
    { class: cls('edge-group', dimmed && 'dim') },
    s('path', {
      class: cls('edge', `edge-${e.kind}`, e.kind === 'up' && e.rate > 0 && 'flowing'),
      d,
      'stroke-width': (1.2 + Math.min(1.8, e.rate / 6)).toFixed(2),
      'marker-end': `url(#arrow-${e.kind})`,
    }),
    s('path', { class: 'hit', d, 'data-tip': title }),
  );
}

function edgePath(a, b) {
  const control = {
    x: (a.x + b.x) / 2 - (b.y - a.y) * BOW,
    y: (a.y + b.y) / 2 + (b.x - a.x) * BOW,
  };
  const [sx, sy] = towards(a, control, radiusOf(a));
  const [ex, ey] = towards(b, control, radiusOf(b) + GAP);
  return `M${sx} ${sy} Q${control.x.toFixed(1)} ${control.y.toFixed(1)} ${ex} ${ey}`;
}

function towards(from, to, distance) {
  const len = Math.hypot(to.x - from.x, to.y - from.y) || 1;
  return [
    (from.x + ((to.x - from.x) * distance) / len).toFixed(1),
    (from.y + ((to.y - from.y) * distance) / len).toFixed(1),
  ];
}

const radiusOf = (n) => (n.primary ? PRIMARY_R : NODE_R);

function nodeEl(n) {
  const r = radiusOf(n);
  const label = labelAt(n.angle, r);
  const view = n.rep?.up ? `v${n.rep.node.view}` : null;
  return s(
    'g',
    {
      class: cls('node', n.addr === state.selected && 'is-selected'),
      'data-status': n.status,
      'data-addr': n.addr,
      'data-tip': remember(nodeTip(n)),
      transform: `translate(${n.x} ${n.y})`,
    },
    n.primary && s('circle', { class: 'ring', r: r + 7 }),
    s('circle', { class: 'body', r }),
    view && s('text', { class: 'node-view', 'text-anchor': 'middle', dy: '0.35em' }, view),
    n.primary &&
      s(
        'g',
        { class: 'badge', transform: `translate(${r * 0.75} ${-r * 0.75})` },
        s('circle', { r: 9 }),
        s('text', { 'text-anchor': 'middle', dy: '0.35em' }, 'P'),
      ),
    s(
      'text',
      {
        class: cls('label', n.primary && 'is-primary'),
        x: label.x,
        y: label.y,
        'text-anchor': label.anchor,
        dy: label.dy,
      },
      n.addr,
    ),
  );
}

function labelAt(angle, r) {
  const distance = r + 10;
  const c = Math.cos(angle);
  const sn = Math.sin(angle);
  return {
    x: c * distance,
    y: sn * distance,
    anchor: c > 0.3 ? 'start' : c < -0.3 ? 'end' : 'middle',
    dy: sn > 0.3 ? '0.9em' : sn < -0.3 ? '0' : '0.35em',
  };
}

function remember(tip) {
  state.tips.set(tip.title, tip);
  return tip.title;
}

function edgeTip(e) {
  const title = `${e.from} → ${e.to}`;
  if (e.kind === 'unknown') {
    return { title, rows: [['state', 'unknown'], ['reason', 'sender unreachable']] };
  }
  const { peer } = e;
  return {
    title,
    rows: [
      ['state', peer.connected ? 'connected' : 'disconnected'],
      [peer.connected ? 'connected for' : 'down for', e.age === null ? '—' : durationText(e.age)],
      ['sent', `${int(peer.sent)} (${rateText(e.rate)}/s)`],
      ['dropped', int(peer.dropped)],
      ['queued', int(peer.queued)],
      ['dials', int(peer.dials)],
    ],
  };
}

function nodeTip(n) {
  const rep = n.rep;
  const rows = [['status', n.status]];
  if (!rep) rows.push(['note', 'in config, not polled']);
  if (rep?.node) rows.push(...nodeRows(rep.node));
  if (rep && !rep.up) rows.push(['last seen', rep.ns.lastSeen ? sinceText(Date.now() - rep.ns.lastSeen) : 'never']);
  if (rep?.target.error) rows.push(['error', rep.target.error]);
  return { title: n.addr, rows };
}

function nodeRows(node) {
  return [
    ['epoch', int(node.epoch)],
    ['view', int(node.view)],
    ['op / commit', `${int(node.opNum)} / ${int(node.commitNum)}`],
    ['log', `${int(node.logLen)} from ${int(node.logBase)}`],
    ['checkpoint', `${int(node.checkpointOp)} · ${bytesText(node.checkpointBytes)}`],
  ];
}

function refreshTip() {
  const box = $('tip');
  const tip = state.hover ? state.tips.get(state.hover) : null;
  box.hidden = !tip;
  if (!tip) return;
  box.replaceChildren(
    h('div', { class: 'tip-title' }, tip.title),
    h(
      'dl',
      {},
      tip.rows.flatMap(([key, value]) => [h('dt', {}, key), h('dd', {}, value)]),
    ),
  );
  if (state.pointer) placeTip(state.pointer);
}

function placeTip({ x, y }) {
  const box = $('tip');
  const gap = 14;
  const left = Math.min(x + gap, innerWidth - box.offsetWidth - 8);
  const top = y + gap + box.offsetHeight > innerHeight ? y - gap - box.offsetHeight : y + gap;
  box.style.transform = `translate(${Math.max(8, left)}px, ${Math.max(8, top)}px)`;
}

function onPointer(event) {
  const hit = event.target.closest('[data-tip]');
  state.hover = hit ? hit.dataset.tip : null;
  state.pointer = { x: event.clientX, y: event.clientY };
  refreshTip();
}

function renderCards(model) {
  const rank = (rep) => model.index.get(rep.addr) ?? model.index.size;
  const sorted = [...model.replicas].sort((a, b) => rank(a) - rank(b));
  $('cards').replaceChildren(...sorted.map((rep) => replicaCard(rep, model)));
}

function replicaCard(rep, model) {
  const selected = Boolean(rep.addr) && rep.addr === state.selected;
  return h(
    'article',
    {
      class: cls('card', !rep.up && 'stale', selected && 'is-selected'),
      'data-status': statusOf(rep),
      'data-addr': rep.addr || null,
      tabindex: rep.addr ? 0 : null,
      role: rep.addr ? 'button' : null,
      'aria-pressed': rep.addr ? String(selected) : null,
    },
    cardHead(rep),
    rep.node ? [cardStats(rep, model), cardSparks(rep)] : h('p', { class: 'muted' }, 'No status yet'),
    rep.up ? null : cardNotice(rep),
  );
}

function cardHead(rep) {
  return h(
    'header',
    { class: 'card-head' },
    h(
      'div',
      { class: 'card-id' },
      h('span', { class: 'addr' }, rep.addr || rep.url),
      rep.up && rep.node.primary && h('span', { class: 'pbadge', title: 'primary' }, 'P'),
    ),
    h('span', { class: 'chip' }, statusOf(rep)),
  );
}

function cardStats(rep, model) {
  const n = rep.node;
  const lag = model.maxCommit - n.commitNum;
  return h(
    'div',
    { class: 'stats' },
    h(
      'dl',
      { class: 'kv' },
      stat('View', int(n.view)),
      stat('Epoch', int(n.epoch)),
      stat('Op', int(n.opNum)),
      stat('Commit', lag > 0 ? `${int(n.commitNum)} (−${int(lag)})` : int(n.commitNum)),
      stat('Commit rate', `${rateText(rep.ns.rates.commit)}/s`),
      stat('Latency', msText(rep.target.latencyMs)),
      stat('Checkpoint', `${int(n.checkpointOp)} · ${bytesText(n.checkpointBytes)}`),
      stat('Uptime', uptimeText(n)),
      stat('Received', int(n.received)),
      stat('Inbox dropped', int(n.inboxDropped)),
    ),
    logRow(n, model.maxLog),
  );
}

const stat = (label, value) => h('div', {}, h('dt', {}, label), h('dd', {}, value));

function logRow(n, maxLog) {
  const pct = Math.min(100, (n.logLen / Math.max(1, maxLog)) * 100);
  return h(
    'div',
    { class: 'log' },
    h(
      'div',
      { class: 'log-head' },
      h('span', {}, 'Log'),
      h('span', {}, `${int(n.logLen)} entries from base ${int(n.logBase)}`),
    ),
    h('div', { class: 'bar' }, h('span', { class: 'bar-fill', style: `width: ${pct.toFixed(1)}%` })),
  );
}

function cardSparks(rep) {
  return h(
    'div',
    { class: 'sparks' },
    sparkBlock('Commit', rep.ns.hist.commit),
    sparkBlock('Log length', rep.ns.hist.log),
  );
}

function sparkBlock(label, series) {
  return h(
    'div',
    { class: 'spark-block' },
    h(
      'div',
      { class: 'spark-head' },
      h('span', {}, label),
      h('span', {}, series.length ? int(series.at(-1)) : '—'),
    ),
    sparkline(series),
  );
}

function sparkline(series) {
  const attrs = { class: 'spark', viewBox: `0 0 ${SPARK_W} ${SPARK_H}`, preserveAspectRatio: 'none', 'aria-hidden': 'true' };
  if (series.length < 2) return s('svg', attrs);
  const lo = min(series);
  const span = max(series) - lo || 1;
  const step = SPARK_W / (HISTORY - 1);
  const points = series.map((v, i) => [
    SPARK_W - (series.length - 1 - i) * step,
    SPARK_H - 3 - ((v - lo) / span) * (SPARK_H - 6),
  ]);
  const line = points.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
  const left = points[0][0].toFixed(1);
  const right = points.at(-1)[0].toFixed(1);
  return s(
    'svg',
    attrs,
    s('polygon', { class: 'spark-area', points: `${left},${SPARK_H} ${line} ${right},${SPARK_H}` }),
    s('polyline', { class: 'spark-line', points: line }),
  );
}

function cardNotice(rep) {
  const seen = rep.ns.lastSeen ? `last seen ${sinceText(Date.now() - rep.ns.lastSeen)}` : 'never seen';
  return h('p', { class: 'card-notice' }, `${rep.target.error || 'unreachable'} · ${seen}`);
}

function toggleSelect(addr) {
  state.selected = state.selected === addr ? null : addr;
  render();
}

function selectFrom(event) {
  const hit = event.target.closest('[data-addr]');
  if (hit) toggleSelect(hit.dataset.addr);
}

function bindEvents() {
  const graph = $('graph');
  graph.addEventListener('pointermove', onPointer);
  graph.addEventListener('pointerdown', onPointer);
  graph.addEventListener('pointerleave', () => {
    state.hover = null;
    refreshTip();
  });
  graph.addEventListener('click', selectFrom);

  const cards = $('cards');
  cards.addEventListener('click', selectFrom);
  cards.addEventListener('keydown', (event) => {
    if ((event.key === 'Enter' || event.key === ' ') && event.target.closest('[data-addr]')) {
      event.preventDefault();
      selectFrom(event);
    }
  });
}

function receive(cluster) {
  applyCluster(cluster);
  render();
}

function setConn(kind) {
  state.conn = kind;
  renderStatus();
}

function startLive() {
  const open = () => {
    const source = new EventSource('api/stream');
    source.onopen = () => {
      stopPolling();
      setConn('live');
    };
    source.onmessage = (event) => receive(JSON.parse(event.data));
    source.onerror = () => {
      source.close();
      setConn('reconnecting');
      startPolling();
      setTimeout(open, RETRY_MS);
    };
  };
  open();
}

let pollTimer = null;

function startPolling() {
  if (pollTimer === null) pollTimer = setInterval(poll, POLL_MS);
  poll();
}

function stopPolling() {
  clearInterval(pollTimer);
  pollTimer = null;
}

async function poll() {
  try {
    const response = await fetch('api/cluster', { cache: 'no-store' });
    if (response.ok) receive(await response.json());
  } catch {
    setConn('reconnecting');
  }
}

function createSim(now) {
  return {
    tick: 0,
    view: 0,
    viewChange: 0,
    replicas: DEMO_ADDRS.map((addr, index) => newReplica(addr, index, now)),
  };
}

function newReplica(addr, index, now) {
  const peers = DEMO_ADDRS.filter((other) => other !== addr).map((other) => [
    other,
    { addr: other, connected: true, since: now, queued: 0, sent: 0, dropped: 0, dials: 1 },
  ]);
  return {
    addr,
    index,
    url: `http://${addr.split(':')[0]}:8080`,
    status: 'normal',
    opNum: 1200,
    commitNum: 1195,
    logBase: 1000,
    checkpointOp: 1000,
    checkpointBytes: 5120,
    received: 40000,
    inboxDropped: 0,
    started: now - 7200000 - index * 60000,
    down: 0,
    recover: 0,
    error: '',
    peers: new Map(peers),
  };
}

function stepSim(sim, now) {
  sim.tick += 1;
  scheduleFaults(sim);
  if (sim.viewChange > 0 && --sim.viewChange === 0) sim.view = nextView(sim);
  for (const r of sim.replicas) {
    if (r.down > 0 && --r.down === 0) {
      r.recover = RECOVER_TICKS;
      r.started = now;
    }
  }
  const up = sim.replicas.filter((r) => !r.down);
  for (const r of up) r.status = statusFor(sim, r);
  const primary = primaryOf(sim);
  if (!primary.down && primary.status === 'normal') grow(primary);
  for (const r of up) {
    if (r !== primary) follow(r, primary);
  }
  for (const r of up) {
    checkpoint(sim, r);
    traffic(sim, r, now);
  }
  return { time: iso(now), targets: sim.replicas.map((r) => targetOf(sim, r, now)) };
}

function scheduleFaults(sim) {
  const phase = sim.tick % DEMO_CYCLE;
  if (phase === 60) takeDown(sim, primaryOf(sim), 14, 'connection refused');
  if (phase === 150) takeDown(sim, sim.replicas[3], 20, 'context deadline exceeded');
}

function takeDown(sim, r, ticks, error) {
  r.down = ticks;
  r.error = error;
  if (r === primaryOf(sim)) sim.viewChange = VIEW_CHANGE_TICKS;
}

function primaryOf(sim) {
  return sim.replicas[sim.view % sim.replicas.length];
}

function nextView(sim) {
  let view = sim.view + 1;
  while (sim.replicas[view % sim.replicas.length].down) view += 1;
  return view;
}

function statusFor(sim, r) {
  if (r.recover > 0) {
    r.recover -= 1;
    return 'recovering';
  }
  return sim.viewChange > 0 ? 'view-change' : 'normal';
}

function grow(p) {
  p.opNum += 4 + Math.floor(Math.random() * 4);
  p.commitNum = Math.max(p.commitNum, p.opNum - 1 - (Math.random() < 0.3 ? 1 : 0));
}

function follow(r, p) {
  if (r.status === 'recovering') {
    Object.assign(r, { opNum: p.opNum, commitNum: p.commitNum, logBase: p.logBase });
  } else if (r.status === 'normal') {
    r.opNum = Math.max(r.opNum, p.opNum - Math.floor(Math.random() * 3));
    r.commitNum = Math.max(r.commitNum, Math.min(p.commitNum, r.opNum));
  }
}

function checkpoint(sim, r) {
  if (r.status !== 'normal' || (sim.tick + r.index * 7) % CHECKPOINT_TICKS !== 0) return;
  r.checkpointOp = Math.max(r.checkpointOp, r.commitNum - 2);
  r.logBase = Math.max(r.logBase, r.checkpointOp);
  r.checkpointBytes = 5000 + Math.floor(Math.random() * 2000);
}

function traffic(sim, r, now) {
  r.received += 6 + Math.floor(Math.random() * 6);
  if (Math.random() < 0.01) r.inboxDropped += 1;
  const sending = r === primaryOf(sim);
  for (const peer of r.peers.values()) {
    const connected = linkUp(sim, r, peer.addr);
    if (connected !== peer.connected) {
      peer.connected = connected;
      peer.since = now;
      if (connected) peer.dials += 1;
    }
    if (connected) {
      peer.queued = Math.random() < 0.15 ? 1 : 0;
      peer.sent += (sending ? 5 : 2) + Math.floor(Math.random() * 3);
    } else {
      peer.queued = Math.min(peer.queued + 3, 48);
      if (peer.queued === 48) peer.dropped += 3;
      if (sim.tick % 4 === 0) peer.dials += 1;
    }
  }
}

function linkUp(sim, from, to) {
  const phase = sim.tick % DEMO_CYCLE;
  const cut = from.addr === 'r2:7000' && to === 'r5:7000' && phase >= 100 && phase < 130;
  return !cut && !sim.replicas.find((r) => r.addr === to).down;
}

function targetOf(sim, r, now) {
  if (r.down) return { url: r.url, reachable: false, error: r.error, latencyMs: 300 };
  return { url: r.url, reachable: true, latencyMs: 1 + Math.random() * 3, node: nodeOf(sim, r, now) };
}

function nodeOf(sim, r, now) {
  const primary = primaryOf(sim);
  return {
    addr: r.addr,
    status: r.status,
    primary: r === primary && sim.viewChange === 0,
    primaryAddr: primary.addr,
    epoch: DEMO_EPOCH,
    view: sim.view,
    opNum: r.opNum,
    commitNum: r.commitNum,
    logBase: r.logBase,
    logLen: r.opNum - r.logBase,
    checkpointOp: r.checkpointOp,
    checkpointBytes: r.checkpointBytes,
    config: DEMO_ADDRS,
    received: r.received,
    inboxDropped: r.inboxDropped,
    started: iso(r.started),
    now: iso(now),
    peers: [...r.peers.values()].map((peer) => ({ ...peer, since: iso(peer.since) })),
  };
}

function startDemo() {
  setConn('demo');
  const t0 = Date.now() - WARMUP_TICKS * TICK_MS;
  const sim = createSim(t0);
  for (let i = 0; i < WARMUP_TICKS; i += 1) applyCluster(stepSim(sim, t0 + i * TICK_MS));
  render();
  setInterval(() => receive(stepSim(sim, Date.now())), TICK_MS);
}

function start() {
  setConn('reconnecting');
  bindEvents();
  setInterval(renderStatus, 1000);
  if (new URLSearchParams(location.search).has('demo')) startDemo();
  else startLive();
}

start();
