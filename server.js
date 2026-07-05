"use strict";
/**
 * Project Void - Game Server (SQLite + native auth)
 * Deploy on Fly.io.
 *
 * Architecture:
 *   - All game data stored in SQLite (better-sqlite3)
 *   - Auth: server-native (scrypt password hashing + JWT tokens)
 *   - Real-time push via WebSocket (all data reads/writes go through WS)
 *   - No external dependencies for auth or data storage
 *
 * Environment variables:
 *   JWT_SECRET  - secret key for signing JWT tokens (generate with: openssl rand -hex 32)
 *   DB_PATH     - path to SQLite database file (use persistent volume)
 *   PORT        - set automatically by Fly.io
 */

const http    = require("http");
const fs      = require("fs");
const path    = require("path");
const zlib    = require("zlib");
const crypto  = require("crypto");
const os      = require("os");
const { WebSocketServer } = require("ws");
const Database = require("better-sqlite3");

// ── Redis pub/sub for cross-machine broadcast (Upstash / any Redis) ───────────
// Set REDIS_URL env var to enable. Without it, server works single-machine.
// Format: redis://default:<password>@<host>:<port>  (or rediss:// for TLS)
const REDIS_URL = process.env.REDIS_URL || null;
let redisPub = null;  // publishes events
let redisSub = null;  // subscribes, receives events from all machines
const PV_CHANNEL = "pv:broadcast"; // single channel, all events routed by type

function _redisConnect() {
  if (!REDIS_URL) return;
  if (process.env.REDIS_ENABLED !== "true") { console.log("[REDIS] disabled (REDIS_ENABLED != true) — skipping"); return; }
  try {
    const net  = require("net");
    const tls  = require("tls");
    const isTLS = REDIS_URL.startsWith("rediss://");
    // Lightweight Redis client — only needs SUBSCRIBE and PUBLISH
    // No external dependency: we implement the Redis RESP protocol directly
    function createRedisClient(onMessage) {
      const url  = new URL(REDIS_URL);
      const host = url.hostname;
      const port = parseInt(url.port) || (isTLS ? 6380 : 6379);
      const pass = url.password ? decodeURIComponent(url.password) : null;
      const user = url.username || "default";
      let sock, buf = "", ready = false, subMode = false;
      const pending = [];

      function connect() {
        sock = isTLS
          ? tls.connect(port, host, { servername: host })
          : net.connect(port, host);
        sock.setEncoding("utf8");
        sock.on("connect", () => {
          if (pass) sock.write(`*3\r\n$4\r\nAUTH\r\n$${user.length}\r\n${user}\r\n$${pass.length}\r\n${pass}\r\n`);
          ready = true;
          for (const cmd of pending) sock.write(cmd);
          pending.length = 0;
        });
        sock.on("data", chunk => {
          buf += chunk;
          // Parse RESP responses — we only care about message arrays
          const lines = buf.split("\r\n");
          buf = lines.pop();
          for (let i = 0; i < lines.length; i++) {
            // Look for *3 (array of 3) — subscribe confirmation or message
            if (lines[i] === "*3" && i + 6 < lines.length) {
              const kind = lines[i+2];
              const ch   = lines[i+4];
              const data = lines[i+6];
              if (kind === "message" && onMessage) {
                try { onMessage(ch, data); } catch(e) {}
              }
              i += 6;
            }
          }
        });
        sock.on("error", e => console.error("[REDIS] socket error:", e.message));
        sock.on("close", () => {
          ready = false;
          console.warn("[REDIS] disconnected, reconnecting in 3s...");
          setTimeout(connect, 3000);
        });
      }

      connect();

      return {
        send(cmd) {
          if (ready && sock) sock.write(cmd);
          else pending.push(cmd);
        },
        subscribe(channel) {
          subMode = true;
          const cmd = `*2\r\n$9\r\nSUBSCRIBE\r\n$${channel.length}\r\n${channel}\r\n`;
          this.send(cmd);
        },
        publish(channel, msg) {
          const cmd = `*3\r\n$7\r\nPUBLISH\r\n$${channel.length}\r\n${channel}\r\n$${Buffer.byteLength(msg)}\r\n${msg}\r\n`;
          this.send(cmd);
        }
      };
    }

    // Publisher connection
    redisPub = createRedisClient(null);

    // Subscriber connection (separate socket — Redis requires it)
    redisSub = createRedisClient((channel, data) => {
      if (channel !== PV_CHANNEL) return;
      try {
        const ev = JSON.parse(data);
        _handleRedisEvent(ev);
      } catch(e) { console.error("[REDIS] bad message:", e.message); }
    });
    redisSub.subscribe(PV_CHANNEL);

    console.log(`[REDIS] connecting to ${new URL(REDIS_URL).hostname}`);
  } catch(e) {
    console.error("[REDIS] init error:", e.message);
    redisPub = null; redisSub = null;
  }
}

function _redisPublish(ev) {
  if (!redisPub) return;
  try { redisPub.publish(PV_CHANNEL, JSON.stringify(ev)); } catch(e) {}
}

// Handle an event received from Redis (published by any machine including us)
// We skip events published by THIS machine to avoid double-delivery
const MY_MACHINE_ID = process.env.FLY_MACHINE_ID || process.env.HOSTNAME || require("crypto").randomBytes(4).toString("hex");

function _handleRedisEvent(ev) {
  if (ev._src === MY_MACHINE_ID) return; // our own publish — skip
  switch (ev.t) {
    case "zone_chat":
      _pushZoneChatToZone(ev.zone, ev.msg);
      break;
    case "guild_chat":
      _pushGuildChatToMembers(ev.guildId, ev.msg, ev.excl || null);
      break;
    case "party_chat":
      _pushPartyChatToMembers(ev.partyId, ev.msg, ev.excl || null);
      break;
    case "inbox":
      _pushInbox(ev.uid);
      break;
    case "dms":
      _pushDms(ev.uid);
      break;
    case "party":
      _pushPartyToSubscribers(ev.partyId, ev.excl || null);
      break;
    case "party_hp_patch": {
      // Relay lightweight HP patch to members on this machine
      const patchStr = JSON.stringify({ type: "party_hp_patch", partyId: ev.partyId, uid: ev.uid, hp: ev.hp, maxHp: ev.maxHp });
      const patchParty = dbGetParty(ev.partyId);
      if (patchParty && patchParty.members) {
        for (const m of patchParty.members) {
          if (!m.uid || m.uid === ev.excl) continue;
          const c = clients.get(m.uid);
          if (c && c.ws.readyState === 1) c.ws.send(patchStr);
        }
      }
      break;
    }
    case "presence": {
      // Notify friends of this player that are on THIS machine — O(friends) via reverse index
      const _presenceUsername = ev.username.toLowerCase();
      const msg = { type: ev.online ? "friend_online" : "friend_offline", name: ev.username, zone: ev.zone || null };
      const watchers = _friendsOfMe.get(_presenceUsername);
      if (watchers) {
        for (const watcherUid of watchers) {
          const c = clients.get(watcherUid);
          if (c && c.ws.readyState === 1) send(c.ws, msg);
        }
      }
      // Also update _usernameToUid so this machine knows the user is online elsewhere
      if (ev.online) {
        _usernameToUid.set(ev.username.toLowerCase(), ev.uid);
      } else {
        if (_usernameToUid.get(ev.username.toLowerCase()) === ev.uid) {
          _usernameToUid.delete(ev.username.toLowerCase());
        }
      }
      break;
    }
    case "broadcast":
      _broadcastToAll(ev.obj);
      break;
    case "item_delta_zoned": {
      // Cross-machine zone-scoped item delta. Each machine filters for its own
      // connected players, same logic as _broadcastItemDelta's local loop.
      if (!ev.upsertedItems) break;
      const _baseZd = {
        type: "catalog_delta",
        itemsV: ev.itemsV, zonesV: ev.zonesV, hostilesV: ev.hostilesV, actionsV: ev.actionsV,
        deletedItems: ev.deletedItems || [], upsertedZones: [], deletedZones: [],
        upsertedHostiles: [], deletedHostiles: [],
      };
      // Build zone->items map for this batch
      const _zdByZone = new Map();
      for (const item of ev.upsertedItems) {
        if (!item.zones || !item.zones.length) continue;
        for (const zid of item.zones) {
          if (!_zdByZone.has(zid)) _zdByZone.set(zid, []);
          _zdByZone.get(zid).push(item);
        }
      }
      for (const client of clients.values()) {
        if (!client.ws || client.ws.readyState !== 1) continue;
        const zoneItems = client.zone ? (_zdByZone.get(client.zone) || []) : [];
        if (!zoneItems.length && !(ev.deletedItems && ev.deletedItems.length)) continue;
        client.ws.send(JSON.stringify({ ..._baseZd, upsertedItems: zoneItems }));
      }
      break;
    }
    case "item_delta_owned": {
      // Cross-machine ownership-scoped item delta.
      // Intentionally reads live saves rather than _itemOwners here: the index
      // on this machine may be stale for players whose saves were mutated on the
      // originating machine (e.g. during start_combat _applyFixes). Reading the
      // cached save is the authoritative source of truth for cross-machine delivery.
      // The local path (_broadcastItemToOwners) uses _itemOwners for O(1) speed
      // since it runs on the same machine that performed the mutation.
      if (!ev.slimItems && !ev.deletedIds) break;
      const _ownSlim    = ev.slimItems  || [];
      const _ownDel     = ev.deletedIds || [];
      const _ownUpdated = new Set(_ownSlim.map(i => i.id));
      const _ownChanged = new Set([..._ownUpdated, ..._ownDel]);
      for (const [uid, client] of clients.entries()) {
        if (!client.ws || client.ws.readyState !== 1) continue;
        const save = _getCachedSave(uid);
        if (!save?.player) continue;
        const p = save.player;
        const owned = new Set();
        const eq = p.equipment || {};
        if (eq.gear?.id)  owned.add(eq.gear.id);
        (eq.accessories || []).forEach(a => { if (a?.id) owned.add(a.id); });
        const inv = p.inventory || {};
        ['gears','accessories','provisions','materials'].forEach(key => {
          (inv[key] || []).forEach(it => { if (it?.id) owned.add(it.id); });
        });
        (p.learnedActions || []).forEach(id => owned.add(id));
        if (![..._ownChanged].some(id => owned.has(id))) continue;
        const playerZone    = client.zone;
        const zoneItemIds   = playerZone ? (_zoneItemIndex.get(playerZone) || new Set()) : new Set();
        const globalItemIds = _zoneItemIndex.get('__global__') || new Set();
        const toSend   = _ownSlim.filter(i => owned.has(i.id) && !(zoneItemIds.has(i.id) || globalItemIds.has(i.id)));
        const toDelete = _ownDel.filter(id => owned.has(id));
        if (!toSend.length && !toDelete.length) continue;
        client.ws.send(JSON.stringify({
          type: "catalog_delta",
          itemsV: ev.itemsV, zonesV: ev.zonesV, hostilesV: ev.hostilesV, actionsV: ev.actionsV,
          upsertedItems: toSend, deletedItems: toDelete,
          upsertedZones: [], deletedZones: [], upsertedHostiles: [], deletedHostiles: [],
          _slim: true,
        }));
      }
      break;
    }
    case "hostile_delta_zoned": {
      // Cross-machine zone-scoped hostile delta.
      if (!ev.upsertedHostiles) break;
      const _hBaseZd = {
        type: "catalog_delta",
        itemsV: ev.itemsV, zonesV: ev.zonesV, hostilesV: ev.hostilesV, actionsV: ev.actionsV,
        deletedHostiles: [], upsertedZones: [], deletedZones: [],
        upsertedItems: [], deletedItems: [],
      };
      // O(1) reverse-index lookup
      const _hzdByZone = new Map();
      for (const hostile of ev.upsertedHostiles) {
        const zones = _hostileToZones.get(hostile.id);
        if (!zones) continue;
        for (const zid of zones) {
          if (!_hzdByZone.has(zid)) _hzdByZone.set(zid, []);
          _hzdByZone.get(zid).push(hostile);
        }
      }
      for (const client of clients.values()) {
        if (!client.ws || client.ws.readyState !== 1) continue;
        const zoneHostiles = client.zone ? (_hzdByZone.get(client.zone) || []) : [];
        if (!zoneHostiles.length) continue;
        client.ws.send(JSON.stringify({ ..._hBaseZd, upsertedHostiles: zoneHostiles }));
      }
      break;
    }
    case "combat_msg": {
      // Route a combat message (combat_start, combat_end, full_state, fled) to specific uids on this machine
      if (!ev.uids || !ev.msg) break;
      const str = JSON.stringify(ev.msg);
      for (const uid of ev.uids) {
        const c = clients.get(uid);
        if (c && c.ws.readyState === 1) c.ws.send(str);
      }
      break;
    }
    case "combat_tick": {
      // Route a compact combat tick to specific uids on this machine
      if (!ev.uids || !ev.tick) break;
      const str = JSON.stringify(ev.tick);
      for (const uid of ev.uids) {
        const c = clients.get(uid);
        if (c && c.ws.readyState === 1) c.ws.send(str);
      }
      break;
    }
    case "combat_action": {
      // A party member on another machine sent an action — find the room here and handle it
      if (!ev.uid || !ev.msg) break;
      const actionRoom = findRoomForUid(ev.uid);
      if (actionRoom && !actionRoom.ended) actionRoom.handleAction(ev.uid, ev.msg);
      break;
    }
    case "combat_flee": {
      if (!ev.uid) break;
      const fleeRoom = findRoomForUid(ev.uid);
      if (fleeRoom && !fleeRoom.ended) fleeRoom.handleFlee(ev.uid);
      break;
    }
  }
}

// Publish wrappers — deliver locally AND fan out via Redis
function _pub(ev) {
  ev._src = MY_MACHINE_ID;
  _redisPublish(ev);
}

// ── JWT helpers (lightweight, no external dependency) ─────────────────────────
const JWT_SECRET = process.env.JWT_SECRET || (() => {
  console.error("[CRITICAL] JWT_SECRET not set!");
  return crypto.randomBytes(32).toString("hex");
})();
const JWT_EXPIRY_S = 86400 * 30; // 30 days

// Reserved usernames — checked on every register call, hoisted for efficiency

// Whitelisted save paths the client is permitted to write via patch_save
// Prototype pollution keys blocked in patch_save path traversal
const DANGEROUS_KEYS = new Set(["__proto__", "constructor", "prototype"]);
const SAFE_FIELDS = new Set([
  // Short keys (current)
  "p/nm","p/mhp","p/as","p/la","p/rz","p/cd","p/lz","p/ls","p/en","p/ia","p/pt",
  "pce","ece","fce","tce","pid","cc","sc",
  // Long keys (legacy fallback — remove after all clients updated)
  "player/name","player/baseMaxHp","player/actionSlots","player/learnedActions",
  "player/respawnZone","player/cooldowns","player/lastZone","player/lastSeen",
  "player/energy","player/isAlive","player/totalPlaytime","charCreated","provisionCooldownEnd",
  "exploreCooldownEnd","fishCooldownEnd","travelCooldownEnd","partyId","savedCombat",
]);
const RESERVED_NAMES = new Set(["admin","administrator","moderator","mod","system","server","gm","gamemaster","projectvoid","support"]);

function _jwtSign(payload) {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify({ ...payload, iat: Math.floor(Date.now()/1000), exp: Math.floor(Date.now()/1000) + JWT_EXPIRY_S })).toString("base64url");
  const sig = crypto.createHmac("sha256", JWT_SECRET).update(header + "." + body).digest("base64url");
  return header + "." + body + "." + sig;
}
function _jwtVerify(token) {
  try {
    const [header, body, sig] = token.split(".");
    const expectedSig = crypto.createHmac("sha256", JWT_SECRET).update(header + "." + body).digest("base64url");
    if (sig !== expectedSig) return null;
    const payload = JSON.parse(Buffer.from(body, "base64url").toString());
    if (payload.exp && payload.exp < Math.floor(Date.now()/1000)) return null;
    return payload;
  } catch { return null; }
}

// ── Password hashing (bcrypt-like using scrypt - no native addon needed) ─────
function _hashPassword(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");
  return salt + ":" + hash;
}
function _verifyPassword(password, stored) {
  const [salt, hash] = stored.split(":");
  if (!salt || !hash) return false;
  const check = crypto.scryptSync(password, salt, 64).toString("hex");
  return check === hash;
}


// ── SQLite init ──────────────────────────────────────────────────────────────
const DB_PATH = process.env.DB_PATH || path.join(__dirname, "projectvoid.db");
const DB_DIR = path.dirname(DB_PATH);
if (!fs.existsSync(DB_DIR)) { fs.mkdirSync(DB_DIR, { recursive: true }); }
const db = new Database(DB_PATH, { verbose: process.env.DB_VERBOSE ? console.log : undefined });
console.log(`[DB] ${DB_PATH}`);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("foreign_keys = ON");
// Periodic passive WAL checkpoint — keeps WAL file bounded without blocking writers.
// PASSIVE mode: flushes what it can without waiting; never stalls the process.
setInterval(() => {
  try { db.pragma("wal_checkpoint(PASSIVE)"); }
  catch (e) { console.error("[WAL] checkpoint error:", e.message); }
}, 5 * 60 * 1000 + Math.floor(Math.random() * 5000)); // every 5 min + up to 5s jitter (avoids colliding with 10s stock flush)

// ── Create tables ────────────────────────────────────────────────────────────
db.exec(`
  -- User accounts: login credentials mapping
  CREATE TABLE IF NOT EXISTS accounts (
    username TEXT PRIMARY KEY,
    uid      TEXT,
    zone     TEXT,
    charName TEXT,
    hash     TEXT,
    created  INTEGER DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_accounts_uid ON accounts(uid);

  -- Player saves: full game state (JSON blob for flexibility)
  CREATE TABLE IF NOT EXISTS saves (
    uid      TEXT PRIMARY KEY,
    data     TEXT NOT NULL DEFAULT '{}'
  );

  -- Character name index: public lookup
  CREATE TABLE IF NOT EXISTS charnames (
    name_lower TEXT PRIMARY KEY,
    username   TEXT,
    uid        TEXT
  );

  -- Social data: friends, pending requests, notifications (JSON blob)
  CREATE TABLE IF NOT EXISTS social (
    uid  TEXT PRIMARY KEY,
    data TEXT NOT NULL DEFAULT '{}'
  );

  -- Guilds: one row per guild
  CREATE TABLE IF NOT EXISTS guilds (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    name_lower TEXT NOT NULL UNIQUE,
    leader_uid TEXT NOT NULL,
    data       TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_guilds_leader ON guilds(leader_uid);

  -- Guild members: one row per (guild, member)
  CREATE TABLE IF NOT EXISTS guild_members (
    guild_id   TEXT NOT NULL,
    uid        TEXT NOT NULL,
    username   TEXT NOT NULL,
    char_name  TEXT NOT NULL DEFAULT '',
    role       TEXT NOT NULL DEFAULT 'member',
    joined_at  INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, uid)
  );
  CREATE INDEX IF NOT EXISTS idx_gm_uid ON guild_members(uid);

  -- Public profiles: visible to other players
  CREATE TABLE IF NOT EXISTS profiles (
    uid  TEXT PRIMARY KEY,
    data TEXT NOT NULL DEFAULT '{}'
  );

  -- Inbox: friend requests, acks, removals, cancels, party invites, pings
  -- Each entry is a separate row for efficient per-key operations
  CREATE TABLE IF NOT EXISTS inbox (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    target_uid TEXT NOT NULL,
    category   TEXT NOT NULL,  -- 'freqs', 'facks', 'frems', 'fcancels', 'invites', 'ppings', 'system'
    entry_key  TEXT NOT NULL,
    data       TEXT,
    created_at INTEGER DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_inbox_target ON inbox(target_uid, category);
  CREATE UNIQUE INDEX IF NOT EXISTS idx_inbox_unique ON inbox(target_uid, category, entry_key);

  -- Direct messages
  CREATE TABLE IF NOT EXISTS dms (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    target_uid TEXT NOT NULL,
    entry_key  TEXT NOT NULL,
    sender     TEXT,
    sender_name TEXT,
    message    TEXT,
    created_at INTEGER DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_dms_target ON dms(target_uid);

  -- Party state (JSON blob - replaces pv/party/<partyId>)
  CREATE TABLE IF NOT EXISTS parties (
    party_id     TEXT PRIMARY KEY,
    data         TEXT NOT NULL DEFAULT '{}',
    updated_at   INTEGER DEFAULT 0,
    member_count INTEGER DEFAULT 0
  );

  -- Party votes
  CREATE TABLE IF NOT EXISTS party_votes (
    party_id TEXT NOT NULL,
    username TEXT NOT NULL,
    vote     TEXT,
    PRIMARY KEY (party_id, username)
  );

  -- Zone chat messages
  CREATE TABLE IF NOT EXISTS zonechat (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_id TEXT NOT NULL,
    msg_key TEXT NOT NULL,
    name    TEXT,
    message TEXT,
    uid     TEXT,
    created_at INTEGER DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_zonechat_zone ON zonechat(zone_id, created_at);

  -- Presence tracking
  CREATE TABLE IF NOT EXISTS presence (
    username TEXT PRIMARY KEY,
    t        INTEGER DEFAULT 0,
    u        INTEGER DEFAULT 0,
    z        TEXT
  );

  -- Zone presence (who's in which zone)
  CREATE TABLE IF NOT EXISTS zone_presence (
    username TEXT NOT NULL,
    zone_id  TEXT NOT NULL,
    t        INTEGER DEFAULT 0,
    PRIMARY KEY (username, zone_id)
  );

  -- Anomaly flags
  CREATE TABLE IF NOT EXISTS anomalies (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    uid     TEXT NOT NULL,
    reason  TEXT,
    details TEXT,
    ts      INTEGER DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_anomalies_uid ON anomalies(uid);

  -- Auth credentials
  CREATE TABLE IF NOT EXISTS auth_credentials (
    uid       TEXT PRIMARY KEY,
    username  TEXT UNIQUE NOT NULL,
    password  TEXT NOT NULL,
    created   INTEGER DEFAULT 0,
    must_reset INTEGER DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_auth_username ON auth_credentials(username);

  -- Save snapshots: automatic backups before destructive operations
  CREATE TABLE IF NOT EXISTS save_snapshots (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    uid     TEXT NOT NULL,
    reason  TEXT NOT NULL,
    data    TEXT NOT NULL,
    ip      TEXT DEFAULT 'unknown',
    device_id   TEXT DEFAULT 'unknown',
    fingerprint TEXT DEFAULT 'unknown',
    ts      INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_snapshots_uid ON save_snapshots(uid, ts);

  -- Transaction log: buy, sell, equip, loot, admin actions
  CREATE TABLE IF NOT EXISTS transaction_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    uid     TEXT NOT NULL,
    action  TEXT NOT NULL,
    details TEXT NOT NULL DEFAULT '{}',
    ts      INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_txlog_uid ON transaction_log(uid, ts);

  -- Combat room persistence: saved on SIGTERM, restored on boot, deleted when room ends normally
  CREATE TABLE IF NOT EXISTS combat_rooms (
    party_id     TEXT PRIMARY KEY,
    room_data    TEXT NOT NULL,
    saved_at     INTEGER NOT NULL,
    restored_at  INTEGER
  );
`);

// Migration: add must_reset column to existing auth_credentials tables
try { db.exec("ALTER TABLE auth_credentials ADD COLUMN must_reset INTEGER DEFAULT 0"); } catch(e) { /* column already exists */ }
// Migration: add ip column to existing save_snapshots tables
try { db.exec("ALTER TABLE save_snapshots ADD COLUMN ip TEXT DEFAULT 'unknown'"); } catch(e) { /* column already exists */ }
// Migration: add device_id and fingerprint columns to existing save_snapshots tables
try { db.exec("ALTER TABLE save_snapshots ADD COLUMN device_id TEXT DEFAULT 'unknown'"); } catch(e) { /* column already exists */ }
try { db.exec("ALTER TABLE save_snapshots ADD COLUMN fingerprint TEXT DEFAULT 'unknown'"); } catch(e) { /* column already exists */ }
// Migration: add member_count column to existing parties tables
try { db.exec("ALTER TABLE parties ADD COLUMN member_count INTEGER DEFAULT 0"); } catch(e) { /* column already exists */ }
// Migration: add hash column to profiles for delta delivery
try { db.exec("ALTER TABLE profiles ADD COLUMN hash TEXT DEFAULT ''"); } catch(e) { /* column already exists */ }

// Migration: rename category 'weapons' → 'gears' in dev_entities, and patch type/equipType
// in the stored JSON data field. Safe to run every boot — only affects rows that still
// use the old name.
try {
  const _weaponRows = db.prepare("SELECT rowid, id, data FROM dev_entities WHERE category='weapons'").all();
  if (_weaponRows.length > 0) {
    const _patchWeaponRow = db.prepare("UPDATE dev_entities SET category='gears', data=? WHERE rowid=?");
    const _migrateWeapons = db.transaction(() => {
      for (const row of _weaponRows) {
        let d = row.data;
        try {
          const parsed = JSON.parse(d);
          // Patch type and equipType fields inside the JSON blob
          if (parsed.type === 'weapon')     parsed.type     = 'gear';
          if (parsed.equipType === 'weapon') parsed.equipType = 'gear';
          if (parsed.weaponType !== undefined && parsed.gearType === undefined) {
            parsed.gearType = parsed.weaponType;
            delete parsed.weaponType;
          }
          d = JSON.stringify(parsed);
        } catch(e) { /* leave data as-is if JSON is malformed */ }
        _patchWeaponRow.run(d, row.rowid);
      }
    });
    _migrateWeapons();
    console.log(`[MIGRATE] weapons→gears: patched ${_weaponRows.length} dev_entities row(s).`);
  }
} catch(e) { console.error('[MIGRATE] weapons→gears failed:', e.message); }

// ── Market Stock (player-driven economy for materials & provisions) ────────────
// Creates table if not exists (migration-safe)
db.exec(`
  CREATE TABLE IF NOT EXISTS market_stock (
    item_id TEXT PRIMARY KEY,
    qty     INTEGER NOT NULL DEFAULT 0
  );
`);
// In-memory stock map — authoritative during runtime, persisted to SQLite on change
const MARKET_STOCK = new Map();
// Prepare inline here — needed before the shared stmt block is initialised
let stmtGetAllMarketStock;
try { stmtGetAllMarketStock = db.prepare('SELECT item_id, qty FROM market_stock'); } catch(e) { console.error('[MARKET] stmt prepare failed:', e.message); }
// Load persisted stock into memory at boot
(function _loadMarketStock(){
  try {
    const rows = stmtGetAllMarketStock.all();
    for (const r of rows) MARKET_STOCK.set(r.item_id, r.qty || 0);
    console.log(`[MARKET] Loaded ${MARKET_STOCK.size} stock entries from DB`);
  } catch(e) { console.error("[MARKET] Failed to load stock:", e.message); }
})();
const _stmtUpsertStock = db.prepare("INSERT INTO market_stock (item_id, qty) VALUES (?, ?) ON CONFLICT(item_id) DO UPDATE SET qty=excluded.qty");
// Dirty set tracks items whose stock changed since the last flush.
// In-memory MARKET_STOCK is always authoritative at runtime; DB is only for restart recovery.
// Flushing every 10s instead of per-call avoids a synchronous write on every buy/sell.
const _stockDirty = new Set();
function _setStock(itemId, qty) {
  const safeQty = Math.max(0, qty);
  MARKET_STOCK.set(itemId, safeQty);
  _stockDirty.add(itemId);
}
setInterval(() => {
  if (!_stockDirty.size) return;
  const flush = db.transaction(() => {
    for (const itemId of _stockDirty) {
      try { _stmtUpsertStock.run(itemId, MARKET_STOCK.get(itemId) || 0); } catch(e) { console.error("[MARKET] stock flush error:", e.message); }
    }
  });
  try { flush(); _stockDirty.clear(); } catch(e) { console.error("[MARKET] stock flush transaction error:", e.message); }
}, 10000); // flush every 10 seconds
function _getStock(itemId) { return MARKET_STOCK.get(itemId) || 0; }

// ── Prepared statements (performance) ────────────────────────────────────────
const stmt = {
  // Accounts
  getAccount:     db.prepare("SELECT * FROM accounts WHERE username = ?"),
  getAccountByUid: db.prepare("SELECT username FROM accounts WHERE uid = ?"),
  upsertAccount:  db.prepare("INSERT INTO accounts (username, uid, zone, charName, hash, created) VALUES (@username, @uid, @zone, @charName, @hash, @created) ON CONFLICT(username) DO UPDATE SET uid=@uid, zone=@zone, charName=@charName, hash=@hash"),
  updateAccountZone: db.prepare("UPDATE accounts SET zone = ? WHERE username = ?"),

  // Saves
  getSave:        db.prepare("SELECT data FROM saves WHERE uid = ?"),
  upsertSave:     db.prepare("INSERT INTO saves (uid, data) VALUES (?, ?) ON CONFLICT(uid) DO UPDATE SET data=excluded.data"),
  deleteSave:     db.prepare("DELETE FROM saves WHERE uid = ?"),

  // Charnames
  getCharname:    db.prepare("SELECT * FROM charnames WHERE name_lower = ?"),
  upsertCharname: db.prepare("INSERT INTO charnames (name_lower, username, uid) VALUES (?, ?, ?) ON CONFLICT(name_lower) DO UPDATE SET username=excluded.username, uid=excluded.uid"),

  // Social
  getSocial:      db.prepare("SELECT data FROM social WHERE uid = ?"),
  upsertSocial:   db.prepare("INSERT INTO social (uid, data) VALUES (?, ?) ON CONFLICT(uid) DO UPDATE SET data=excluded.data"),

  // Profiles
  getProfile:     db.prepare("SELECT data, hash FROM profiles WHERE uid = ?"),
  upsertProfile:  db.prepare("INSERT INTO profiles (uid, data, hash) VALUES (?, ?, ?) ON CONFLICT(uid) DO UPDATE SET data=excluded.data, hash=excluded.hash"),

  // Inbox
  getInbox:       db.prepare("SELECT * FROM inbox WHERE target_uid = ?"),
  upsertInbox:    db.prepare("INSERT INTO inbox (target_uid, category, entry_key, data, created_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(target_uid, category, entry_key) DO UPDATE SET data=excluded.data, created_at=excluded.created_at"),
  deleteInbox:    db.prepare("DELETE FROM inbox WHERE target_uid = ? AND category = ? AND entry_key = ?"),
  deleteInboxCat: db.prepare("DELETE FROM inbox WHERE target_uid = ? AND category = ?"),
  deleteInboxAll: db.prepare("DELETE FROM inbox WHERE target_uid = ?"),

  // DMs
  getDms:         db.prepare("SELECT * FROM dms WHERE target_uid = ? ORDER BY created_at ASC"),
  insertDm:       db.prepare("INSERT INTO dms (target_uid, entry_key, sender, sender_name, message, created_at) VALUES (?, ?, ?, ?, ?, ?)"),
  deleteDm:       db.prepare("DELETE FROM dms WHERE target_uid = ? AND entry_key = ?"),
  deleteDmsAll:   db.prepare("DELETE FROM dms WHERE target_uid = ?"),

  // Parties
  getParty:       db.prepare("SELECT data FROM parties WHERE party_id = ?"),
  upsertParty:    db.prepare("INSERT INTO parties (party_id, data, updated_at, member_count) VALUES (?, ?, ?, ?) ON CONFLICT(party_id) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at, member_count=excluded.member_count"),
  deleteParty:    db.prepare("DELETE FROM parties WHERE party_id = ?"),
  getStaleParties:db.prepare("SELECT party_id, data FROM parties WHERE updated_at < ?"),
  getSoloParties: db.prepare("SELECT party_id, updated_at FROM parties WHERE member_count = 1"),

  // Party votes
  setVote:        db.prepare("INSERT INTO party_votes (party_id, username, vote) VALUES (?, ?, ?) ON CONFLICT(party_id, username) DO UPDATE SET vote=excluded.vote"),
  getVotes:       db.prepare("SELECT username, vote FROM party_votes WHERE party_id = ?"),
  deleteVotes:    db.prepare("DELETE FROM party_votes WHERE party_id = ?"),

  // Zone chat
  insertZoneChat: db.prepare("INSERT INTO zonechat (zone_id, msg_key, name, message, uid, created_at) VALUES (?, ?, ?, ?, ?, ?)"),
  pruneZoneChat:  db.prepare("DELETE FROM zonechat WHERE created_at < ?"),

  // Anomalies
  insertAnomaly:  db.prepare("INSERT INTO anomalies (uid, reason, details, ts) VALUES (?, ?, ?, ?)"),
  getAllAnomalies: db.prepare("SELECT uid, reason, details, ts FROM anomalies ORDER BY ts DESC LIMIT 50000"),
  deleteAnomalies:db.prepare("DELETE FROM anomalies WHERE uid = ?"),

  // Auth credentials
  getAuthByUsername: db.prepare("SELECT * FROM auth_credentials WHERE username = ?"),
  insertAuth:       db.prepare("INSERT INTO auth_credentials (uid, username, password, created) VALUES (?, ?, ?, ?)"),
  updateAuthPassword: db.prepare("UPDATE auth_credentials SET password = ?, must_reset = 0 WHERE uid = ?"),
  updateAuthPasswordKeepReset: db.prepare("UPDATE auth_credentials SET password = ? WHERE uid = ?"),
  setMustReset:     db.prepare("UPDATE auth_credentials SET must_reset = 1, password = ? WHERE uid = ?"),

  // Save snapshots
  insertSnapshot:   db.prepare("INSERT INTO save_snapshots (uid, reason, data, ip, device_id, fingerprint, ts) VALUES (?, ?, ?, ?, ?, ?, ?)"),
  getSnapshots:     db.prepare("SELECT id, reason, ip, device_id, fingerprint, ts FROM save_snapshots WHERE uid = ? ORDER BY ts DESC LIMIT 20"),
  getSnapshotById:  db.prepare("SELECT data FROM save_snapshots WHERE id = ? AND uid = ?"),
  getLatestSnapshot:db.prepare("SELECT id, data, reason, ts FROM save_snapshots WHERE uid = ? ORDER BY ts DESC LIMIT 1"),
  pruneSnapshots:   db.prepare("DELETE FROM save_snapshots WHERE ts < ?"),
  trimSnapshotsForUser: db.prepare("DELETE FROM save_snapshots WHERE uid = ? AND id NOT IN (SELECT id FROM save_snapshots WHERE uid = ? ORDER BY ts DESC LIMIT 50)"),

  // Transaction log
  insertTxLog:      db.prepare("INSERT INTO transaction_log (uid, action, details, ts) VALUES (?, ?, ?, ?)"),
  getTxLog:         db.prepare("SELECT action, details, ts FROM transaction_log WHERE uid = ? ORDER BY ts DESC LIMIT ?"),
  pruneTxLog:       db.prepare("DELETE FROM transaction_log WHERE ts < ?"),

  // Combat room persistence
  saveCombatRoom:   db.prepare("INSERT INTO combat_rooms (party_id, room_data, saved_at) VALUES (?, ?, ?) ON CONFLICT(party_id) DO UPDATE SET room_data=excluded.room_data, saved_at=excluded.saved_at, restored_at=NULL"),
  getAllCombatRooms: db.prepare("SELECT party_id, room_data, saved_at, restored_at FROM combat_rooms"),
  markCombatRoomRestored: db.prepare("UPDATE combat_rooms SET restored_at=? WHERE party_id=?"),
  deleteCombatRoom: db.prepare("DELETE FROM combat_rooms WHERE party_id=?"),

  // Guild statements
  getGuild:           db.prepare("SELECT * FROM guilds WHERE id = ?"),
  getGuildByName:     db.prepare("SELECT * FROM guilds WHERE name_lower = ?"),
  insertGuild:        db.prepare("INSERT INTO guilds (id, name, name_lower, leader_uid, data, created_at) VALUES (?, ?, ?, ?, ?, ?)"),
  updateGuildData:    db.prepare("UPDATE guilds SET data = ? WHERE id = ?"),
  updateGuildLeader:  db.prepare("UPDATE guilds SET leader_uid = ? WHERE id = ?"),
  deleteGuild:        db.prepare("DELETE FROM guilds WHERE id = ?"),
  getGuildMembers:    db.prepare("SELECT * FROM guild_members WHERE guild_id = ? ORDER BY role ASC, char_name ASC"),
  getGuildMember:     db.prepare("SELECT * FROM guild_members WHERE guild_id = ? AND uid = ?"),
  getMemberGuild:     db.prepare("SELECT guild_id, role FROM guild_members WHERE uid = ?"),
  insertGuildMember:  db.prepare("INSERT OR IGNORE INTO guild_members (guild_id, uid, username, char_name, role, joined_at) VALUES (?, ?, ?, ?, ?, ?)"),
  updateGuildRole:    db.prepare("UPDATE guild_members SET role = ? WHERE guild_id = ? AND uid = ?"),
  updateGuildCharName:db.prepare("UPDATE guild_members SET char_name = ? WHERE guild_id = ? AND uid = ?"),
  deleteGuildMember:  db.prepare("DELETE FROM guild_members WHERE guild_id = ? AND uid = ?"),
  countGuildMembers:  db.prepare("SELECT COUNT(*) as cnt FROM guild_members WHERE guild_id = ?"),
  countGuildOfficers: db.prepare("SELECT COUNT(*) as cnt FROM guild_members WHERE guild_id = ? AND role = 'officer'"),

  // Full account purge
  deleteAccount:        db.prepare("DELETE FROM accounts WHERE username = ?"),
  deleteCharname:       db.prepare("DELETE FROM charnames WHERE uid = ?"),
  deleteSocial:         db.prepare("DELETE FROM social WHERE uid = ?"),
  deleteProfile:        db.prepare("DELETE FROM profiles WHERE uid = ?"),
  deletePresence:       db.prepare("DELETE FROM presence WHERE username = ?"),
  deleteZonePresenceAll:db.prepare("DELETE FROM zone_presence WHERE username = ?"),
  deleteAuth:           db.prepare("DELETE FROM auth_credentials WHERE uid = ?"),
  deleteSnapshots:      db.prepare("DELETE FROM save_snapshots WHERE uid = ?"),
  deleteTxLog:          db.prepare("DELETE FROM transaction_log WHERE uid = ?"),
  deleteZoneChat:       db.prepare("DELETE FROM zonechat WHERE uid = ?"),
};

// ── Prepared statements for single-entity delta rebuilds ─────────────────────
// Hoisted to module level so db.prepare() compiles once, not on every approve/delete call.
const _stmtGetDevEntity        = db.prepare("SELECT id, name, data FROM dev_entities WHERE id=? AND status='live'");
const _stmtGetDevEntityHostile = db.prepare("SELECT id, name, data FROM dev_entities WHERE id=? AND (category='hostile' OR category='boss') AND status='live'");

// ── SQLite helper: get save as parsed object ─────────────────────────────────
function dbGetSave(uid) {
  const row = stmt.getSave.get(uid);
  if (!row) return null;
  try { return JSON.parse(row.data); } catch { return null; }
}
function dbSetSave(uid, save) {
  stmt.upsertSave.run(uid, JSON.stringify(save));
}

// ── Save snapshots — automatic backup before destructive operations ──────────
const SNAPSHOT_MAX_AGE_MS = 30 * 24 * 3600 * 1000; // 30 days exactly

function _snapshotSave(uid, reason) {
  try {
    const save = _getCachedSave(uid);
    if (!save) return;
    // Serialize immediately — callers mutate the save object after this call,
    // so we must capture the string now to guarantee a pre-mutation snapshot.
    const data = JSON.stringify(save);
    const client = clients.get(uid);
    const ip = client?.ip || client?.ws?._ip || "offline";
    const deviceId = client?.deviceId || "unknown";
    const fingerprint = client?.fingerprint || "unknown";
    stmt.insertSnapshot.run(uid, reason, data, ip, deviceId, fingerprint, Date.now());
    // Keep at most 50 snapshots per user — age-based prune alone lets heavy
    // traders accumulate thousands within the 30-day window.
    stmt.trimSnapshotsForUser.run(uid, uid);
  } catch (e) { console.error("[SNAPSHOT] error:", e.message); }
}

// ── Transaction log — tracks buy, sell, equip, loot, admin actions ───────────
const TXLOG_MAX_AGE_MS = 30 * 24 * 3600 * 1000; // 30 days exactly

function _logTx(uid, action, details) {
  try {
    const client = clients.get(uid);
    const ip = client?.ip || client?.ws?._ip || "offline";
    const deviceId = client?.deviceId || "";
    const fingerprint = client?.fingerprint || "";
    const fullDetails = { ...details, ip, deviceId, fingerprint };
    stmt.insertTxLog.run(uid, action, JSON.stringify(fullDetails), Date.now());
  } catch (e) { console.error("[TXLOG] error:", e.message); }
}

// Prune old snapshots and txlog entries on startup and every 24h
const _stmtPruneSnapBatch = db.prepare("DELETE FROM save_snapshots WHERE id IN (SELECT id FROM save_snapshots WHERE ts < ? LIMIT 1000)");
const _stmtPruneTxBatch   = db.prepare("DELETE FROM transaction_log WHERE id IN (SELECT id FROM transaction_log WHERE ts < ? LIMIT 1000)");

function _pruneOldData() {
  // Defer off the event loop so in-flight WS messages aren't stalled.
  // Batched deletes (1000 rows at a time) keep each DB call short.
  setImmediate(() => {
    try {
      const snapCutoff = Date.now() - SNAPSHOT_MAX_AGE_MS;
      const txCutoff = Date.now() - TXLOG_MAX_AGE_MS;
      const gInvCutoff = Date.now() - GUILD_INVITE_TTL_MS;
      let snapTotal = 0, txTotal = 0;
      let r;
      do { r = _stmtPruneSnapBatch.run(snapCutoff); snapTotal += r.changes; } while (r.changes > 0);
      do { r = _stmtPruneTxBatch.run(txCutoff);     txTotal   += r.changes; } while (r.changes > 0);
      if (snapTotal || txTotal) console.log(`[PRUNE] snapshots=${snapTotal} txlog=${txTotal}`);
    } catch (e) { console.error("[PRUNE] error:", e.message); }
  });
}
_pruneOldData();
setInterval(_pruneOldData, 24 * 3600 * 1000);

// ── Catalog changelog pruning ─────────────────────────────────────────────────
// Keeps only the last 90 days of changelog rows. Since the changelog is only used
// to diff offline players back to current, rows older than 90 days are irrelevant —
// a player absent for 90+ days gets a full catalog download (cold-client path).
// Runs once at boot and then weekly. Each prune is a single indexed DELETE.
// Prepare inline here — needed before the shared stmt block is initialised
let stmtDeleteChangelog;
try { stmtDeleteChangelog = db.prepare('DELETE FROM catalog_changelog WHERE ts < ?'); } catch(e) { console.error('[CHANGELOG] stmt prepare failed:', e.message); }
function _pruneChangelog() {
  try {
    const cutoff = Date.now() - (90 * 24 * 3600 * 1000);
    const result = stmtDeleteChangelog.run(cutoff);
    if (result.changes > 0) {
      console.log(`[CHANGELOG] pruned ${result.changes} rows older than 90 days`);
    }
  } catch(e) { console.error('[CHANGELOG] prune error:', e.message); }
}
_pruneChangelog();
setInterval(_pruneChangelog, 7 * 24 * 3600 * 1000); // weekly
function dbGetSocial(uid) {
  const row = stmt.getSocial.get(uid);
  if (!row) return null;
  try { return JSON.parse(row.data); } catch { return null; }
}
function dbGetParty(partyId) {
  const row = stmt.getParty.get(partyId);
  if (!row) return null;
  try { return JSON.parse(row.data); } catch { return null; }
}
function dbSetParty(partyId, data) {
  const memberCount = Array.isArray(data.members) ? data.members.length : 0;
  stmt.upsertParty.run(partyId, JSON.stringify(data), Date.now(), memberCount);
}

// ── Party subscription map (uid → partyId) ────────────────────────────────────
// Server tracks which clients are subscribed to which party. On any party write,
// server pushes full doc to all subscribers. Survives reconnect via auth_ok re-subscribe.
const partySubscriptions = new Map(); // uid → partyId

function _pushPartyToSubscribers(partyId, excludeUid) {
  const partyData = dbGetParty(partyId);
  const str = partyData ? JSON.stringify({ type: "party_update", partyId, data: partyData }) : null;
  const nullStr = JSON.stringify({ type: "party_update", partyId, data: null });
  for (const [uid, pid] of partySubscriptions.entries()) {
    if (pid !== partyId) continue;
    if (excludeUid && uid === excludeUid) continue;
    const c = clients.get(uid);
    if (c && c.ws.readyState === 1) c.ws.send(partyData ? str : nullStr);
  }
  // Cross-machine relay (Redis)
  _pub({ t: "party", partyId, excl: excludeUid || null });
}

function _dissolveParty(partyId) {
  // Notify all subscribers then delete
  const nullStr = JSON.stringify({ type: "party_update", partyId, data: null });
  for (const [uid, pid] of partySubscriptions.entries()) {
    if (pid !== partyId) continue;
    const c = clients.get(uid);
    if (c && c.ws.readyState === 1) c.ws.send(nullStr);
    partySubscriptions.delete(uid);
  }
  stmt.deleteParty.run(partyId);
  stmt.deleteVotes.run(partyId);
  _pub({ t: "party", partyId, excl: null });
}
function dbGetProfile(uid) {
  const row = stmt.getProfile.get(uid);
  if (!row) return null;
  try { return JSON.parse(row.data); } catch { return null; }
}
// Fast FNV-1a 32-bit hash for profile delta detection
function _profileHash(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = (h * 16777619) >>> 0; }
  return h.toString(36);
}

// ── Build inbox object from rows ─────────────────────────────────────────────
function dbGetInboxObject(uid) {
  const rows = stmt.getInbox.all(uid);
  const inbox = {};
  for (const row of rows) {
    if (!inbox[row.category]) inbox[row.category] = {};
    try {
      inbox[row.category][row.entry_key] = row.data ? JSON.parse(row.data) : row.created_at;
    } catch {
      inbox[row.category][row.entry_key] = row.created_at;
    }
  }
  return inbox;
}

// ── Game constants (mirrors client) ──────────────────────────────────────────
// Only core actions are hardcoded. All player-learnable actions come from dev_entities.
const ACTION_DB = {
  basic_attack: { name:"Basic Attack", damage:[5,10], difficulty:20, cooldown:0, _effects:[] },
  flee:         { name:"Flee",         difficulty:35, isFlee:true,   _effects:[] },
  provisions:   { name:"Provision",    difficulty:0,  isProvisions:true, _effects:[] },
};

const ENEMY_DB = {
  // Enemies are loaded live from dev_entities at boot and on approval
};

// Rebuild ENEMY_DB from live hostile/boss dev_entities at boot and on approval
function _rebuildEnemiesFromDB() {
  try {
    // Remove any previously live-loaded entries so deleted/rejected entities don't persist
    for (const k of Object.keys(ENEMY_DB)) { if (ENEMY_DB[k]._live) delete ENEMY_DB[k]; }
    // Also remove previously compiled hostile action entries from ACTION_DB
    for (const k of Object.keys(ACTION_DB)) { if (ACTION_DB[k]._hostileAction) delete ACTION_DB[k]; }

    const rows = stmtGetLiveHostilesAndBosses.all();
    for (const row of rows) {
      try {
        const d = JSON.parse(row.data || '{}');
        const eid = d.id || row.id;
        if (!eid) continue;

        // Build actions list from _actions array — compile each into ACTION_DB
        const rawActions = d._actions || [];
        const actionIds = [];
        for (const a of rawActions) {
          const aName = (a.name || '').trim();
          if (!aName) continue;
          // Create a unique stable key per hostile so actions don't collide across enemies
          const aKey = `${eid}__${aName.toLowerCase().replace(/\s+/g,'_')}`;
          // Extract damage range from effects, or use defaults
          let dmgMin = 0, dmgMax = 0;
          const effects = Array.isArray(a._effects) ? a._effects : [];
          const dmgEffect = effects.find(e => e && (e.type === 'damage' || e.type === 'dot'));
          if (dmgEffect && Array.isArray(dmgEffect.value) && dmgEffect.value.length >= 2) {
            dmgMin = parseInt(dmgEffect.value[0]) || 0;
            dmgMax = parseInt(dmgEffect.value[1]) || 0;
          } else if (dmgEffect && dmgEffect.valueMin != null) {
            dmgMin = parseInt(dmgEffect.valueMin) || 0;
            dmgMax = parseInt(dmgEffect.valueMax) || 0;
          } else if (dmgEffect && dmgEffect.value != null) {
            dmgMin = dmgMax = parseInt(dmgEffect.value) || 0;
          }
          // Compile the action's effects, then strip out the *instant damage*
          // atom (if any) that we just used to derive the base damage range
          // above. Without this, an action whose damage is defined the normal
          // way — a single "damage" effect atom — would hit twice: once as
          // the base attack roll, and again when _evalEffects fires that same
          // atom on_hit. DoT atoms are intentionally left in _effects since
          // the base roll only borrows their value range for difficulty/accuracy
          // purposes — the lingering tick is a separate, non-duplicated effect.
          // Matched by source-object identity (not array position), since a
          // malformed/typeless effect entry elsewhere in the array could shift
          // indices between the raw and compiled lists.
          const compiledEffects = _compileEffects(a, true);
          const finalEffects = (dmgEffect && dmgEffect.type === 'damage')
            ? compiledEffects.filter(ce => ce._srcRaw !== dmgEffect)
            : compiledEffects;
          ACTION_DB[aKey] = {
            name:       aName,
            damage:     [dmgMin, dmgMax],
            difficulty: parseInt(a.difficulty) || 0,
            cooldown:   parseInt(a.cooldown)   || 0,
            _effects:   finalEffects,
            _hostileAction: true,
          };
          actionIds.push(aKey);
        }

        // Build loot list from _loot array
        const loot = (d._loot || []).map(l => {
          const ltype = (l.type || 'material').toLowerCase();
          let qty = [1, 1];
          if (ltype === 'gold') {
            // Gold: l.name holds min amount, l.max holds max
            const qMin = parseInt(l.name ?? l.min) || 0;
            const qMax = parseInt(l.max) || qMin;
            qty = [qMin, Math.max(qMin, qMax)];
          } else {
            // Material/item: read explicit qty range fields from dev panel
            const qMin = parseInt(l.qtyMin) || parseInt(l.qty) || 1;
            const qMax = parseInt(l.qtyMax) || parseInt(l.max) || qMin;
            qty = [qMin, Math.max(qMin, qMax)];
          }
          // Resolve item reference to canonical ITEM_DB key (name or ID)
          const rawRef = ltype === 'gold' ? '' : (l.id || l.item || l.name || '');
          const itemId = ltype === 'gold' ? '' : (_resolveItemKey(rawRef) || rawRef);
          return {
            type: ltype,
            item: itemId,
            qty,
            chance: (l.chance !== undefined && l.chance !== '') ? (parseFloat(l.chance) || 0) : 100,
          };
        });
        ENEMY_DB[eid] = {
          name:    d.name || row.name || eid,
          maxHp:   parseInt(d.health) || parseInt(d.hp) || 0,
          actions: actionIds.length ? actionIds : ['basic_attack'],
          loot,
          rarity:  d.rarity || 'common',
          _live:   true,
        };
        if (loot.length) console.log(`[ENEMIES] ${eid} loot: ${JSON.stringify(loot)}`);
      } catch(ee) { console.error('[ENEMIES] parse error:', row.id, ee.message); }
    }
    ENEMY_DB_VERSION = Date.now();
    ACTION_DB_VERSION  = Date.now();
    console.log(`[ENEMIES] catalog: ${Object.keys(ENEMY_DB).length} enemies v${ENEMY_DB_VERSION}`);
  } catch(e) { console.error('[ENEMIES] rebuild error:', e.message); }
}

// Returns the public-safe hostile catalog array (strips _live flag).
// NOTE: Dev-panel only (get_live_hostiles). All player-facing paths use
// _buildZoneHostiles() for zone-scoped delivery. Do NOT call in player code.
function _buildHostilesCatalog() {
  return Object.entries(ENEMY_DB)
    .filter(([, v]) => v._live)
    .map(([id, v]) => { const { _live, ...pub } = v; return { id, ...pub }; });
}

// Rebuild a single hostile/boss entity from the DB and return a delta descriptor.
// Returns { hostile, deleted } — hostile is the built entry, deleted is the id if removed.
function _rebuildOneHostile(entityId) {
  try {
    const row = _stmtGetDevEntityHostile.get(entityId);
    if (!row) {
      // entityId didn't match a live hostile directly — could be a draft key
      // (e.g. __draft_1234) passed instead of the real entity ID. Before
      // treating this as a deletion, verify the entity truly doesn't exist
      // under any live hostile ID. If a live hostile IS found, fall back to a
      // full rebuild so the in-memory catalog stays consistent.
      if (ENEMY_DB[entityId]) {
        // We had an entry for this exact key — safe to treat as deletion.
      } else {
        // entityId is not a known ENEMY_DB key. It may be a stale draft key
        // with no corresponding live row. Do NOT scrub anything — just trigger
        // a full rebuild to ensure the catalog is current.
        console.warn(`[ENEMIES] _rebuildOneHostile: no live row for "${entityId}" and not in ENEMY_DB — running full rebuild as fallback`);
        try { _rebuildEnemiesFromDB(); } catch(e) { console.error('[ENEMIES] fallback full rebuild error:', e.message); }
        return null;
      }
      // Confirmed deletion: entityId was a known key and is now gone.
      delete ENEMY_DB[entityId];
      for (const zid of Object.keys(ZONE_DB)) {
        const z = ZONE_DB[zid];
        if (Array.isArray(z.enemies) && z.enemies.includes(entityId)) {
          z.enemies = z.enemies.filter(e => e !== entityId);
          if (z.weights) delete z.weights[entityId];
        }
      }
      ENEMY_DB_VERSION = Date.now();
      return { deleted: entityId, hostile: null };
    }
    const d = JSON.parse(row.data || '{}');
    const eid = d.id || row.id;
    if (!eid) return null;
    // Detect ID rename: evict old key so stale entry doesn't linger
    const oldDeleted = (eid !== entityId && ENEMY_DB[entityId]) ? entityId : null;
    if (oldDeleted) {
      delete ENEMY_DB[entityId];
      // Also clean up old hostile's compiled actions
      for (const k of Object.keys(ACTION_DB)) {
        if (ACTION_DB[k]._hostileAction && k.startsWith(`${entityId}__`)) delete ACTION_DB[k];
      }
      console.log(`[ENEMY_DB] ID renamed: evicting old key "${entityId}" → "${eid}"`);
    }
    // Remove previously compiled hostile actions for this entity
    for (const k of Object.keys(ACTION_DB)) {
      if (ACTION_DB[k]._hostileAction && k.startsWith(`${eid}__`)) delete ACTION_DB[k];
    }
    // Compile actions
    const rawActions = d._actions || [];
    const actionIds = [];
    for (const a of rawActions) {
      const aName = (a.name || '').trim();
      if (!aName) continue;
      const aKey = `${eid}__${aName.toLowerCase().replace(/\s+/g, '_')}`;
      let dmgMin = 0, dmgMax = 0;
      const effects = Array.isArray(a._effects) ? a._effects : [];
      const dmgEffect = effects.find(e => e && (e.type === 'damage' || e.type === 'dot'));
      if (dmgEffect && Array.isArray(dmgEffect.value) && dmgEffect.value.length >= 2) {
        dmgMin = parseInt(dmgEffect.value[0]) || 0; dmgMax = parseInt(dmgEffect.value[1]) || 0;
      } else if (dmgEffect && dmgEffect.valueMin != null) {
        dmgMin = parseInt(dmgEffect.valueMin) || 0; dmgMax = parseInt(dmgEffect.valueMax) || 0;
      } else if (dmgEffect && dmgEffect.value != null) {
        dmgMin = dmgMax = parseInt(dmgEffect.value) || 0;
      }
      // Strip the instant-damage atom used for the base roll above so it
      // doesn't also fire a second time via _evalEffects on_hit. See the
      // matching comment in _rebuildEnemiesFromDB for the full rationale.
      const compiledActionEffects = _compileEffects(a, true);
      const finalActionEffects = (dmgEffect && dmgEffect.type === 'damage')
        ? compiledActionEffects.filter(ce => ce._srcRaw !== dmgEffect)
        : compiledActionEffects;
      ACTION_DB[aKey] = {
        name: aName, damage: [dmgMin, dmgMax],
        difficulty: parseInt(a.difficulty) || 0, cooldown: parseInt(a.cooldown) || 0,
        _effects: finalActionEffects, _hostileAction: true,
      };
      actionIds.push(aKey);
    }
    // Compile loot
    const loot = (d._loot || []).map(l => {
      const ltype = (l.type || 'material').toLowerCase();
      let qty = [1, 1];
      if (ltype === 'gold') {
        const qMin = parseInt(l.name ?? l.min)||0; const qMax = parseInt(l.max)||qMin; qty = [qMin, Math.max(qMin, qMax)];
      } else {
        const qMin = parseInt(l.qtyMin)||parseInt(l.qty)||1; const qMax = parseInt(l.qtyMax)||parseInt(l.max)||qMin; qty = [qMin, Math.max(qMin, qMax)];
      }
      const rawRef = ltype === 'gold' ? '' : (l.id || l.item || l.name || '');
      const itemId = ltype === 'gold' ? '' : (_resolveItemKey(rawRef) || rawRef);
      return { type: ltype, item: itemId, qty, chance: (l.chance !== undefined && l.chance !== '') ? (parseFloat(l.chance) || 0) : 100 };
    });
    ENEMY_DB[eid] = {
      name: d.name || row.name || eid,
      maxHp: parseInt(d.health) || parseInt(d.hp) || 0,
      actions: actionIds.length ? actionIds : ['basic_attack'],
      loot, rarity: d.rarity || 'common', _live: true,
    };
    ENEMY_DB_VERSION = Date.now();
    ACTION_DB_VERSION  = Date.now();
    const { _live, ...pub } = ENEMY_DB[eid];
    return { hostile: { id: eid, ...pub }, deleted: null, oldDeleted: oldDeleted || null };
  } catch(e) {
    console.error('[ENEMIES] delta rebuild error:', entityId, e.message);
    return null;
  }
}

const ZONE_DB = {
  // Zones are loaded live from dev_entities at boot and on approval
};
// ── Live zone catalog ─────────────────────────────────────────────────────────
// Merges hardcoded base zones with live approved zone entities from dev_entities.
// Call at boot and whenever a zone entity is approved.
// Resolves a loot item reference (name or ID) to the canonical ITEM_DB key.
// Tries direct ID match first, then name match. Returns null if unresolvable.
function _resolveItemKey(raw) {
  if (!raw) return null;
  if (ITEM_DB[raw]) return raw;
  const lower = raw.toLowerCase();
  for (const [k, v] of Object.entries(ITEM_DB)) {
    if ((v.name || '').toLowerCase() === lower) return k;
  }
  return null; // truly unknown — silently skip at drop time
}

// Resolves an assignment reference (which may be an ID or a display name) to
// the canonical ENEMY_DB key. Tries direct ID match first, then name match.
// Returns the resolved key if found in ENEMY_DB, otherwise the raw value.
function _resolveEnemyKey(raw) {
  if (!raw) return null;
  if (ENEMY_DB[raw]) return raw; // already a valid ID
  // Fall back to name match
  const lower = raw.toLowerCase();
  for (const [k, v] of Object.entries(ENEMY_DB)) {
    if ((v.name || '').toLowerCase() === lower) return k;
  }
  return raw; // unresolved — combat validator will filter it out
}

function _rebuildLiveZones() {
  try {
    // Remove previously live-loaded zone entries so deleted/rejected zones don't persist
    for (const k of Object.keys(ZONE_DB)) { if (ZONE_DB[k]._live) delete ZONE_DB[k]; }
    const liveRows = stmtGetLiveZones.all();
    for (const row of liveRows) {
      try {
        const d = JSON.parse(row.data || '{}');
        const zid = d.id || row.id;
        if (!zid) continue;
        const respawnRule = d.respawnRule || d.type || '';
        const isSafe = respawnRule === 'Safe';
        const subzones = Array.isArray(d._subzones) ? d._subzones : [];
        const hasFishing = subzones.some(sz => sz.type === 'Fishing');
        const assignments = Array.isArray(d._assignments) ? d._assignments : [];
        const enemies = assignments.map(a => _resolveEnemyKey(a.entity || a.name || a.id)).filter(Boolean);
        const weights = {};
        for (const a of assignments) {
          const key = _resolveEnemyKey(a.entity || a.name || a.id);
          if (key && a.spawn) weights[key] = parseFloat(a.spawn) || 1;
        }
        ZONE_DB[zid] = {
          ...(ZONE_DB[zid] || {}),
          name:        d.name  || row.name || zid,
          safe:        isSafe,
          enemies,
          weights:     Object.keys(weights).length ? weights : undefined,
          hasFishing,
          hasDungeon:  subzones.some(sz => sz.type === 'Dungeon'),
          hasRaid:     subzones.some(sz => sz.type === 'Raid'),
          hasTrial:    subzones.some(sz => sz.type === 'Trial'),
          subzones,
          market:      d._market  || null,
          rarity:      d.rarity   || 'common',
          description: d.description || '',
          zoneNumber:  d.zoneNumber != null ? Number(d.zoneNumber) : null,
          music:       d.music || '',
          background:  d.background || '',
          exploreMusic:  d.exploreMusic  || '',
          arenaMusic1v1: d.arenaMusic1v1 || '',
          arenaMusic2v2: d.arenaMusic2v2 || '',
          arenaMusic4v4: d.arenaMusic4v4 || '',
          arenaBackground: d.arenaBackground || '',
          _live: true,
        };
      } catch(ez) { console.error('[ZONE_DB] parse error:', row.id, ez.message); }
    }
  } catch(e) { console.error('[ZONE_DB] rebuild error:', e.message); }
  ZONE_DB_VERSION = Date.now();
  const totalEnemySlots = Object.values(ZONE_DB).reduce((s,z) => s + (z.enemies||[]).length, 0);
  const resolvedSlots   = Object.values(ZONE_DB).reduce((s,z) => s + (z.enemies||[]).filter(e => ENEMY_DB[e]).length, 0);
  console.log(`[ZONE_DB] catalog: ${Object.keys(ZONE_DB).length} zones, ${resolvedSlots}/${totalEnemySlots} enemy slots resolved v${ZONE_DB_VERSION}`);
}

// ── Zone item index ──────────────────────────────────────────────────────────
// Maps zoneId -> Set of itemIds for O(1) zone item lookups instead of O(n)
// full catalog scans. Rebuilt after any item approve/delete. Global items
// (zones:[]) are stored under the special key '__global__'.
const _zoneItemIndex = new Map(); // zoneId -> Set<itemId>

// ── Zone → clients reverse index ─────────────────────────────────────────────
// Maintained by _setClientZone. Lets zone-scoped broadcasts iterate only players
// in a zone instead of scanning the full clients map.
const _zoneClients = new Map(); // zoneId -> Set<uid>

// Set a client's zone and keep _zoneClients in sync. Always use this instead of
// directly assigning client.zone so the reverse index stays accurate.
function _setClientZone(uid, newZone) {
  const client = clients.get(uid);
  if (!client) return;
  const old = client.zone;
  if (old === newZone) return;
  // Remove from old zone bucket
  if (old) {
    const s = _zoneClients.get(old);
    if (s) { s.delete(uid); if (!s.size) _zoneClients.delete(old); }
  }
  client.zone = newZone;
  // Add to new zone bucket
  if (newZone) {
    if (!_zoneClients.has(newZone)) _zoneClients.set(newZone, new Set());
    _zoneClients.get(newZone).add(uid);
  }
}

// Returns the ID of the safe zone with zoneNumber 1
function _defaultRespawnZone() {
  const zone1 = Object.entries(ZONE_DB).find(([, z]) => z.safe && z.zoneNumber === 1);
  if (!zone1) { console.warn("[RESPAWN] No safe zone with zoneNumber 1 found in ZONE_DB"); }
  return zone1 ? zone1[0] : null;
}

function _rebuildZoneIndex() {
  _zoneItemIndex.clear();
  _zoneItemIndex.set('__global__', new Set());
  for (const [id, item] of Object.entries(ITEM_DB)) {
    if (!item._live) continue;
    if (!item.zones || !item.zones.length) {
      _zoneItemIndex.get('__global__').add(id);
    } else {
      for (const zid of item.zones) {
        if (!_zoneItemIndex.has(zid)) _zoneItemIndex.set(zid, new Set());
        _zoneItemIndex.get(zid).add(id);
      }
    }
  }
}

// Update zone index for a single item after approve/delete — avoids full rebuild
function _updateZoneIndex(itemId, item) {
  // Remove old entry from all zone sets
  for (const [, set] of _zoneItemIndex) set.delete(itemId);
  if (!item) return; // deleted — just removal needed
  if (!item.zones || !item.zones.length) {
    if (!_zoneItemIndex.has('__global__')) _zoneItemIndex.set('__global__', new Set());
    _zoneItemIndex.get('__global__').add(itemId);
  } else {
    for (const zid of item.zones) {
      if (!_zoneItemIndex.has(zid)) _zoneItemIndex.set(zid, new Set());
      _zoneItemIndex.get(zid).add(itemId);
    }
  }
}

// Build item catalog for a specific zone — O(1) index lookup instead of O(n) scan.
// Returns slim-safe objects (strips _live flag) ready to send to clients.
function _buildZoneItems(zoneId) {
  const globalIds = _zoneItemIndex.get('__global__') || new Set();
  const zoneIds   = _zoneItemIndex.get(zoneId)       || new Set();
  const result = [];
  for (const id of [...globalIds, ...zoneIds]) {
    const item = ITEM_DB[id];
    if (!item) continue;
    const { _live, ...safe } = item;
    result.push(safe);
  }
  return result;
}

// ── Zone hostile indexes ──────────────────────────────────────────────────────
// _zoneHostileIndex : zoneId  -> Set<hostileId>  (zone → which hostiles are in it)
// _hostileToZones   : hostile -> Set<zoneId>      (reverse, O(1) lookup per hostile)
// Both are built from ZONE_DB[].enemies. Rebuilt after any zone or hostile change.
const _zoneHostileIndex = new Map();
const _hostileToZones   = new Map();

function _rebuildZoneHostileIndex() {
  _zoneHostileIndex.clear();
  _hostileToZones.clear();
  for (const [zid, zone] of Object.entries(ZONE_DB)) {
    const enemies = zone.enemies || [];
    if (!enemies.length) continue;
    if (!_zoneHostileIndex.has(zid)) _zoneHostileIndex.set(zid, new Set());
    for (const eid of enemies) {
      _zoneHostileIndex.get(zid).add(eid);
      if (!_hostileToZones.has(eid)) _hostileToZones.set(eid, new Set());
      _hostileToZones.get(eid).add(zid);
    }
  }
}

// Surgical single-hostile update — avoids full rebuild on approve/delete.
function _updateZoneHostileIndex(hostileId, isDelete) {
  if (isDelete) {
    const zones = _hostileToZones.get(hostileId);
    if (zones) {
      for (const zid of zones) { const s = _zoneHostileIndex.get(zid); if (s) s.delete(hostileId); }
    }
    _hostileToZones.delete(hostileId);
    return;
  }
  const newZones = new Set();
  for (const [zid, zone] of Object.entries(ZONE_DB)) {
    if ((zone.enemies || []).includes(hostileId)) {
      newZones.add(zid);
      if (!_zoneHostileIndex.has(zid)) _zoneHostileIndex.set(zid, new Set());
      _zoneHostileIndex.get(zid).add(hostileId);
    } else if (_zoneHostileIndex.has(zid)) {
      _zoneHostileIndex.get(zid).delete(hostileId);
    }
  }
  if (newZones.size) _hostileToZones.set(hostileId, newZones);
  else _hostileToZones.delete(hostileId);
}

// ── Reverse ownership index ──────────────────────────────────────────────────
// _itemOwners      : itemId    -> Set<uid>  — who currently owns this item (equipped/inv/learned)
// _uidToOwnedItems : uid       -> Set<itemId> — reverse of _itemOwners for O(1) clear on disconnect/re-seed
// _hostileViewers  : hostileId -> Set<uid>  — who has opened this hostile's detail popup this session
// _uidToViewedHostiles: uid    -> Set<hostileId> — reverse of _hostileViewers for O(1) clear
//
// _itemOwners lets _broadcastItemToOwners skip scanning all saves (O(1) per item).
// _uidToOwnedItems makes _clearItemOwners O(owned items) instead of O(all items in catalog).
// _hostileViewers ensures players who've viewed a hostile get live updates even after
// leaving the zone, so the detail popup never shows stale data.
const _itemOwners         = new Map(); // itemId    -> Set<uid>
const _uidToOwnedItems    = new Map(); // uid       -> Set<itemId>
const _hostileViewers     = new Map(); // hostileId -> Set<uid>
const _uidToViewedHostiles = new Map(); // uid      -> Set<hostileId>

// Seed _itemOwners for one player from their save — called on login and after any
// inventory mutation that doesn't go through a dedicated add/remove call.
function _seedItemOwners(uid, save) {
  // First clear all existing entries for this uid (O(owned) via reverse index)
  _clearItemOwners(uid);
  if (!save?.player) return;
  const p   = save.player;
  const eq  = p.equipment || {};
  const inv = p.inventory  || {};
  const toAdd = new Set();
  if (eq.gear?.id)  toAdd.add(eq.gear.id);
  (eq.accessories || []).forEach(a => { if (a?.id) toAdd.add(a.id); });
  ['gears','accessories','provisions','materials'].forEach(key => {
    (inv[key] || []).forEach(it => { if (it?.id) toAdd.add(it.id); });
  });
  (p.learnedActions || []).forEach(id => toAdd.add(id));
  for (const id of toAdd) _addItemOwner(uid, id);
}

function _addItemOwner(uid, itemId) {
  if (!itemId) return;
  if (!_itemOwners.has(itemId)) _itemOwners.set(itemId, new Set());
  _itemOwners.get(itemId).add(uid);
  // Maintain reverse index
  if (!_uidToOwnedItems.has(uid)) _uidToOwnedItems.set(uid, new Set());
  _uidToOwnedItems.get(uid).add(itemId);
}

function _removeItemOwner(uid, itemId) {
  if (!itemId) return;
  const s = _itemOwners.get(itemId);
  if (s) { s.delete(uid); if (!s.size) _itemOwners.delete(itemId); }
  const r = _uidToOwnedItems.get(uid);
  if (r) { r.delete(itemId); if (!r.size) _uidToOwnedItems.delete(uid); }
}

// Clear all ownership entries for a uid — O(owned items) via reverse index
function _clearItemOwners(uid) {
  const owned = _uidToOwnedItems.get(uid);
  if (!owned) return;
  for (const itemId of owned) {
    const s = _itemOwners.get(itemId);
    if (s) { s.delete(uid); if (!s.size) _itemOwners.delete(itemId); }
  }
  _uidToOwnedItems.delete(uid);
}

function _addHostileViewer(uid, hostileId) {
  if (!hostileId) return;
  if (!_hostileViewers.has(hostileId)) _hostileViewers.set(hostileId, new Set());
  _hostileViewers.get(hostileId).add(uid);
  // Maintain reverse index
  if (!_uidToViewedHostiles.has(uid)) _uidToViewedHostiles.set(uid, new Set());
  _uidToViewedHostiles.get(uid).add(hostileId);
}

// Clear all hostile-viewer entries for a uid — O(viewed) via reverse index
function _clearHostileViewers(uid) {
  const viewed = _uidToViewedHostiles.get(uid);
  if (!viewed) return;
  for (const hid of viewed) {
    const s = _hostileViewers.get(hid);
    if (s) { s.delete(uid); if (!s.size) _hostileViewers.delete(hid); }
  }
  _uidToViewedHostiles.delete(uid);
}

// Return hostile objects for a specific zone — O(1) index lookup.
function _buildZoneHostiles(zoneId) {
  const ids = _zoneHostileIndex.get(zoneId) || new Set();
  const result = [];
  for (const id of ids) {
    const h = ENEMY_DB[id];
    if (!h || !h._live) continue;
    const { _live, ...pub } = h;
    result.push({ id, ...pub });
  }
  return result;
}

// Strip server-internal fields; return array safe to send to clients
function _buildZoneCatalog() {
  return Object.entries(ZONE_DB).map(([id, z]) => ({
    id,
    name:        z.name        || id,
    safe:        !!z.safe,
    hasFishing:  !!z.hasFishing,
    hasDungeon:  !!z.hasDungeon,
    hasRaid:     !!z.hasRaid,
    hasTrial:    !!z.hasTrial,
    subzones:    z.subzones    || [],
    market:      z.market      || null,
    rarity:      z.rarity      || 'common',
    description: z.description || '',
    enemies:     z.enemies     || [],
    zoneNumber:  z.zoneNumber  != null ? Number(z.zoneNumber) : null,
    music:       z.music       || '',
    background:  z.background  || '',
    exploreMusic:  z.exploreMusic  || '',
    arenaMusic1v1: z.arenaMusic1v1 || '',
    arenaMusic2v2: z.arenaMusic2v2 || '',
    arenaMusic4v4: z.arenaMusic4v4 || '',
    arenaBackground: z.arenaBackground || '',
  }));
}

// ── Single-entity delta rebuild helpers ──────────────────────────────────────
// These rebuild and return just one item or zone instead of the full catalog.
// Used on approve/delete so we only broadcast the changed entity, not everything.

// ── Slim item catalog ────────────────────────────────────────────────────────
// On login, clients receive only the fields needed for browsing and market display.
// Full item detail (description, effects, stats) is fetched on demand via get_item_detail.
// Fields kept in slim: id, name, rarity, cost, marketValue, type, gearType,
//   provisionType, zones, category, requiresGearType — everything needed to render
//   market listings, inventory icons, and zone filtering without the heavy fields.
function _buildSlimItem(item) {
  if (!item) return null;
  const s = {
    id:          item.id,
    name:        item.name,
    rarity:      item.rarity      || 'common',
    cost:        item.cost        || 0,
    marketValue: item.marketValue || 0,
    type:        item.type        || item.category || 'material',
    zones:       item.zones       || [],
  };
  if (item.gearType)          s.gearType          = item.gearType;
  if (item.provisionType)       s.provisionType       = item.provisionType;
  if (item.category === 'action') {
    s.category              = 'action';
    s.requiresGearType    = item.requiresGearType || null;
    // Actions need damage + cooldown for the action bar tooltip
    s.damage                = item.damage             || null;
    s.cooldown              = item.cooldown           || 0;
    s.difficulty            = item.difficulty         || 0;
  }
  // Gears stat lines shown in market — include the numbers
  if (item.type === 'gear') { s.dmg = item.dmg || 0; s.acc = item.acc || 0; s.maxHp = item.maxHp || 0; }
  if (item.type === 'accessory') { s.dmg = item.dmg || 0; s.acc = item.acc || 0; s.maxHp = item.maxHp || 0; }
  if (item.type === 'provision') { s.healHp = item.healHp || 0; s.usableInCombat = !!item.usableInCombat; }
  return s;
}

function _rebuildOneItem(entityId) {
  // Returns { item, deleted } — item is the built ITEM_DB entry (or null if deleted)
  try {
    const row = _stmtGetDevEntity.get(entityId);
    if (!row) {
      // entityId didn't match a live item directly — could be a draft key.
      // Only treat as a real deletion if we actually had this key in ITEM_DB.
      if (ITEM_DB[entityId]) {
        // Confirmed deletion.
        // If this was an action, clean up its ACTION_DB entry too.
        if (ITEM_DB[entityId].category === 'action' && ACTION_DB[entityId]) {
          delete ACTION_DB[entityId];
          ACTION_DB_VERSION = Date.now();
        }
        delete ITEM_DB[entityId];
        _updateZoneIndex(entityId, null);
        ITEM_DB_VERSION = Date.now();
        return { deleted: entityId, item: null };
      }
      // Unknown / draft key with no corresponding live row — do not scrub.
      // Fall back to a full rebuild to keep the catalog consistent.
      console.warn(`[ITEMS] _rebuildOneItem: no live row for "${entityId}" and not in ITEM_DB — running full rebuild as fallback`);
      try { _rebuildItemsFromDB(); } catch(e) { console.error('[ITEMS] fallback full rebuild error:', e.message); }
      return null;
    }
    const d = JSON.parse(row.data || '{}');
    const iid = d.id || row.id;
    if (!iid) return null;
    // Detect ID rename: evict old key so stale entry doesn't linger
    const oldDeleted = (iid !== entityId && ITEM_DB[entityId]) ? entityId : null;
    if (oldDeleted) {
      if (ITEM_DB[entityId].category === 'action' && ACTION_DB[entityId]) delete ACTION_DB[entityId];
      _updateZoneIndex(entityId, null);
      delete ITEM_DB[entityId];
      console.log(`[ITEM_DB] ID renamed: evicting old key "${entityId}" → "${iid}"`);
    }
    const base = {
      id:          iid,
      name:        d.name  || row.name || iid,
      cost:        parseInt(d.cost)  || 0,
      marketValue: parseInt(d.marketValue) || parseInt(d.sellValue) || Math.floor((parseInt(d.cost)||0) * 0.75),
      description: d.description || '',
      rarity:      d.rarity || 'common',
      zones:       Array.isArray(d.zones) ? d.zones : [],
      _live:       true,
    };
    if (d.type === 'gear' || d.equipType === 'gear') {
      Object.assign(base, { type:'gear', gearType: d.gearType||'brutality', dmg: parseInt(d.dmg)||0, acc: parseInt(d.acc)||0, maxHp: parseInt(d.maxHp)||0 });
    } else if (d.type === 'accessory' || d.equipType === 'accessory') {
      Object.assign(base, { type:'accessory', dmg: parseInt(d.dmg)||0, acc: parseInt(d.acc)||0, maxHp: parseInt(d.maxHp)||0 });
    } else if (d.type === 'action' || d.category === 'action') {
      const dmgRange = Array.isArray(d.damage) ? d.damage : [parseInt(d.minDamage)||5, parseInt(d.maxDamage)||10];
      Object.assign(base, {
        category:'action',
        damage:     dmgRange,                        // stored on ITEM_DB so _buildSlimItem can include it
        difficulty: parseInt(d.difficulty) || 20,   // stored on ITEM_DB so _buildSlimItem can include it
        cooldown:   parseInt(d.cooldown)   || 0,    // stored on ITEM_DB so _buildSlimItem can include it
        requiresGearType: d.requiresGearType||null,
      });
      try {
        const compiled = _compileEffects(d);
        ACTION_DB[iid] = {
          name: d.name || row.name || iid, damage: dmgRange,
          difficulty: parseInt(d.difficulty)||20, cooldown: parseInt(d.cooldown)||0,
          requiresGearType: d.requiresGearType||null,
          dotEffect: !!d.dotEffect, energyStopEffect: !!d.energyStopEffect,
          energyOvertimeEffect: !!d.energyOvertimeEffect, healEffect: !!d.healEffect,
          healAmount: d.healAmount||null, healTicks: d.healTicks||null,
          _effects: compiled, _live: true,
        };
      } catch(ec) { console.error('[ACTIONS] delta compile error:', iid, ec.message); }
    } else if (d.type === 'provision') {
      const provEffects = _compileEffects(d);
      Object.assign(base, { type:'provision', provisionType: d.provisionType||'potion', healHp: parseInt(d.healHp)||0, usableInCombat: !!d.usableInCombat, _effects: provEffects });
    } else if (d.type === 'material') {
      Object.assign(base, { type:'material' });
    }
    ITEM_DB[iid] = base;
    // Update zone index for this single item
    _updateZoneIndex(iid, base);
    // Refresh valid learned set for this action
    if (base.category === 'action') {
      const _newLearned = _buildValidLearned();
      VALID_LEARNED.clear(); _newLearned.forEach(id => VALID_LEARNED.add(id));
      VALID_ACTION_IDS.clear(); _newLearned.forEach(id => { if(id !== 'provisions') VALID_ACTION_IDS.add(id); });
      ACTION_DB_VERSION = Date.now();
    }
    ITEM_DB_VERSION = Date.now();
    const { _live, ...pub } = base;
    return { item: pub, deleted: null, oldDeleted: oldDeleted || null };
  } catch(e) {
    console.error('[ITEMS] delta rebuild error:', entityId, e.message);
    return null;
  }
}

function _rebuildOneZone(entityId) {
  // Returns { zone, deleted, oldDeleted } — zone is the client-facing zone object (or null if deleted).
  // oldDeleted is set when the zone's id field was renamed (entityId != zid) so the caller
  // can evict the old key from the catalog and broadcast a deletion to clients.
  try {
    const row = _stmtGetDevEntity.get(entityId);
    if (!row) {
      // entityId didn't match a live zone directly — could be a draft key.
      // Only treat as a real deletion if we actually had this key in ZONE_DB.
      if (ZONE_DB[entityId]) {
        // Confirmed deletion.
        delete ZONE_DB[entityId];
        ZONE_DB_VERSION = Date.now();
        return { deleted: entityId, zone: null, oldDeleted: null };
      }
      // Unknown / draft key with no corresponding live row — do not scrub.
      // Fall back to a full rebuild to keep the catalog consistent.
      console.warn(`[ZONE_DB] _rebuildOneZone: no live row for "${entityId}" and not in ZONE_DB — running full rebuild as fallback`);
      try { _rebuildLiveZones(); } catch(e) { console.error('[ZONE_DB] fallback full rebuild error:', e.message); }
      return null;
    }
    const d = JSON.parse(row.data || '{}');
    const zid = d.id || row.id;
    if (!zid) return null;
    // Detect ID rename: evict old key so stale entry doesn't linger
    const oldDeleted = (zid !== entityId && ZONE_DB[entityId]) ? entityId : null;
    if (oldDeleted) {
      delete ZONE_DB[entityId];
      console.log(`[ZONE_DB] ID renamed: evicting old key "${entityId}" → "${zid}"`);
    }
    const respawnRule = d.respawnRule || d.type || '';
    const isSafe = respawnRule === 'Safe';
    const subzones = Array.isArray(d._subzones) ? d._subzones : [];
    const assignments = Array.isArray(d._assignments) ? d._assignments : [];
    const enemies = assignments.map(a => _resolveEnemyKey(a.entity || a.name || a.id)).filter(Boolean);
    const weights = {};
    for (const a of assignments) {
      const key = _resolveEnemyKey(a.entity || a.name || a.id);
      if (key && a.spawn) weights[key] = parseFloat(a.spawn) || 1;
    }
    ZONE_DB[zid] = {
      ...(ZONE_DB[zid] || {}),
      name: d.name || row.name || zid, safe: isSafe, enemies,
      weights: Object.keys(weights).length ? weights : undefined,
      hasFishing: subzones.some(sz => sz.type === 'Fishing'),
      hasDungeon: subzones.some(sz => sz.type === 'Dungeon'),
      hasRaid:    subzones.some(sz => sz.type === 'Raid'),
      hasTrial:   subzones.some(sz => sz.type === 'Trial'),
      subzones, market: d._market||null, rarity: d.rarity||'common',
      description: d.description||'',
      zoneNumber: d.zoneNumber != null ? Number(d.zoneNumber) : null,
      music: d.music || '',
      background: d.background || '',
      exploreMusic: d.exploreMusic || '',
      arenaMusic1v1: d.arenaMusic1v1 || '',
      arenaMusic2v2: d.arenaMusic2v2 || '',
      arenaMusic4v4: d.arenaMusic4v4 || '',
      arenaBackground: d.arenaBackground || '',
      _live: true,
    };
    ZONE_DB_VERSION = Date.now();
    const z = ZONE_DB[zid];
    return { zone: {
      id: zid, name: z.name||zid, safe: !!z.safe,
      hasFishing: !!z.hasFishing, hasDungeon: !!z.hasDungeon,
      hasRaid: !!z.hasRaid, hasTrial: !!z.hasTrial,
      subzones: z.subzones||[], market: z.market||null,
      rarity: z.rarity||'common', description: z.description||'',
      enemies: z.enemies||[], zoneNumber: z.zoneNumber!=null?Number(z.zoneNumber):null,
      music: z.music||'',
      background: z.background||'',
      exploreMusic: z.exploreMusic||'',
      arenaMusic1v1: z.arenaMusic1v1||'',
      arenaMusic2v2: z.arenaMusic2v2||'',
      arenaMusic4v4: z.arenaMusic4v4||'',
      arenaBackground: z.arenaBackground||'',
    }, deleted: null, oldDeleted };
  } catch(e) {
    console.error('[ZONE_DB] delta rebuild error:', entityId, e.message);
    return null;
  }
}

function _pickZoneEnemy(zone) {
  // Only pick hostiles that still exist in ENEMY_DB — deleted ones are skipped
  const validEnemies = (zone.enemies || []).filter(e => ENEMY_DB[e]);
  const pool = validEnemies.length ? validEnemies : (zone.enemies || []);
  const w = zone.weights;
  if (!w) return pool[Math.floor(Math.random() * pool.length)];
  const entries = pool.map(e => ({ type:e, weight: w[e]||1 }));
  const total = entries.reduce((s,e) => s+e.weight, 0);
  let r = Math.random() * total;
  for (const e of entries) { r -= e.weight; if (r <= 0) return e.type; }
  return entries[entries.length-1].type;
}

// ── Item databases ───────────────────────────────────────────────────────────
const ITEM_DB = {};
// All items are loaded live from dev_entities at boot and on approval via _rebuildItemsFromDB()

// ── Catalog version counters ─────────────────────────────────────────────────
// Incremented whenever items or zones are rebuilt. Sent to clients on auth so
// they can skip downloading catalogs they already have cached in localStorage.
let ITEM_DB_VERSION    = Date.now();
let ZONE_DB_VERSION    = Date.now();
let ENEMY_DB_VERSION = Date.now();
let ACTION_DB_VERSION  = Date.now();

// ════════════════════════════════════════════════════════════════════════════
// FUTURE UPGRADE — STAGE 2 (PostgreSQL migration):
// ════════════════════════════════════════════════════════════════════════════
// UPGRADE 2: Database-backed item lookups with Redis caching
//
// DO THIS when you migrate from SQLite → PostgreSQL at Stage 2.
//
// Current problem this solves:
//   ITEM_DB is a plain in-memory JS object. Fine for thousands of items,
//   but at tens of thousands it consumes significant RAM, and at millions it
//   becomes impossible — the whole catalog can't live in one Node.js process.
//
// What to build:
//   1. Replace ITEM_DB[id] everywhere with: await getItem(id)
//   2. getItem() checks Redis first (hot items, TTL ~10 min), then PostgreSQL.
//   3. _rebuildItemsFromDB() becomes a no-op or is removed entirely — items
//      are never bulk-loaded into memory, they're fetched on demand.
//   4. On approve/delete, invalidate that item's Redis key so the next fetch
//      goes to the DB and gets fresh data.
//   5. ITEM_DB stays as a hot-item cache for items used in the current tick
//      (combat lookups etc.), but is never the source of truth.
//
// Why not now:
//   - better-sqlite3 is synchronous. Redis/PostgreSQL are async.
//     Converting every ITEM_DB[id] reference to await getItem(id) touches
//     hundreds of call sites and is a high-risk refactor on a codebase still
//     actively changing. Do it once, cleanly, at the PostgreSQL migration.
//   - SQLite in-process reads are faster than a Redis round trip anyway.
//     Redis caching only pays off when the DB is on a separate machine.
//
// Search for ITEM_DB to find every place that needs updating.
// ════════════════════════════════════════════════════════════════════════════

// ════════════════════════════════════════════════════════════════════════════
// FUTURE UPGRADE — STAGE 5 (second machine):
// ════════════════════════════════════════════════════════════════════════════
// UPGRADE 3: Stateless WebSocket servers
//
// DO THIS when you add a second Fly.io machine at Stage 5.
//
// Current problem this solves:
//   Player session state lives in the `clients` Map on whichever machine the
//   player connected to. Combat rooms live in memory on the machine that
//   started them. With two machines:
//     - A party invite from machine 1 to a player on machine 2 silently fails
//       (machine 1 can't find the player in its local clients Map).
//     - Combat rooms are machine-local — party members split across machines
//       can't share a room.
//     - DMs, friend presence, zone chat all have the same cross-machine gap.
//   The Redis pub/sub broadcast (already built) handles fire-and-forget global
//   messages but does NOT solve targeted per-player routing.
//
// What to build:
//   1. Move `clients` session data (uid, username, zone, deviceId) into Redis
//      with a TTL that refreshes on every message. Key: pv:session:{uid}
//   2. Move combat room state into Redis (or a shared DB table for durability).
//      Key: pv:room:{roomId}
//   3. Replace direct clients.get(uid).ws.send() with a Redis pub/sub message
//      to the channel pv:player:{uid} — the machine that holds that player's
//      WebSocket subscribes to it and forwards to the socket.
//   4. Add sticky sessions at the Fly.io load balancer level as a temporary
//      measure so party members preferentially route to the same machine while
//      full stateless migration is underway.
//   5. Keep the local `clients` Map as a fast lookup for players on THIS
//      machine. Cross-machine messages go via Redis.
//
// Why not now:
//   - You only have one machine. All of this complexity is irrelevant until
//     a second machine exists. Building it now adds risk for zero benefit.
//   - The Redis pub/sub broadcast wrapper already built handles catalog deltas
//     and presence broadcasts correctly. That's sufficient for Stage 5 entry.
//
// Search for `clients.get(` to find every targeted send that needs updating.
// ════════════════════════════════════════════════════════════════════════════

// ── Dynamic item catalog from dev_entities ───────────────────────────────────
// Merges approved equipment/action entities into ITEM_DB at boot + on approval.
function _rebuildItemsFromDB() {
  try {
    // Remove previously live-loaded item entries so deleted/rejected items don't persist
    for (const k of Object.keys(ITEM_DB)) { if (ITEM_DB[k]._live) delete ITEM_DB[k]; }
    const rows = db.prepare(
      `SELECT id, name, data FROM dev_entities WHERE category IN ('equipment','action','material','provision','gears','accessories') AND status='live'`
    ).all();
    for (const row of rows) {
      try {
        const d = JSON.parse(row.data || '{}');
        const iid = d.id || row.id;
        if (!iid) continue;
        // Map dev panel category/type fields to ITEM_DB shape
        const base = {
          id:          iid,
          name:        d.name  || row.name || iid,
          cost:        parseInt(d.cost)  || 0,
          marketValue: parseInt(d.marketValue) || parseInt(d.sellValue) || Math.floor((parseInt(d.cost)||0) * 0.75),
          description: d.description || '',
          rarity:      d.rarity || 'common',
          zones:       Array.isArray(d.zones) ? d.zones : [],
          _live:       true,
        };
        // Equipment subtypes
        if (d.type === 'gear' || d.equipType === 'gear') {
          Object.assign(base, { type:'gear', gearType: d.gearType||'brutality', dmg: parseInt(d.dmg)||0, acc: parseInt(d.acc)||0, maxHp: parseInt(d.maxHp)||0 });
        } else if (d.type === 'accessory' || d.equipType === 'accessory') {
          Object.assign(base, { type:'accessory', dmg: parseInt(d.dmg)||0, acc: parseInt(d.acc)||0, maxHp: parseInt(d.maxHp)||0 });
        } else if (d.type === 'action' || d.category === 'action') {
          const dmgRange = Array.isArray(d.damage) ? d.damage : [parseInt(d.minDamage)||5, parseInt(d.maxDamage)||10];
          Object.assign(base, {
            category:'action',
            damage:     dmgRange,                        // stored on ITEM_DB so _buildSlimItem can include it
            difficulty: parseInt(d.difficulty) || 20,   // stored on ITEM_DB so _buildSlimItem can include it
            cooldown:   parseInt(d.cooldown)   || 0,    // stored on ITEM_DB so _buildSlimItem can include it
            requiresGearType: d.requiresGearType||null,
          });
          // ── Compile effect atoms and load full action def into ACTION_DB ──
          try {
            const compiled = _compileEffects(d);
            ACTION_DB[iid] = {
              name:        d.name || row.name || iid,
              damage:      dmgRange,
              difficulty:  parseInt(d.difficulty) || 20,
              cooldown:    parseInt(d.cooldown)   || 0,
              requiresGearType: d.requiresGearType || null,
              dotEffect:        !!d.dotEffect,
              energyStopEffect: !!d.energyStopEffect,
              energyOvertimeEffect:!!d.energyOvertimeEffect,
              healEffect:       !!d.healEffect,
              healAmount:       d.healAmount || null,
              healTicks:        d.healTicks  || null,
              _effects: compiled,
              _live: true,
            };
          } catch(ec) { console.error('[ACTIONS] compile error:', iid, ec.message); }
        } else if (d.type === 'provision') {
          const provEffects = _compileEffects(d);
          Object.assign(base, { type:'provision', provisionType: d.provisionType||'potion', healHp: parseInt(d.healHp)||0, usableInCombat: !!d.usableInCombat, _effects: provEffects });
        } else if (d.type === 'material') {
          Object.assign(base, { type:'material' });
          if (d._fishBehavior && typeof d._fishBehavior === 'object') base._fishBehavior = d._fishBehavior;
        }
        ITEM_DB[iid] = base;
      } catch(ei) { console.error('[ITEMS] parse error:', row.id, ei.message); }
    }
    ITEM_DB_VERSION   = Date.now();
    ACTION_DB_VERSION = Date.now();
    console.log(`[ITEMS] catalog: ${Object.keys(ITEM_DB).length} items v${ITEM_DB_VERSION}`);
    // Rebuild zone index so zone-scoped lookups stay O(1)
    _rebuildZoneIndex();
    // Refresh valid learned actions set
    const _newLearned = _buildValidLearned();
    VALID_LEARNED.clear(); _newLearned.forEach(id => VALID_LEARNED.add(id));
    VALID_ACTION_IDS.clear(); _newLearned.forEach(id => { if(id !== 'provisions') VALID_ACTION_IDS.add(id); });
  } catch(e) { console.error('[ITEMS] rebuild error:', e.message); }
}

function getInvKey(item) {
  if (item.type === "provision") return "provisions";
  if (item.type === "material") return "materials";
  if (item.type === "gear") return "gears";
  return "accessories";
}

// ── Fish database (legacy fallback — live fish come from ITEM_DB via dev panel) ──
// Fish definitions come from dev_entities (category=material, source=Fishing)
// This DB is populated at boot from live dev_entities via _rebuildItemsFromDB
const MATERIALS_DB = {
  // Sanosuke "The Boosk" — hardcoded memorial item, always valid
  sanosuke: { id:"sanosuke", name:'Sagara Sanosuke "The Boosk"', rarity:"epic", type:"material", marketValue:0 },
};
const _fishCooldowns = new Map();
const FISH_COOLDOWN_MS = 8000;
const _fishRareLog = new Map(); // uid → timestamps of epic/legendary/mythic catches (anomaly detection)

// Fixed rarity percentages — must match client FIXED_PCTS exactly
const FISH_RARITY_PCTS = { common:52, uncommon:20, rare:15, epic:10, legendary:1.5, mythic:0.5 };
const FISH_RARITY_ORDER = ['common','uncommon','rare','epic','legendary','mythic'];

// ── Server-authoritative reel simulation ─────────────────────────────────────
// The client only ever sends "I am holding the reel button" or "I let go" —
// it never tells the server it caught anything. The server runs the exact
// same physics formulas the client uses to render the minigame, and decides
// win/loss itself. This removes the trust boundary entirely: there is no
// message a forged/scripted client can send that grants a reward by itself.
const _fishReelSessions = new Map(); // uid → session object (see _fishStartSession)
const FISH_TICK_MS = 90; // server simulation tick rate (~11/sec — plenty for this minigame's pace)

// Full physics table by rarity — mirrors client FISH_RARITIES exactly (common→epic)
// and extends the same progression to legendary/mythic, which the live client
// currently doesn't define (it silently falls back to "common" physics for those —
// a separate pre-existing client bug; the server uses the correct harder values
// regardless of what the client renders).
const FISH_PHYSICS_FULL = {
  common:    { zoneWidth:30.0, zoneSpeed:0.500, cursorDrift:0.500, cursorPush:1.000, tensionRate:0.500, tensionRecover:0.500, catchRate:0.500, catchDecay:0.100, biteMin:5000, biteMax:10000, dirChangeRate:0.005 },
  uncommon:  { zoneWidth:27.0, zoneSpeed:0.550, cursorDrift:0.550, cursorPush:0.900, tensionRate:0.550, tensionRecover:0.450, catchRate:0.480, catchDecay:0.110, biteMin:5000, biteMax:10000, dirChangeRate:0.010 },
  rare:      { zoneWidth:24.0, zoneSpeed:0.600, cursorDrift:0.600, cursorPush:0.800, tensionRate:0.600, tensionRecover:0.400, catchRate:0.460, catchDecay:0.120, biteMin:5000, biteMax:10000, dirChangeRate:0.020 },
  epic:      { zoneWidth:22.5, zoneSpeed:0.625, cursorDrift:0.625, cursorPush:0.750, tensionRate:0.625, tensionRecover:0.375, catchRate:0.450, catchDecay:0.125, biteMin:5000, biteMax:10000, dirChangeRate:0.038 },
  legendary: { zoneWidth:20.0, zoneSpeed:0.650, cursorDrift:0.650, cursorPush:0.700, tensionRate:0.650, tensionRecover:0.350, catchRate:0.440, catchDecay:0.135, biteMin:5000, biteMax:10000, dirChangeRate:0.060 },
  mythic:    { zoneWidth:18.0, zoneSpeed:0.700, cursorDrift:0.700, cursorPush:0.650, tensionRate:0.700, tensionRecover:0.300, catchRate:0.420, catchDecay:0.150, biteMin:5000, biteMax:10000, dirChangeRate:0.090 },
};

// Resolve the physics to use for a given fish: rarity defaults, overridden field-by-field
// by any custom _fishBehavior.params saved on the material in the dev panel.
function _fishPhysicsFor(fishDef) {
  const rarity = (fishDef && fishDef.rarity) || 'common';
  const def = FISH_PHYSICS_FULL[rarity] || FISH_PHYSICS_FULL.common;
  const params = (fishDef && fishDef._fishBehavior && fishDef._fishBehavior.params) || {};
  const out = Object.assign({}, def);
  Object.keys(def).forEach(k => {
    const v = parseFloat(params[k]);
    if (params[k] !== undefined && params[k] !== '' && !isNaN(v)) out[k] = v;
  });
  return out;
}

// ── Trait engine ──────────────────────────────────────────────────────────────
// Traits are authored in the dev panel (Chapter 10 of the guidebook) and saved as
// fishDef._fishBehavior.{traits: [...keys], params: {...flat key/value overrides}}.
// All 59 traits across Zone Shape, Movement, and Special are implemented here,
// server-authoritative like everything else in this file. Movement traits with
// fundamentally different position models (orbit, pendulum, teleport, spiral,
// bounce, ricochet, float, strobe, reel, swarm, coil, vortex) replace the default
// bounce-and-wall motion; shape traits (shrink/warp/pulse/expand/drift/shatter/
// elastic/crumble/taper/morph/heartbeat/ripple) are mutually exclusive on zone
// width, first match wins; multi-zone traits (split, split3, twin, cascade, clone,
// phantom, echo, ghost, invisible, split_fade) populate a generic `extraZones`
// array the client renders identically to the original decoy band. Drop a new
// `if (T.has('x'))` block into _fishTick following these patterns to add more.
function _fishTP(s, key, def) {
  const v = s.params[key];
  if (v === undefined || v === '') return def;
  if (typeof v === 'string') { const n = parseFloat(v); return isNaN(n) ? v : n; }
  return v;
}

function _fishClearSession(uid) {
  const s = _fishReelSessions.get(uid);
  if (!s) return;
  if (s.biteTimer) clearTimeout(s.biteTimer);
  _fishReelSessions.delete(uid);
}

// Begin a new fishing session: roll the fish, schedule the bite, and once it
// bites, start the authoritative tick loop. Nothing here trusts the client.
function _fishStartSession(uid, ws, assignedFish, fishDef) {
  _fishClearSession(uid); // cancel any stale session for this uid first
  const physics = _fishPhysicsFor(fishDef);
  const biteDelay = physics.biteMin + Math.random() * Math.max(0, physics.biteMax - physics.biteMin);
  const session = {
    fishId: assignedFish, fishDef, physics,
    traits: new Set((fishDef._fishBehavior && Array.isArray(fishDef._fishBehavior.traits)) ? fishDef._fishBehavior.traits : []),
    params: (fishDef._fishBehavior && fishDef._fishBehavior.params) || {},
    traitState: {},
    state: 'bite_wait',
    pressing: false,
    biteTimer: null, lastTime: 0, elapsed: 0,
    zonePos: 0, zoneVel: 0, zoneWidth: 0, cursorPos: 50, catchPct: 0, tension: 0,
  };
  _fishReelSessions.set(uid, session);
  session.biteTimer = setTimeout(() => _fishBeginReel(uid, ws), biteDelay);
}

function _fishBeginReel(uid, ws) {
  const s = _fishReelSessions.get(uid);
  if (!s || s.state !== 'bite_wait') return;
  s.state = 'reeling';
  s.zonePos = 20 + Math.random() * 20;
  s.zoneWidth = s.physics.zoneWidth;
  s.zoneVel = (Math.random() < 0.5 ? 1 : -1) * s.physics.zoneSpeed;
  s.cursorPos = 50; s.catchPct = 0; s.tension = 0;
  s.lastTime = Date.now(); s.elapsed = 0;
  if (ws && ws.readyState === 1) send(ws, { type: "fish_bite", fishId: s.fishId });
  // No per-session timer anymore — the single global _fishGlobalTick (see below)
  // ticks every active session. Cheaper at scale than one setInterval per angler.
}

// The authoritative simulation step — same base formulas as the client's original
// local minigame, plus the trait engine above, just run here instead of trusted
// from there. dt is derived from the server's own clock, never anything the client supplies.
function _fishTick(uid) {
  const s = _fishReelSessions.get(uid);
  if (!s || s.state !== 'reeling') return;
  const client = clients.get(uid); // re-looked-up every tick — survives client reconnects mid-session
  const ws = client && client.ws;
  const now = Date.now();
  const dt = Math.min((now - s.lastTime) / 1000, 0.2);
  s.lastTime = now;
  s.elapsed += dt;
  const p = s.physics;
  const T = s.traits;
  const ts = s.traitState;
  const tp = (key, def) => _fishTP(s, key, def);

  // ── Speed modifiers (apply before movement) ──
  let speedMult = 1;

  if (T.has('accelerate')) {
    speedMult *= Math.min(tp('accelMax', 2.0), 1 + (s.catchPct / 10) * tp('accelRate', 0.2));
  }
  if (T.has('berserk')) {
    if (!ts.berserkActive && s.catchPct >= tp('berserkThreshold', 75)) ts.berserkActive = true;
    if (ts.berserkActive) {
      speedMult *= tp('berserkSpeedMult', 2.5);
      if (Math.random() < tp('berserkErratic', 0.08) * dt * 60) {
        s.zoneVel = (Math.random() < 0.5 ? 1 : -1) * p.zoneSpeed * speedMult;
      }
    }
  }
  if (T.has('zigzag')) {
    const phaseDur = tp('phaseDur', 0.4);
    ts.zigzagTimer = (ts.zigzagTimer || 0) + dt;
    if (ts.zigzagTimer >= phaseDur) { ts.zigzagTimer = 0; ts.zigzagFast = !ts.zigzagFast; }
    speedMult *= ts.zigzagFast ? tp('fastMult', 1.6) : tp('slowMult', 0.35);
  }

  // ── Zone movement ──
  const dm = 1 + s.catchPct / 100 * 0.4;
  const clampZone = () => { s.zonePos = Math.max(2 + s.zoneWidth / 2, Math.min(98 - s.zoneWidth / 2, s.zonePos)); };
  if (T.has('chase')) {
    // Gentle continuous pull toward the cursor, layered on top of normal motion.
    s.zonePos += (s.cursorPos - s.zonePos) * tp('chaseStrength', 0.04) * dt * 6;
  }
  if (T.has('hesitate')) {
    ts.hesitating = ts.hesitating || false;
    if (!ts.hesitating && (s.zonePos - s.zoneWidth / 2 < 3 || s.zonePos + s.zoneWidth / 2 > 97)) {
      ts.hesitating = true; ts.hesitateTimer = tp('pauseMin', 0.1) + Math.random() * (tp('pauseMax', 0.3) - tp('pauseMin', 0.1));
    }
  }

  // ── Alternate movement models — replace the default bounce-and-wall logic ──
  let altMovement = true;
  if (T.has('orbit')) {
    const r = tp('orbitRadius', 55), spd = tp('orbitSpeed', 1.8);
    ts.orbitAngle = (ts.orbitAngle || 0) + spd * dt;
    s.zonePos = 50 + (r / 2) * Math.sin(ts.orbitAngle);
    clampZone();
  } else if (T.has('pendulum')) {
    const period = tp('pendulumPeriod', 2.2), amp = tp('pendulumAmp', 70);
    s.zonePos = 50 + (amp / 2) * Math.sin(2 * Math.PI * s.elapsed / period);
    clampZone();
  } else if (T.has('teleport')) {
    ts.teleTimer = (ts.teleTimer || 0) + dt;
    const interval = tp('teleInterval', 3.5);
    ts.teleWarnActive = ts.teleTimer >= interval - tp('teleWarn', 0.3);
    if (ts.teleTimer >= interval) { s.zonePos = 5 + Math.random() * 90; clampZone(); ts.teleTimer = 0; ts.teleWarnActive = false; }
  } else if (T.has('spiral')) {
    ts.spiralPhase = (ts.spiralPhase || 0) + dt * tp('spiralSpeed', 1.6);
    const amp = 46 * (0.5 + 0.5 * Math.cos(ts.spiralPhase * tp('spiralDecay', 0.8) * 0.3));
    s.zonePos = 50 + amp * Math.sin(ts.spiralPhase);
    clampZone();
  } else if (T.has('bounce')) {
    ts.bounceEnergy = ts.bounceEnergy === undefined ? 1 : ts.bounceEnergy;
    ts.bounceHits = ts.bounceHits || 0;
    s.zonePos += s.zoneVel * speedMult * ts.bounceEnergy * dm * 60 * dt;
    const accel = tp('bounceAccel', 0.2), resetAfter = tp('bounceReset', 5);
    if (s.zonePos - s.zoneWidth / 2 < 2) { s.zonePos = s.zoneWidth / 2 + 2; s.zoneVel = Math.abs(s.zoneVel); ts.bounceHits++; ts.bounceEnergy += accel; }
    if (s.zonePos + s.zoneWidth / 2 > 98) { s.zonePos = 98 - s.zoneWidth / 2; s.zoneVel = -Math.abs(s.zoneVel); ts.bounceHits++; ts.bounceEnergy += accel; }
    if (ts.bounceHits >= resetAfter) { ts.bounceEnergy = 1; ts.bounceHits = 0; }
  } else if (T.has('ricochet')) {
    ts.ricVel = ts.ricVel === undefined ? s.zoneVel * tp('ricSpeed', 1.6) : ts.ricVel;
    s.zonePos += ts.ricVel * 60 * dt;
    if (s.zonePos - s.zoneWidth / 2 < 2 || s.zonePos + s.zoneWidth / 2 > 98) {
      clampZone();
      const variance = tp('ricAngleVar', 0.25);
      ts.ricVel = -ts.ricVel * (1 - variance / 2 + Math.random() * variance);
    }
  } else if (T.has('float')) {
    ts.floatVel = ts.floatVel === undefined ? 0 : ts.floatVel;
    const drag = tp('floatDrag', 0.88), impulse = tp('floatImpulse', 0.2);
    if (Math.random() < 0.06) ts.floatVel += (Math.random() - 0.5) * impulse;
    ts.floatVel *= Math.pow(drag, dt * 60);
    s.zonePos += ts.floatVel * 60 * dt;
    if (s.zonePos - s.zoneWidth / 2 < 2) { s.zonePos = s.zoneWidth / 2 + 2; ts.floatVel = Math.abs(ts.floatVel) * 0.5; }
    if (s.zonePos + s.zoneWidth / 2 > 98) { s.zonePos = 98 - s.zoneWidth / 2; ts.floatVel = -Math.abs(ts.floatVel) * 0.5; }
  } else if (T.has('strobe')) {
    ts.strobeTimer = (ts.strobeTimer || 0) + dt;
    const rate = tp('strobeRate', 4), step = tp('strobeStep', 10);
    if (ts.strobeTimer >= 1 / rate) { ts.strobeTimer = 0; s.zonePos += (Math.random() < 0.5 ? -1 : 1) * step; clampZone(); }
  } else if (T.has('reel')) {
    ts.reelDir = ts.reelDir === undefined ? 1 : ts.reelDir;
    const rs = tp('reelSpeed', 0.08), snap = tp('reelSnapSpeed', 1.5);
    ts.reelPhase = (ts.reelPhase || 0) + rs * dt;
    if (ts.reelPhase >= 1) { ts.reelDir *= -1; ts.reelPhase = 0; }
    s.zonePos += ts.reelDir * (ts.reelPhase < 0.05 ? snap * 15 : rs * 8) * dt;
    clampZone();
  } else if (T.has('swarm')) {
    const count = tp('swarmCount', 4), cohesion = tp('swarmCohesion', 0.5);
    ts.swarmPhase = (ts.swarmPhase || 0) + dt;
    s.zonePos += s.zoneVel * speedMult * dm * 60 * dt * cohesion;
    s.zonePos += Math.sin(ts.swarmPhase * count * 0.7) * (1 - cohesion) * 30 * dt;
    if (s.zonePos - s.zoneWidth / 2 < 2) { s.zonePos = s.zoneWidth / 2 + 2; s.zoneVel = Math.abs(s.zoneVel); }
    if (s.zonePos + s.zoneWidth / 2 > 98) { s.zonePos = 98 - s.zoneWidth / 2; s.zoneVel = -Math.abs(s.zoneVel); }
  } else if (T.has('coil')) {
    const coilDur = tp('coilDur', 1.0), burst = tp('coilBurstSpeed', 5.0);
    ts.coilTimer = (ts.coilTimer || 0) + dt;
    if (ts.coilTimer < coilDur) { s.zonePos += Math.sin(ts.coilTimer * 20) * 2; clampZone(); }
    else {
      s.zonePos += (s.zoneVel >= 0 ? 1 : -1) * burst * 60 * dt;
      if (s.zonePos - s.zoneWidth / 2 < 2 || s.zonePos + s.zoneWidth / 2 > 98) { clampZone(); ts.coilTimer = 0; s.zoneVel = -s.zoneVel; }
    }
  } else if (T.has('vortex')) {
    const shrink = tp('vortexShrinkRate', 0.1), esc = tp('vortexEscapeSpeed', 1.2);
    ts.vortexRadius = ts.vortexRadius === undefined ? 45 : ts.vortexRadius;
    ts.vortexAngle = (ts.vortexAngle || 0) + dt * 2 * esc;
    ts.vortexRadius -= shrink * 60 * dt;
    if (ts.vortexRadius < 4) ts.vortexRadius = 45;
    s.zonePos = 50 + ts.vortexRadius * Math.sin(ts.vortexAngle);
  } else {
    altMovement = false;
  }

  if (!altMovement && !(T.has('hesitate') && ts.hesitating)) {
    s.zonePos += s.zoneVel * speedMult * dm * 60 * dt;
    if (s.zonePos - s.zoneWidth / 2 < 2) { s.zonePos = s.zoneWidth / 2 + 2; s.zoneVel = Math.abs(s.zoneVel); }
    if (s.zonePos + s.zoneWidth / 2 > 98) { s.zonePos = 98 - s.zoneWidth / 2; s.zoneVel = -Math.abs(s.zoneVel); }
  }
  if (T.has('hesitate') && ts.hesitating) {
    ts.hesitateTimer -= dt;
    if (ts.hesitateTimer <= 0) ts.hesitating = false;
  }

  if (T.has('flip')) {
    const chance = tp('flipChance', 0.25);
    if (!ts.flipWarnActive && Math.random() < chance * dt) { ts.flipWarnActive = true; ts.flipWarnTimer = 0; }
    if (ts.flipWarnActive) {
      ts.flipWarnTimer += dt;
      if (ts.flipWarnTimer >= tp('flipWarn', 0.15)) { s.zonePos = 100 - s.zonePos; clampZone(); ts.flipWarnActive = false; }
    }
  }
  if (T.has('shimmy')) {
    s.zonePos += Math.sin(s.elapsed * tp('shimmyFreq', 10)) * (tp('shimmyAmp', 4) / 100) * 100 * dt;
  }

  if (T.has('sine')) {
    s.zonePos += Math.sin(s.elapsed * tp('sineFreq', 1.4)) * tp('sineAmp', 0.18) * 60 * dt;
  }
  if (T.has('mirror') && Math.random() < tp('mirrorChance', 0.6) * dt) {
    s.zoneVel = -s.zoneVel;
  }
  clampZone();

  const dirChangeRate = T.has('erratic') ? tp('erraticRate', 0.03) : p.dirChangeRate;
  if (Math.random() < dirChangeRate * dt * 60) {
    const variance = T.has('erratic') ? tp('erraticSpeedVar', 0.4) : 0.4;
    s.zoneVel = (Math.random() < 0.5 ? 1 : -1) * p.zoneSpeed * (1 - variance / 2 + Math.random() * variance);
  }

  // ── Zone width (mutually exclusive shape traits — first match wins) ──
  const baseWidth = p.zoneWidth;
  if (T.has('shrink')) {
    s.zoneWidth = Math.max(baseWidth * tp('minWidthMult', 0.5), baseWidth - s.catchPct * tp('shrinkRate', 0.15));
  } else if (T.has('warp')) {
    const wMin = tp('warpMin', 0.5), wMax = tp('warpMax', 1.8);
    const t01 = 0.5 + 0.5 * Math.sin(s.elapsed * tp('warpSpeed', 1.6));
    s.zoneWidth = baseWidth * (wMin + (wMax - wMin) * t01);
  } else if (T.has('pulse')) {
    const minMult = 1 / Math.max(1.01, tp('pulseDepth', 2.5));
    const t01 = 0.5 + 0.5 * Math.sin(s.elapsed * tp('pulseSpeed', 2.2));
    s.zoneWidth = baseWidth * (1 - (1 - minMult) * t01);
  } else if (T.has('expand')) {
    ts.expandMult = ts.expandMult === undefined ? 1 : ts.expandMult;
    s.zoneWidth = baseWidth * ts.expandMult;
  } else if (T.has('drift')) {
    ts.driftTimer = (ts.driftTimer || 0) + dt;
    const reset = tp('driftReset', 4.0);
    if (ts.driftTimer >= reset) { ts.driftTimer = 0; }
    s.zonePos += tp('driftSpeed', 0.08) * 60 * dt * (ts.driftTimer < reset / 2 ? 1 : -1);
    s.zoneWidth = baseWidth;
  } else if (T.has('shatter')) {
    ts.shatterTimer = (ts.shatterTimer || 0) + dt;
    const interval = tp('shatterInterval', 2.5), dur = tp('shatterDur', 0.8), pieces = tp('shatterPieces', 3);
    if (ts.shatterTimer >= interval) { ts.shatterActive = true; ts.shatterActiveTimer = 0; ts.shatterTimer = 0; }
    if (ts.shatterActive) {
      ts.shatterActiveTimer += dt;
      s.zoneWidth = baseWidth / pieces;
      if (ts.shatterActiveTimer >= dur) ts.shatterActive = false;
    } else s.zoneWidth = baseWidth;
  } else if (T.has('elastic')) {
    const stretch = tp('elasticStretch', 2.0), decay = tp('elasticDecay', 1.2);
    ts.elasticMult = ts.elasticMult === undefined ? 1 : ts.elasticMult;
    if (s.zonePos - baseWidth / 2 < 2.5 || s.zonePos + baseWidth / 2 > 97.5) ts.elasticMult = stretch;
    ts.elasticMult += (1 - ts.elasticMult) * Math.min(1, decay * dt);
    s.zoneWidth = baseWidth * ts.elasticMult;
  } else if (T.has('crumble')) {
    ts.crumbleAmt = (ts.crumbleAmt || 0) + tp('crumbleRate', 0.03) * dt;
    s.zoneWidth = Math.max(baseWidth * tp('crumbleMin', 0.3), baseWidth - ts.crumbleAmt * baseWidth);
  } else if (T.has('taper')) {
    ts.taperTimer = (ts.taperTimer || 0) + dt;
    const flipInt = tp('taperFlipInterval', 4.0);
    if (flipInt > 0 && ts.taperTimer >= flipInt) { ts.taperTimer = 0; ts.taperFlipped = !ts.taperFlipped; }
    const ratio = tp('taperRatio', 0.35);
    const side = ts.taperFlipped ? (1 - s.zonePos / 100) : (s.zonePos / 100);
    s.zoneWidth = baseWidth * (ratio + (1 - ratio) * side);
  } else if (T.has('morph')) {
    ts.morphSeed = ts.morphSeed === undefined ? Math.random() * 1000 : ts.morphSeed;
    const t01 = 0.5 + 0.5 * Math.sin(s.elapsed * tp('morphSpeed', 1.2) + ts.morphSeed);
    s.zoneWidth = baseWidth * (1 + (t01 - 0.5) * tp('morphRange', 0.9));
  } else if (T.has('heartbeat')) {
    const beatDur = tp('hbBeatDur', 0.15), restDur = tp('hbRestDur', 1.0), depth = tp('hbDepth', 0.5);
    ts.hbPhase = (ts.hbPhase || 0) + dt;
    const cycle = beatDur * 2 + restDur;
    const ph = ts.hbPhase % cycle;
    const inBeat = ph < beatDur || (ph >= beatDur * 1.0 && ph < beatDur * 2);
    s.zoneWidth = inBeat ? baseWidth : baseWidth * (1 - depth);
  } else if (T.has('ripple')) {
    s.zoneWidth = baseWidth * (1 - tp('rippleDepth', 0.1) * Math.sin(s.elapsed * tp('rippleFreq', 3.5)));
  } else {
    // Default base behavior — zone narrows slightly as catch fills (unchanged from before traits existed).
    s.zoneWidth = Math.max(baseWidth * 0.7, baseWidth - s.catchPct * 0.07);
  }

  // ── Hit detection ── (INVERT flips the meaning: you must stay OUTSIDE a gap)
  let hit, inverted = false, gapWidth = null;
  if (T.has('invert')) {
    inverted = true;
    gapWidth = s.zoneWidth * tp('invertWidth', 0.8);
    hit = !(s.cursorPos >= (s.zonePos - gapWidth / 2) && s.cursorPos <= (s.zonePos + gapWidth / 2));
  } else {
    hit = s.cursorPos >= (s.zonePos - s.zoneWidth / 2) && s.cursorPos <= (s.zonePos + s.zoneWidth / 2);
  }

  // REVERSAL — periodically flips the meaning of "hit" entirely
  let reversed = false;
  if (T.has('reversal')) {
    ts.reversalTimer = (ts.reversalTimer || 0) + dt;
    const interval = tp('reversalInterval', 5.0), dur = tp('reversalDur', 1.5);
    if (!ts.reversalActive && ts.reversalTimer >= interval) { ts.reversalActive = true; ts.reversalActiveTimer = 0; ts.reversalTimer = 0; }
    if (ts.reversalActive) {
      ts.reversalActiveTimer += dt;
      reversed = true; hit = !hit;
      if (ts.reversalActiveTimer >= dur) ts.reversalActive = false;
    }
  }

  // ANCHOR — zone briefly locks in place, then releases with a speed burst
  if (T.has('anchor')) {
    ts.anchorTimer = (ts.anchorTimer || 0) + dt;
    const interval = tp('anchorInterval', 4.5), dur = tp('anchorDur', 1.2);
    if (!ts.anchorActive && ts.anchorTimer >= interval) { ts.anchorActive = true; ts.anchorActiveTimer = 0; ts.anchorTimer = 0; ts.anchorPos = s.zonePos; }
    if (ts.anchorActive) {
      ts.anchorActiveTimer += dt;
      s.zonePos = ts.anchorPos;
      if (ts.anchorActiveTimer >= dur) { ts.anchorActive = false; s.zoneVel *= tp('anchorReleaseSpeed', 2.0); }
    }
  }

  if (T.has('expand')) {
    ts.expandMult = Math.max(1, Math.min(tp('expandMax', 1.6), ts.expandMult + (hit ? tp('expandRate', 0.08) : -tp('expandDecay', 0.5)) * dt));
  }
  if (T.has('gravity')) {
    if (!hit) s.zonePos += (50 - s.zonePos) * tp('gravPull', 0.25) * dt;
    else s.zonePos += (s.zonePos > 50 ? 1 : -1) * tp('gravSnap', 1.2) * dt * 10;
    s.zonePos = Math.max(2 + s.zoneWidth / 2, Math.min(98 - s.zoneWidth / 2, s.zonePos));
  }

  // DASH — telegraphed teleport to a new spot
  let dashWarn = false;
  if (T.has('dash')) {
    ts.dashTimer = (ts.dashTimer || 0) + dt;
    if (!ts.dashWarn && ts.dashTimer >= tp('dashInterval', 3.0)) { ts.dashWarn = true; ts.dashWarnTimer = 0; }
    if (ts.dashWarn) {
      dashWarn = true;
      ts.dashWarnTimer += dt;
      if (ts.dashWarnTimer >= tp('dashWarnDur', 1.0)) {
        const minDist = tp('dashMinDist', 25);
        let newPos, tries = 0;
        do { newPos = 5 + Math.random() * 90; tries++; } while (Math.abs(newPos - s.zonePos) < minDist && tries < 10);
        s.zonePos = Math.max(2 + s.zoneWidth / 2, Math.min(98 - s.zoneWidth / 2, newPos));
        ts.dashWarn = false; ts.dashTimer = 0;
      }
    }
  }

  // STUTTER — random brief movement freezes (re-applies the pre-tick position)
  if (T.has('stutter')) {
    if (!ts.stutterActive && Math.random() < tp('stutterChance', 0.01) * dt * 60) { ts.stutterActive = true; ts.stutterTimer = 0; }
    if (ts.stutterActive) {
      s.zonePos = (ts.frozenPos !== undefined) ? ts.frozenPos : s.zonePos;
      ts.stutterTimer += dt;
      if (ts.stutterTimer >= tp('stutterDur', 0.2)) ts.stutterActive = false;
    }
  }
  ts.frozenPos = s.zonePos;

  // BLACKOUT — bar goes dark periodically (visual-only flag, mechanics unaffected)
  let blackout = false, blackoutWarn = false;
  if (T.has('blackout')) {
    if (!ts.blackoutActive) {
      ts.blackoutTimer = (ts.blackoutTimer || 0) + dt;
      if (ts.blackoutTimer >= tp('blackoutInterval', 4.0)) { ts.blackoutActive = true; ts.blackoutActiveTimer = 0; ts.blackoutTimer = 0; }
    }
    if (ts.blackoutActive) {
      ts.blackoutActiveTimer += dt;
      const warn = tp('blackoutWarn', 0.2), dur = tp('blackoutDur', 1.0);
      blackoutWarn = ts.blackoutActiveTimer < warn;
      blackout = ts.blackoutActiveTimer >= warn;
      if (ts.blackoutActiveTimer >= warn + dur) ts.blackoutActive = false;
    }
  }

  // ── Multi-zone / visual traits ── these can add extra catchable or fake zones,
  // and/or modify the main zone's opacity. extraZones entries with real:true also
  // count toward "hit" (so the player can catch any of them); real:false (decoys,
  // phantoms, echoes) never count and exist purely to mislead.
  const extraZones = [];
  let zoneAlpha = 1;
  let displayPos = s.zonePos;

  if (T.has('lens')) {
    displayPos = s.zonePos + tp('lensOffset', 10) * Math.sin(s.elapsed * tp('lensSpeed', 1.8));
  }

  if (T.has('split')) {
    ts.split2Pos = ts.split2Pos === undefined ? (100 - s.zonePos) : ts.split2Pos;
    ts.split2Vel = ts.split2Vel === undefined ? -s.zoneVel * tp('zone2SpeedMult', 0.85) : ts.split2Vel;
    ts.split2Pos += ts.split2Vel * 60 * dt;
    if (ts.split2Pos - s.zoneWidth / 2 < 2) { ts.split2Pos = s.zoneWidth / 2 + 2; ts.split2Vel = Math.abs(ts.split2Vel); }
    if (ts.split2Pos + s.zoneWidth / 2 > 98) { ts.split2Pos = 98 - s.zoneWidth / 2; ts.split2Vel = -Math.abs(ts.split2Vel); }
    const hit2 = s.cursorPos >= (ts.split2Pos - s.zoneWidth / 2) && s.cursorPos <= (ts.split2Pos + s.zoneWidth / 2);
    extraZones.push({ pos: ts.split2Pos, width: s.zoneWidth, alpha: 0.85, real: true });
    if (hit2 && !hit) { hit = true; if (tp('splitHalfSpeed', 'true') === 'true') ts.splitHalfRate = true; }
    else ts.splitHalfRate = false;
  }
  if (T.has('split3')) {
    ts.s3Pos = ts.s3Pos === undefined ? 50 : ts.s3Pos;
    ts.s3Vel = ts.s3Vel === undefined ? s.zoneVel * tp('zone3SpeedMult', 1.1) : ts.s3Vel;
    const w3 = s.zoneWidth * tp('zone3WidthMult', 0.8);
    ts.s3Pos += ts.s3Vel * 60 * dt;
    if (ts.s3Pos - w3 / 2 < 2) { ts.s3Pos = w3 / 2 + 2; ts.s3Vel = Math.abs(ts.s3Vel); }
    if (ts.s3Pos + w3 / 2 > 98) { ts.s3Pos = 98 - w3 / 2; ts.s3Vel = -Math.abs(ts.s3Vel); }
    const hit3 = s.cursorPos >= (ts.s3Pos - w3 / 2) && s.cursorPos <= (ts.s3Pos + w3 / 2);
    extraZones.push({ pos: ts.s3Pos, width: w3, alpha: 0.85, real: true });
    if (hit3) hit = true;
  }
  if (T.has('twin')) {
    const speed = tp('twinPulseSpeed', 2.0), offset = tp('twinOffset', 0.5);
    ts.twinAPos = ts.twinAPos === undefined ? s.zonePos : ts.twinAPos;
    ts.twinBPos = ts.twinBPos === undefined ? (100 - s.zonePos) : ts.twinBPos;
    const aLive = Math.sin(s.elapsed * speed) > 0;
    const bLive = Math.sin(s.elapsed * speed + offset * Math.PI * 2) > 0;
    if (!aLive) zoneAlpha = 0.15;
    const hitB = bLive && s.cursorPos >= (ts.twinBPos - s.zoneWidth / 2) && s.cursorPos <= (ts.twinBPos + s.zoneWidth / 2);
    extraZones.push({ pos: ts.twinBPos, width: s.zoneWidth, alpha: bLive ? 0.85 : 0.15, real: bLive });
    if (hitB) hit = true;
    if (!aLive) hit = false;
  }
  if (T.has('cascade')) {
    const count = tp('cascadeCount', 4), gap = tp('cascadeGap', 0.15), speed = tp('cascadeSpeed', 1.8);
    ts.cascadeOffset = (ts.cascadeOffset || 0) + speed * 20 * dt;
    let cascadeHit = false;
    for (let i = 0; i < count; i++) {
      const pos = ((ts.cascadeOffset + i * (100 / count) + i * gap * 20) % 100);
      const w = s.zoneWidth / count * 1.5;
      if (i === 0) { s.zonePos = pos; s.zoneWidth = w; }
      else extraZones.push({ pos, width: w, alpha: 0.85, real: true });
      if (s.cursorPos >= (pos - w / 2) && s.cursorPos <= (pos + w / 2)) cascadeHit = true;
    }
    hit = cascadeHit;
  }
  if (T.has('clone')) {
    ts.cloneTimer = (ts.cloneTimer || 0) + dt;
    const interval = tp('cloneInterval', 5.0), dur = tp('cloneDur', 2.0);
    if (!ts.cloneActive && ts.cloneTimer >= interval) { ts.cloneActive = true; ts.cloneActiveTimer = 0; ts.cloneTimer = 0; ts.clonePos = s.zonePos; ts.cloneVel = s.zoneVel * tp('cloneSpeedMult', 1.2); }
    if (ts.cloneActive) {
      ts.cloneActiveTimer += dt;
      ts.clonePos += ts.cloneVel * 60 * dt;
      if (ts.clonePos - s.zoneWidth / 2 < 2 || ts.clonePos + s.zoneWidth / 2 > 98) ts.cloneVel = -ts.cloneVel;
      ts.clonePos = Math.max(2 + s.zoneWidth / 2, Math.min(98 - s.zoneWidth / 2, ts.clonePos));
      const hitClone = s.cursorPos >= (ts.clonePos - s.zoneWidth / 2) && s.cursorPos <= (ts.clonePos + s.zoneWidth / 2);
      extraZones.push({ pos: ts.clonePos, width: s.zoneWidth, alpha: 0.85, real: true });
      if (hitClone) hit = true;
      if (ts.cloneActiveTimer >= dur) ts.cloneActive = false;
    }
  }
  if (T.has('phantom')) {
    ts.phantomPos = ts.phantomPos === undefined ? (s.zonePos + tp('phantomShift', 35)) % 100 : ts.phantomPos;
    ts.phantomPos += (Math.random() - 0.5) * 4 * dt;
    extraZones.push({ pos: ts.phantomPos, width: s.zoneWidth, alpha: tp('phantomOpacity', 0.75), real: false });
  }
  if (T.has('echo')) {
    ts.echoHistory = ts.echoHistory || [];
    ts.echoHistory.unshift(s.zonePos);
    const count = tp('echoCount', 3);
    if (ts.echoHistory.length > count * 6) ts.echoHistory.length = count * 6;
    for (let i = 1; i <= count; i++) {
      const idx = i * 5;
      if (ts.echoHistory[idx] !== undefined) {
        extraZones.push({ pos: ts.echoHistory[idx], width: s.zoneWidth, alpha: Math.max(0, 0.5 - i * (tp('echoFade', 2.5) / count) * 0.1), real: false });
      }
    }
  }
  if (T.has('ghost')) {
    zoneAlpha *= tp('ghostAlphaMin', 0.2) + (1 - tp('ghostAlphaMin', 0.2)) * (0.5 + 0.5 * Math.sin(s.elapsed * tp('ghostCycleSpeed', 1.5)));
  }
  if (T.has('invisible')) {
    ts.invisTimer = (ts.invisTimer || 0) + dt;
    const visT = tp('invisVisibleTime', 2.5), hidT = tp('invisHiddenTime', 1.2);
    if (!ts.invisHidden && ts.invisTimer >= visT) { ts.invisHidden = true; ts.invisTimer = 0; }
    else if (ts.invisHidden && ts.invisTimer >= hidT) { ts.invisHidden = false; ts.invisTimer = 0; }
    if (ts.invisHidden) { zoneAlpha *= 0.05; s.zonePos += s.zoneVel * speedMult * (tp('invisSpeedMult', 1.8) - 1) * dm * 60 * dt; }
  }
  if (T.has('split_fade')) {
    ts.fadeTimer = (ts.fadeTimer || 0) + dt;
    const swap = tp('fadeSwapTime', 1.8);
    ts.fadeBPos = ts.fadeBPos === undefined ? (100 - s.zonePos) : ts.fadeBPos;
    if (ts.fadeTimer >= swap) { ts.fadeTimer = 0; ts.fadeShowB = !ts.fadeShowB; }
    if (ts.fadeShowB) {
      zoneAlpha = 0.1;
      const hitFB = s.cursorPos >= (ts.fadeBPos - s.zoneWidth / 2) && s.cursorPos <= (ts.fadeBPos + s.zoneWidth / 2);
      extraZones.push({ pos: ts.fadeBPos, width: s.zoneWidth, alpha: 0.85, real: true });
      hit = hitFB;
    }
  }

  // ── Cursor / catch / tension ──
  let cursorDrift = p.cursorDrift;

  // CURSED — catching the zone slowly worsens your own cursor drift
  if (T.has('cursed')) {
    ts.curseMult = ts.curseMult === undefined ? 1 : ts.curseMult;
    ts.curseMult = Math.min(tp('curseDriftMax', 2.0), ts.curseMult + (hit ? tp('curseDriftRate', 0.02) : 0) * dt * 60);
    cursorDrift *= ts.curseMult;
  }

  // MAGNET — periodically locks the cursor near the zone, then violently repels it
  if (T.has('magnet')) {
    ts.magnetTimer = (ts.magnetTimer || 0) + dt;
    const interval = tp('magnetInterval', 4.0), dur = tp('magnetDur', 1.2);
    if (!ts.magnetActive && ts.magnetTimer >= interval) { ts.magnetActive = true; ts.magnetActiveTimer = 0; ts.magnetTimer = 0; }
    if (ts.magnetActive) {
      ts.magnetActiveTimer += dt;
      if (ts.magnetActiveTimer < dur) {
        s.cursorPos += (s.zonePos - s.cursorPos) * 0.15;
      } else {
        s.cursorPos += (s.cursorPos > s.zonePos ? 1 : -1) * tp('magnetForce', 2.0) * 10 * dt;
        ts.magnetActive = false;
      }
    }
  }

  // SIREN — a pull that draws the cursor toward the zone in pulses, fighting player control
  if (T.has('siren')) {
    ts.sirenTimer = (ts.sirenTimer || 0) + dt;
    const interval = tp('sirenInterval', 2.5), dur = tp('sirenDur', 0.8);
    if (!ts.sirenActive && ts.sirenTimer >= interval) { ts.sirenActive = true; ts.sirenActiveTimer = 0; ts.sirenTimer = 0; }
    if (ts.sirenActive) {
      ts.sirenActiveTimer += dt;
      s.cursorPos += (s.zonePos - s.cursorPos) * tp('sirenPull', 0.04) * 60 * dt;
      if (ts.sirenActiveTimer >= dur) ts.sirenActive = false;
    }
  }

  // MIMIC — zone trails the cursor's own past position with a delay
  if (T.has('mimic')) {
    ts.mimicHistory = ts.mimicHistory || [];
    ts.mimicHistory.push({ t: s.elapsed, pos: s.cursorPos });
    const delay = tp('mimicDelay', 1.0), strength = tp('mimicStrength', 0.7);
    while (ts.mimicHistory.length && s.elapsed - ts.mimicHistory[0].t > delay + 0.5) ts.mimicHistory.shift();
    const target = ts.mimicHistory.find(h => s.elapsed - h.t >= delay);
    if (target) s.zonePos += (target.pos - s.zonePos) * strength * dt * 4;
  }

  s.cursorPos = Math.max(1, Math.min(99, s.cursorPos + (s.pressing ? -p.cursorPush : cursorDrift) * 60 * dt));
  let catchDelta = (hit ? p.catchRate : -p.catchDecay) * 60 * dt;
  if (T.has('split') && hit && ts.splitHalfRate) catchDelta *= 0.5;

  // HAUNTED — progress lost to misses doesn't vanish; it returns later as a delayed drain
  if (T.has('haunted')) {
    ts.hauntQueue = ts.hauntQueue || [];
    if (!hit) ts.hauntQueue.push({ t: s.elapsed, amt: p.catchDecay * 60 * dt });
    const delay = tp('hauntDelay', 2.0), rate = tp('hauntRate', 0.1);
    while (ts.hauntQueue.length && s.elapsed - ts.hauntQueue[0].t >= delay) {
      const h = ts.hauntQueue.shift();
      catchDelta -= h.amt * rate * 10;
    }
  }

  // TIME WARP — progress occasionally freezes or reverses
  if (T.has('timewarp')) {
    if (!ts.twarpActive && Math.random() < tp('twarpChance', 0.2) * dt) { ts.twarpActive = true; ts.twarpActiveTimer = 0; }
    if (ts.twarpActive) {
      ts.twarpActiveTimer += dt;
      catchDelta = (tp('twarpMode', 'freeze') === 'reverse') ? -Math.abs(p.catchRate) * 60 * dt : 0;
      if (ts.twarpActiveTimer >= tp('twarpDur', 0.8)) ts.twarpActive = false;
    }
  }

  s.catchPct = Math.max(0, Math.min(100, s.catchPct + catchDelta));
  s.tension  = Math.max(0, Math.min(100, s.tension + (hit ? -p.tensionRecover : p.tensionRate) * 60 * dt));

  // OVERLOAD — filling catch% too fast triggers a discharge back down
  if (T.has('overload')) {
    ts.overloadCd = Math.max(0, (ts.overloadCd || 0) - dt);
    const instRate = catchDelta / Math.max(dt, 0.001) / 60;
    if (ts.overloadCd <= 0 && instRate > tp('overloadThresh', 0.08)) {
      s.catchPct = Math.max(0, s.catchPct - tp('overloadDrain', 20));
      ts.overloadCd = tp('overloadCooldown', 3.0);
    }
  }

  // DECOY — a fake zone wanders independently; sitting on it while missing the
  // real zone drains extra progress, punishing players who chase the wrong one.
  let decoyPos, decoyWidth;
  if (T.has('decoy')) {
    if (ts.decoyPos === undefined) {
      ts.decoyPos = 20 + Math.random() * 60;
      ts.decoyVel = (Math.random() < 0.5 ? 1 : -1) * p.zoneSpeed * (1 + tp('decoySpeedOffset', 0.3));
    }
    ts.decoyPos += ts.decoyVel * 60 * dt;
    if (ts.decoyPos - s.zoneWidth / 2 < 2) { ts.decoyPos = s.zoneWidth / 2 + 2; ts.decoyVel = Math.abs(ts.decoyVel); }
    if (ts.decoyPos + s.zoneWidth / 2 > 98) { ts.decoyPos = 98 - s.zoneWidth / 2; ts.decoyVel = -Math.abs(ts.decoyVel); }
    decoyPos = ts.decoyPos; decoyWidth = s.zoneWidth;
    const onDecoy = s.cursorPos >= (decoyPos - decoyWidth / 2) && s.cursorPos <= (decoyPos + decoyWidth / 2);
    if (onDecoy && !hit) s.catchPct = Math.max(0, s.catchPct - p.catchRate * 60 * dt * 1.5);
  }

  if (ws && ws.readyState === 1) {
    const payload = { type: "fish_state", catchPct: s.catchPct, tension: s.tension, zonePos: s.zonePos,
                       zoneWidth: s.zoneWidth, cursorPos: s.cursorPos, hit };
    if (inverted) payload.inverted = true;
    if (reversed) payload.reversed = true;
    if (blackout) payload.blackout = true;
    if (blackoutWarn) payload.blackoutWarn = true;
    if (dashWarn) payload.dashWarn = true;
    if (ts.teleWarnActive) payload.teleWarn = true;
    if (ts.flipWarnActive) payload.flipWarn = true;
    if (decoyPos !== undefined) { payload.decoyPos = decoyPos; payload.decoyWidth = decoyWidth; }
    if (zoneAlpha < 0.999) payload.zoneAlpha = zoneAlpha;
    if (displayPos !== s.zonePos) payload.displayPos = displayPos;
    if (extraZones.length) payload.extraZones = extraZones;
    send(ws, payload);
  }

  if (s.tension >= 100) { _fishResolve(uid, 'lose'); return; }
  if (s.catchPct >= 100) { _fishResolve(uid, 'win'); return; }
}

// Single global tick loop for ALL active reel sessions — far cheaper at scale than
// one setInterval per angler, and the natural place to add cross-session bookkeeping
// later if needed. Runs continuously from boot; idle (near-zero cost) when nobody's fishing.
const _fishGlobalTimer = setInterval(() => {
  for (const uid of _fishReelSessions.keys()) _fishTick(uid);
}, FISH_TICK_MS);

// Win/loss resolution — the ONLY place a fish is actually awarded. Reached purely
// from the server's own simulation state, never from a client-reported claim.
function _fishResolve(uid, result) {
  const s = _fishReelSessions.get(uid);
  if (!s) return;
  const client = clients.get(uid);
  const ws = client && client.ws;
  _fishClearSession(uid);
  const now = Date.now();
  _fishCooldowns.set(uid, now);

  if (result === 'lose') {
    if (ws && ws.readyState === 1) send(ws, { type: "fish_resolved", result: "lose", fishId: s.fishId });
    return;
  }

  // result === 'win'
  const fishDef = s.fishDef;
  if (['epic', 'legendary', 'mythic'].includes(fishDef.rarity)) {
    if (!_fishRareLog.has(uid)) _fishRareLog.set(uid, []);
    const log = _fishRareLog.get(uid);
    log.push(now);
    while (log.length && log[0] < now - 3600000) log.shift();
    const threshold = fishDef.rarity === 'mythic' ? 3 : fishDef.rarity === 'legendary' ? 5 : 10;
    if (log.length > threshold) flagAnomaly(uid, `excessive_${fishDef.rarity}_fish`, { count: log.length });
    if (log.length > threshold * 2) {
      flagAnomaly(uid, `excessive_${fishDef.rarity}_fish_blocked`, { count: log.length });
      if (ws && ws.readyState === 1) send(ws, { type: "fish_resolved", result: "lose", fishId: s.fishId, reason: "flagged" });
      return;
    }
  }

  try {
    const save = _getCachedSave(uid);
    if (!save) { if (ws && ws.readyState === 1) send(ws, { type: "fish_resolved", result: "lose", fishId: s.fishId, reason: "no_save" }); return; }
    const p = save.player;
    const mats = p.inventory?.materials || [];
    const idx = mats.findIndex(m => m.id === s.fishId);
    if (idx >= 0) mats[idx] = { ...mats[idx], qty: (mats[idx].qty || 1) + 1 };
    else mats.push({ id: fishDef.id, name: fishDef.name, rarity: fishDef.rarity,
                     type: fishDef.type || 'material', marketValue: fishDef.marketValue || fishDef.sellValue || 1, qty: 1 });
    p.inventory = p.inventory || {}; p.inventory.materials = mats;
    p.stats = p.stats || {}; p.stats.fishCaught = (p.stats.fishCaught || 0) + 1;
    p.stats.fishCounts = p.stats.fishCounts || {}; p.stats.fishCounts[s.fishId] = (p.stats.fishCounts[s.fishId] || 0) + 1;
    _writeSave(uid, save);
    if (ws && ws.readyState === 1) {
      send(ws, { type: "fish_resolved", result: "win", fishId: s.fishId, materials: p.inventory.materials, stats: p.stats });
    }
    _logTx(uid, "fish_catch", { fishId: s.fishId, rarity: fishDef.rarity });
    console.log(`[FISH] uid=${uid} caught=${s.fishId} rarity=${fishDef.rarity}`);
  } catch (e) {
    console.error("[FISH] error:", e.message);
    if (ws && ws.readyState === 1) send(ws, { type: "fish_resolved", result: "lose", fishId: s.fishId, reason: "server_error" });
  }
}

// Resolve a fish def: prefer live ITEM_DB, fallback to MATERIALS_DB
function _getFishDef(fishId) {
  const live = ITEM_DB[fishId];
  if (live) return live;
  return MATERIALS_DB[fishId] || null;
}

// Pick a fish using the subzone's _fishing pool data.
// Returns fishId string, or null if the pool is empty/missing.
function _rollFishFromSubzone(fishingSubzone) {
  const pool = fishingSubzone._fishing;
  if (!pool) return null;

  // Roll rarity via cumulative probability
  const roll = Math.random() * 100;
  let cumulative = 0;
  let pickedRarity = null;
  for (const r of FISH_RARITY_ORDER) {
    const pct = (pool[r] && typeof pool[r].pct === 'number') ? pool[r].pct : FISH_RARITY_PCTS[r];
    cumulative += pct;
    if (roll < cumulative) { pickedRarity = r; break; }
  }
  if (!pickedRarity) pickedRarity = 'common'; // safety fallback

  // Pick a random fish from that rarity tier's list
  const tierFish = (pool[pickedRarity] && Array.isArray(pool[pickedRarity].fish))
    ? pool[pickedRarity].fish.filter(id => id && typeof id === 'string' && id.trim())
    : [];

  if (!tierFish.length) {
    // Tier has no fish — cascade down to find any populated tier
    for (const r of [...FISH_RARITY_ORDER].reverse()) {
      const fb = (pool[r] && Array.isArray(pool[r].fish))
        ? pool[r].fish.filter(id => id && typeof id === 'string' && id.trim())
        : [];
      if (fb.length) return fb[Math.floor(Math.random() * fb.length)];
    }
    return null; // No fish defined in any tier
  }

  return tierFish[Math.floor(Math.random() * tierFish.length)];
}

// Legacy roll for zones without subzone _fishing data — picks from any live materials
function _rollFishLegacy() {
  const fishItems = Object.values(ITEM_DB).filter(i => i.type === 'material');
  if (!fishItems.length) return null;
  return fishItems[Math.floor(Math.random() * fishItems.length)].id;
}

// ── Cooldown enforcement ─────────────────────────────────────────────────────
const _exploreCooldowns = new Map();
const _travelCooldowns  = new Map();
const EXPLORE_COOLDOWN_MS = 9000;
const TRAVEL_COOLDOWN_MS  = 55000;

function _cooldownOk(uid, type) {
  const now = Date.now();
  if (type === "explore") {
    const last = _exploreCooldowns.get(uid) || 0;
    if (now - last < EXPLORE_COOLDOWN_MS) {
      const remaining = Math.ceil((EXPLORE_COOLDOWN_MS - (now - last)) / 1000);
      return { ok: false, remaining };
    }
    _exploreCooldowns.set(uid, now);
    return { ok: true };
  }
  if (type === "travel") {
    const last = _travelCooldowns.get(uid) || 0;
    if (now - last < TRAVEL_COOLDOWN_MS) {
      const remaining = Math.ceil((TRAVEL_COOLDOWN_MS - (now - last)) / 1000);
      return { ok: false, remaining };
    }
    _travelCooldowns.set(uid, now);
    return { ok: true };
  }
  return { ok: true };
}

// ── HP validation ────────────────────────────────────────────────────────────
function _calcServerMaxHp(save) {
  const p = save?.player || {};
  const baseMaxHp = p.baseMaxHp || 100;
  const eq = p.equipment || {};
  const gear = eq.gear || null;
  const accessories = (eq.accessories || []).filter(Boolean);
  const equipmentMhp = (gear?.maxHp || 0) + accessories.reduce((s, a) => s + ((a?.maxHp) || 0), 0);
  return baseMaxHp + equipmentMhp;
}

// ── Save validator ───────────────────────────────────────────────────────────
// Valid learnable actions = live action entities + core actions. Rebuilt after _rebuildItemsFromDB.
function _buildValidLearned() {
  const liveActionIds = Object.values(ITEM_DB)
    .filter(i => i.category === 'action' || i.type === 'action')
    .map(i => i.id);
  return new Set([...liveActionIds, 'basic_attack', 'flee', 'provisions']);
}
const VALID_ACTION_IDS = new Set(); // populated after _rebuildItemsFromDB runs
const VALID_LEARNED = new Set(['basic_attack', 'flee', 'provisions']); // core always valid

function _validateSave(uid, save) {
  if (!save || !save.player) return { valid: true };
  const p = save.player;
  const anomalies = [];
  const fixes = {};
  let needsFix = false;

  const eq = p.equipment || {};
  // ── Migrate legacy atk key → dmg on any saved equipment ──────────────────
  if (eq.gear && eq.gear.atk !== undefined && eq.gear.dmg === undefined) eq.gear.dmg = eq.gear.atk;
  (eq.accessories || []).forEach(a => { if (a && a.atk !== undefined && a.dmg === undefined) a.dmg = a.atk; });
  if (eq.gear) {
    const def = ITEM_DB[eq.gear.id];
    if (!def || def.type !== "gear") {
      anomalies.push({ reason: "invalid_gear", item: eq.gear });
      fixes["player/equipment/gear"] = null; needsFix = true;
    } else if ((eq.gear.dmg||0) !== (def.dmg||0) || (eq.gear.acc||0) !== (def.acc||0) || (eq.gear.maxHp||0) !== (def.maxHp||0)) {
      anomalies.push({ reason: "gear_stat_tamper", item: eq.gear, expected: { dmg: def.dmg||0, acc: def.acc||0, maxHp: def.maxHp||0 } });
      fixes["player/equipment/gear"] = { ...eq.gear, dmg: def.dmg, acc: def.acc||0, maxHp: def.maxHp||0, gearType: def.gearType };
      needsFix = true;
    }
  }
  (eq.accessories || []).forEach((acc, i) => {
    if (!acc) return;
    const def = ITEM_DB[acc.id];
    if (!def) {
      anomalies.push({ reason: "invalid_accessory", item: acc, slot: i });
      fixes[`player/equipment/accessories/${i}`] = null; needsFix = true;
    } else if ((acc.dmg||0) !== (def.dmg||0) || (acc.acc||0) !== (def.acc||0) || (acc.maxHp||0) !== (def.maxHp||0)) {
      anomalies.push({ reason: "accessory_stat_tamper", item: acc, slot: i, expected: { dmg: def.dmg||0, acc: def.acc||0, maxHp: def.maxHp||0 } });
      fixes[`player/equipment/accessories/${i}`] = { ...acc, dmg: def.dmg||0, acc: def.acc||0, maxHp: def.maxHp||0 };
      needsFix = true;
    }
  });

  const inv = p.inventory || {};
  ["gears", "accessories"].forEach(key => {
    (inv[key] || []).forEach((item, i) => {
      if (!item) return;
      if (!ITEM_DB[item.id]) {
        anomalies.push({ reason: "invalid_inv_item", key, index: i, item });
        if (!fixes[`player/inventory/${key}`]) fixes[`player/inventory/${key}`] = (inv[key] || []).filter(it => it && ITEM_DB[it.id]);
        needsFix = true;
      }
    });
  });
  (inv.provisions || []).forEach((item, i) => {
    if (!item) return;
    if (!ITEM_DB[item.id]) {
      anomalies.push({ reason: "invalid_provision", index: i, item });
      if (!fixes["player/inventory/provisions"]) fixes["player/inventory/provisions"] = (inv.provisions || []).filter(it => it && ITEM_DB[it.id]);
      needsFix = true;
    }
  });
  (inv.materials || []).forEach((item, i) => {
    if (!item) return;
    if (!MATERIALS_DB[item.id] && !ITEM_DB[item.id]) {
      anomalies.push({ reason: "invalid_material", index: i, item });
      if (!fixes["player/inventory/materials"]) fixes["player/inventory/materials"] = (inv.materials || []).filter(it => it && (MATERIALS_DB[it.id] || ITEM_DB[it.id]));
      needsFix = true;
    }
  });

  (p.learnedActions || []).forEach((id, i) => {
    if (!VALID_LEARNED.has(id)) {
      anomalies.push({ reason: "invalid_learned_action", id, index: i });
      if (!fixes["player/learnedActions"]) fixes["player/learnedActions"] = (p.learnedActions || []).filter(a => VALID_LEARNED.has(a));
      needsFix = true;
    }
  });

  if (p.gold != null && p.gold < 0) {
    anomalies.push({ reason: "negative_gold", gold: p.gold });
    fixes["player/gold"] = 0; needsFix = true;
  }

  const calcMax = _calcServerMaxHp(save);
  if (p.hp != null && p.hp > calcMax + 10) {
    anomalies.push({ reason: "hp_over_max", hp: p.hp, calcMax });
    fixes["player/hp"] = calcMax; needsFix = true;
  }
  if (p.maxHp != null && Math.abs(p.maxHp - calcMax) > 50) {
    anomalies.push({ reason: "maxhp_mismatch", claimed: p.maxHp, calcMax });
    fixes["player/maxHp"] = calcMax; needsFix = true;
  }

  return { valid: !needsFix, fixes, anomalies };
}

// ── Anomaly flagging ─────────────────────────────────────────────────────────
// ── Anomaly write queue ───────────────────────────────────────────────────────
// flagAnomaly is called on the hot message-handler path. Doing a synchronous
// SQLite INSERT inline blocks the event loop — under a spam attack this stalls
// every other player's messages. We queue anomaly rows and flush them in a
// batch via setImmediate so the event loop stays free. Admin alerts are still
// sent immediately (they're just sends, not DB writes).
const _anomalyQueue = [];
let _anomalyFlushScheduled = false;

function _flushAnomalyQueue() {
  if (!_anomalyQueue.length) { _anomalyFlushScheduled = false; return; }
  const batch = _anomalyQueue.splice(0, _anomalyQueue.length);
  _anomalyFlushScheduled = false; // reset after splice so any anomaly queued during the transaction schedules a new flush
  const insertMany = db.transaction(rows => {
    for (const r of rows) stmt.insertAnomaly.run(r.uid, r.reason, r.details, r.ts);
  });
  try { insertMany(batch); } catch(e) { console.error('[ANOMALY] batch insert error:', e.message); }
}

function flagAnomaly(uid, reason, details) {
  const ts = Date.now();
  const detailStr = details ? JSON.stringify(details) : null;
  // Queue the DB write — flush after current call stack unwinds
  _anomalyQueue.push({ uid, reason, details: detailStr, ts });
  if (!_anomalyFlushScheduled) {
    _anomalyFlushScheduled = true;
    setImmediate(_flushAnomalyQueue);
  }
  console.warn(`[ANOMALY] uid=${uid} reason=${reason}`, details || "");
  // Admin alerts are sent immediately — no need to defer these
  for (const [adminUid, client] of clients.entries()) {
    if (ADMIN_UIDS.has(adminUid) && client.ws && client.ws.readyState === 1) {
      send(client.ws, { type: "admin_flag_alert", uid, reason, details, ts });
    }
  }
  // Resolve flagged player's charName for the notice message
  let flaggedCharName = "A player";
  try {
    const flaggedSave = _getCachedSave(uid);
    if (flaggedSave?.player?.name) flaggedCharName = flaggedSave.player.name;
  } catch(e) {}
  const noticeMsg = { type: "player_flag_notice", charName: flaggedCharName, reason, ts };
  // Send to the flagged player themselves
  const flaggedClient = clients.get(uid);
  if (flaggedClient?.ws?.readyState === 1) send(flaggedClient.ws, noticeMsg);
  // Send to all current party members of the flagged player
  const flaggedPartyId = partySubscriptions.get(uid);
  if (flaggedPartyId) {
    for (const [memberUid, pid] of partySubscriptions.entries()) {
      if (pid === flaggedPartyId && memberUid !== uid) {
        const mc = clients.get(memberUid);
        if (mc?.ws?.readyState === 1) send(mc.ws, noticeMsg);
      }
    }
  }
}

// Eject any players flagged as cheaters during CombatRoom.create:
// - Send them a combat_end "ejected" so the client cleans up
// - Remove them from the party so the remaining members' frames clear
// - Update their persisted lastZone to respawnZone
function _ejectCheatersFromRoom(room, partyId) {
  if (!room._ejectedUids || !room._ejectedUids.length) return;
  for (const { uid, respawnZone, name } of room._ejectedUids) {
    // 1. Send ejection message to the player
    const ejClient = clients.get(uid);
    if (ejClient && ejClient.ws.readyState === 1) {
      send(ejClient.ws, {
        type: "combat_end",
        outcome: "ejected",
        respawnZone,
        members: [],
        enemies: [],
        kills: {}
      });
      // Also notify save_corrected so client resyncs learnedActions
      send(ejClient.ws, { type: "save_corrected", fields: ["player/learnedActions"] });
    }
    // 2. Update their lastZone in save so they land at respawnZone
    try {
      const ejSave = _getCachedSave(uid);
      if (ejSave && ejSave.player) {
        ejSave.player.lastZone = respawnZone;
        _writeSave(uid, ejSave, { skipOwnerSeed: true });
      }
    } catch(e) { console.error("[EJECT] save update error:", e.message); }
    // 3. Remove from party
    const ejPartyId = partySubscriptions.get(uid);
    if (ejPartyId) {
      const ejDoc = dbGetParty(ejPartyId);
      if (ejDoc) {
        const remaining = (ejDoc.members || []).filter(m => m.uid !== uid);
        if (remaining.length === 0) {
          _dissolveParty(ejPartyId);
        } else {
          let newLeader = ejDoc.leader;
          let newLeaderUid = ejDoc.leaderUid;
          if (ejDoc.leaderUid === uid) {
            newLeader = remaining[0].username;
            newLeaderUid = remaining[0].uid || null;
          }
          const updated = { ...ejDoc, leader: newLeader, leaderUid: newLeaderUid,
            members: remaining, ts: Date.now(), combatSignal: null };
          dbSetParty(ejPartyId, updated);
          _pushPartyToSubscribers(ejPartyId, null);
        }
      }
      partySubscriptions.delete(uid);
      // Tell the ejected player their party slot is gone
      if (ejClient && ejClient.ws.readyState === 1) {
        send(ejClient.ws, { type: "party_update", partyId: ejPartyId, data: null });
      }
    }
    console.log(`[EJECT] uid=${uid} name=${name} removed from combat+party, sent to respawn=${respawnZone}`);
  }
}


const TICK_MS         = 1000;
const ENERGY_PER_TICK = 10;
const ENERGY_DELAY_MS = 1000;
const ENERGY_TO_ACT   = 80;
const ENERGY_TO_PLAYER= 80;

const roll  = (a,b) => Math.floor(Math.random()*(b-a+1))+a;
const rng   = ()    => Math.floor(Math.random()*100)+1;
const clamp = (v,a,b) => Math.max(a,Math.min(b,v));

// ══════════════════════════════════════════════════════════════════════════════
//  EFFECT ATOM ENGINE  (Layers 1–4)
//
//  Schema (stored on ability._effects = Array<EffectAtom>):
//
//  Layer 1 – Atom  (required)
//    type       : "damage" | "dot" | "heal" | "hot" | "energy_stop" | "eot" | "shield" | "stat_mod"
//    target     : "self" | "target"   (default: "target")
//    scaling    : "flat" | "pct_hp" | "pct_max_hp" | "gear_dmg"  (default: "flat")
//    value      : [min, max]   — flat range or percentage (0–100)
//    trigger    : "on_hit" | "on_miss" | "on_use" | "on_expire" | "on_stack"  (default: "on_hit")
//    duration   : ms — how long the timed effect persists (dot/hot/energy_stop/shield)
//    tickInterval: ms — tick period for dot/hot/eot  (default: 2000)
//    ticks      : optional explicit count — computed from duration/tickInterval if absent
//
//  Layer 2 – Conditions  (optional array)
//    { check: "target_hp_below",    value: 50   }   — target HP% < value
//    { check: "target_hp_above",    value: 50   }   — target HP% > value
//    { check: "caster_hp_below",    value: 50   }   — caster HP% < value
//    { check: "caster_hp_above",    value: 50   }   — caster HP% > value
//    { check: "caster_has_status",  value: "id" }   — caster has active status by id
//    { check: "target_has_status",  value: "id" }   — target has active status by id
//    { check: "consecutive_hits",   value: N    }   — Nth+ consecutive hit on same target
//
//  Layer 3 – Chains  (optional)
//    onExpire   : EffectAtom — fires when this timed effect expires
//    onStack    : { threshold: N, effect: EffectAtom } — fires when stack count hits N
//
//  Layer 4 – Modifiers  (optional booleans / numbers)
//    chance      : 0–100  — independent proc chance (default: 100)
//    maxStacks   : number — cap on stacks (default: 1). maxStacks=1 means not
//                  stackable (re-applying refreshes the single instance's
//                  duration). maxStacks>=2 means stackable: re-applying adds
//                  a new stack (refreshing the duration of ALL existing
//                  stacks too), up to the cap — at the cap, re-applying just
//                  refreshes all existing stacks' duration without adding one.
//                  `stackable` is derived from this value, not a separate flag.
//    dispellable : bool   — can be purged (future hook)
//    transferOnKill: bool — moves timed effect to the next enemy alive (future hook)
// ══════════════════════════════════════════════════════════════════════════════

/**
 * _compileEffects(d)
 * Converts raw dev-panel action data into a compiled _effects array.
 * Called by _rebuildItemsFromDB for every action entity.
 * Also handles legacy boolean flags so old content still works.
 */
/**
 * _DEFAULT_EFFECT_LABEL
 * Human-readable fallback label per effect atom type, used only when a
 * dev-panel effect's Name field is left blank. Prevents raw internal type
 * strings (e.g. "damage", "dot") from leaking into the combat log as if
 * they were an action/ability name.
 */
const _DEFAULT_EFFECT_LABEL = {
  damage:       "Strike",
  dot:          "Lingering Wound",
  hot:          "Regeneration",
  heal:         "Heal",
  shield:       "Shield",
  stat_mod:     "Effect",
  energy_stop:  "Energy Lock",
  eot: "Energy Overtime",
};

function _compileEffects(d, _trackSource) {
  const effects = [];

  // ── New-style: explicit _effects array from dev panel ────────────────────
  if (Array.isArray(d._effects)) {
    for (const raw of d._effects) {
      if (!raw || !raw.type) continue;
      const effValue = Array.isArray(raw.value)
                        ? raw.value
                        : (raw.valueMin != null || raw.valueMax != null)
                          ? [parseFloat(raw.valueMin)||0, parseFloat(raw.valueMax)||parseFloat(raw.valueMin)||0]
                          : [parseInt(raw.value)||1, parseInt(raw.value)||1];
      // eot defaults to "Energy Overtime", but if the configured range
      // is entirely negative (a drain) the default should read as a drain,
      // not a boost — only matters when the dev left the Name field blank.
      const defaultLabel = (raw.type === "eot" && effValue[0] < 0 && effValue[1] <= 0)
        ? "Energy Drain"
        : _DEFAULT_EFFECT_LABEL[raw.type];
      const atom = {
        // Layer 1
        type:         raw.type,
        target:       raw.target        || "target",
        scaling:      raw.scaling       || "flat",
        value:        effValue,
        trigger:      raw.trigger       || "on_hit",
        // Used only by on_hp_threshold / on_energy_threshold / on_stack_threshold
        // / on_kill_streak / on_interval — see _passesThresholdGate.
        triggerValue: raw.triggerValue != null ? parseFloat(raw.triggerValue) : null,
        duration:     parseInt(raw.duration)     || 0,
        tickInterval: parseInt(raw.tickInterval) || 2000,
        // Used only by type === "stat_mod" — which stat this buff/debuff
        // modifies (dmgDealt/dmgReceived/acc/maxHp/healDealt/healReceived).
        // NOTE: this was missing entirely until now, so every stat_mod
        // effect ever saved always silently fell back to dmgDealt
        // regardless of what was actually selected in the Stat dropdown.
        stat:         raw.stat || "dmgDealt",
        // Layer 2
        conditions:   Array.isArray(raw.conditions) ? raw.conditions : [],
        // Layer 3
        onExpire:     raw.onExpire  || null,
        onStack:      raw.onStack   || null,
        // Layer 4
        chance:       raw.chance != null ? parseFloat(raw.chance) : 100,
        maxStacks:    parseInt(raw.maxStacks) || (raw.stackable ? 99 : 1),
        dispellable:  raw.dispellable  || false,
        transferOnKill: raw.transferOnKill || false,
        // Metadata
        id:           raw.id || raw.type,
        label:        raw.name || raw.label || defaultLabel || "Effect",
      };
      // stackable is derived from maxStacks (>=2), not a separate flag — the
      // dev-panel checkbox was removed in favor of Max Stacks being the single
      // source of truth. Computed after maxStacks above so it stays consistent
      // even for legacy saved content that still carries an old raw.stackable.
      atom.stackable = atom.maxStacks >= 2;
      // Compute tick count from duration + interval (if not explicit)
      if (!atom.ticks && atom.duration > 0) {
        atom.tickCount = Math.max(1, Math.floor(atom.duration / atom.tickInterval));
      } else {
        atom.tickCount = parseInt(raw.ticks) || 1;
      }
      // Tag with a back-reference to the source raw effect object (non-enumerable
      // so it doesn't leak into logs/serialization). Lets callers identify and
      // exclude a specific compiled atom by identity rather than fragile array
      // position, since malformed entries earlier in d._effects can be skipped
      // above and shift indices.
      if (_trackSource) Object.defineProperty(atom, '_srcRaw', { value: raw, enumerable: false });
      effects.push(atom);
    }
  }

  // ── Legacy boolean flags → synthesise equivalent atoms ──────────────────
  // These keep old approved content working without re-submission.
  if (effects.length === 0) {
    if (d.dotEffect) {
      effects.push({ type:"dot", target:"target", scaling:"flat", value:[1,4], trigger:"on_hit",
        duration:6000, tickInterval:2000, tickCount:3, conditions:[], onExpire:null, onStack:null,
        chance:100, stackable:false, maxStacks:1, id:"legacy_dot", label:"DoT" });
    }
    if (d.energyStopEffect) {
      effects.push({ type:"energy_stop", target:"target", scaling:"flat", value:[0,0], trigger:"on_hit",
        duration:6000, tickInterval:6000, tickCount:1, conditions:[], onExpire:null, onStack:null,
        chance:100, stackable:false, maxStacks:1, id:"legacy_energy_stop", label:"Energy Lock" });
    }
    if (d.energyOvertimeEffect) {
      effects.push({ type:"eot", target:"self", scaling:"flat", value:[20,20], trigger:"on_hit",
        duration:6000, tickInterval:2000, tickCount:3, conditions:[], onExpire:null, onStack:null,
        chance:100, stackable:false, maxStacks:1, id:"legacy_eot", label:"Energy Overtime" });
    }
    if (d.healEffect) {
      const healAmount = d.healAmount || [1,4];
      const healTicks  = d.healTicks  || [2000,4000,6000];
      effects.push({ type:"hot", target:"self", scaling:"flat",
        value: Array.isArray(healAmount) ? healAmount : [healAmount, healAmount],
        trigger:"on_hit", duration: Math.max(...healTicks), tickInterval:2000,
        tickCount: healTicks.length, conditions:[], onExpire:null, onStack:null,
        chance:100, stackable:false, maxStacks:1, id:"legacy_heal", label:"HoT" });
    }
  }

  return effects;
}

/**
 * _hasStatus(room, uid, statusId)
 * True if `uid` currently has an active status whose id is `statusId`.
 * Most status types (dot/hot/eot/energy_stop) are stored under the plain
 * key `${uid}:${statusId}`. Stackable shield/stat_mod statuses are stored
 * one Map entry PER STACK under `${uid}:${statusId}:${stackTag}` so each
 * stack can expire independently — so this does a prefix match rather than
 * an exact key lookup, to catch those too.
 */
function _hasStatus(room, uid, statusId) {
  if (!room.statuses) return false;
  const exact = `${uid}:${statusId}`;
  const prefix = exact + ':';
  for (const key of room.statuses.keys()) {
    if (key === exact || key.startsWith(prefix)) return true;
  }
  return false;
}

/**
 * _checkConditions(conditions, room, caster, target, context)
 * Returns true if ALL conditions pass.
 * Works for both PvE (enemies as targets) and PvP (members as targets).
 */
function _checkConditions(conditions, room, caster, target, context) {
  if (!conditions || conditions.length === 0) return true;
  for (const cond of conditions) {
    const check = cond.check;
    const val   = cond.value;
    switch (check) {
      case "target_hp_below": {
        const pct = target.maxHp > 0 ? (target.hp / target.maxHp) * 100 : 0;
        if (pct >= val) return false;
        break;
      }
      case "target_hp_above": {
        const pct = target.maxHp > 0 ? (target.hp / target.maxHp) * 100 : 0;
        if (pct <= val) return false;
        break;
      }
      case "caster_hp_below": {
        const pct = caster.maxHp > 0 ? (caster.hp / caster.maxHp) * 100 : 0;
        if (pct >= val) return false;
        break;
      }
      case "caster_hp_above": {
        const pct = caster.maxHp > 0 ? (caster.hp / caster.maxHp) * 100 : 0;
        if (pct <= val) return false;
        break;
      }
      case "caster_has_status": {
        // Check room.statuses map for active status on caster
        if (!_hasStatus(room, caster.uid, val)) return false;
        break;
      }
      case "target_has_status": {
        if (!_hasStatus(room, target.uid, val)) return false;
        break;
      }
      case "consecutive_hits": {
        // context.consecutiveHits[caster.uid + ':' + target.uid] → count
        const key = `${caster.uid}:${target.uid}`;
        const count = (context && context.consecutiveHits && context.consecutiveHits[key]) || 0;
        if (count < val) return false;
        break;
      }
      default:
        break; // unknown condition — pass (permissive for future compat)
    }
  }
  return true;
}

/**
 * _resolveValue(atom, caster, target, room)
 * Computes the numeric value of an effect atom after applying scaling.
 * room is optional — when provided, active stat_mod buffs/debuffs are included.
 */
function _resolveValue(atom, caster, target, room) {
  const rawVal = atom.value;
  const [mn, mx] = Array.isArray(rawVal) ? rawVal : [parseInt(rawVal)||0, parseInt(rawVal)||0];
  let base = roll(mn, mx);
  switch (atom.scaling) {
    case "gear_dmg":
      base += (caster.equipmentDmg || 0);
      if (room && caster && caster.uid) base += _getStatMod(room, caster.uid, "dmgDealt") + _getStatMod(room, caster.uid, "dmg");
      break;
    case "pct_hp":
      // Percentage of target's current HP
      base = Math.round((base / 100) * (target.hp || 0));
      break;
    case "pct_max_hp":
      // Percentage of target's max HP
      base = Math.round((base / 100) * (target.maxHp || 100));
      break;
    case "flat":
    default:
      break;
  }
  // eot (an energy drain) and stat_mod (a debuff) are the two atom types
  // allowed to resolve negative — they're +/- modifiers, not magnitudes.
  // Every other type (damage, heal, dot, hot, shield, energy_stop) stays
  // floored at 0, since "negative damage" etc. isn't a meaningful concept.
  return (atom.type === "eot" || atom.type === "stat_mod") ? base : Math.max(0, base);
}

/**
 * _buildTickTimes(atom)
 * Returns an array of relative ms offsets for timed effect ticks.
 */
function _buildTickTimes(atom) {
  const ticks = [];
  const interval = atom.tickInterval || 2000;
  const count    = atom.tickCount    || 3;
  for (let i = 1; i <= count; i++) ticks.push(i * interval);
  return ticks;
}

/**
 * _applyDamage(room, target, rawDmg, _event)
 * Applies rawDmg to target after draining any active shield statuses.
 * Returns the actual HP damage dealt (after shield absorption).
 * Mutates target.hp / target.alive and prunes exhausted shields from room.statuses.
 */
function _applyDamage(room, target, rawDmg, _event, _log) {
  if (!room.statuses) room.statuses = new Map();
  // dmgReceived is a "vulnerability" stat — a positive value means this
  // target takes MORE damage from everything (base hits, DoT ticks, bonus
  // damage atoms — anything that funnels through here), a negative value
  // means a flat damage reduction. Applied once, centrally, rather than at
  // every individual attack-resolution call site, so it can't be missed by
  // a damage source that gets added later.
  const dmgReceivedMod = rawDmg > 0 ? _getStatMod(room, target.uid, "dmgReceived") : 0;
  let remaining = dmgReceivedMod !== 0 ? Math.max(0, rawDmg + dmgReceivedMod) : rawDmg;
  const _incomingDmg = remaining; // post-dmgReceived, pre-shield — for the on_absorb check below

  // Drain shields in insertion order (oldest first)
  for (const [key, st] of room.statuses.entries()) {
    if (remaining <= 0) break;
    if (st.uid !== target.uid || st.type !== "shield" || !st.value) continue;
    const absorbed = Math.min(st.value, remaining);
    st.value -= absorbed;
    remaining -= absorbed;
    if (_event && absorbed > 0) {
      _event({ k: "shield_absorb", vu: target.uid, d: absorbed, ef: "buff" });
    }
    if (st.value <= 0) {
      room.statuses.delete(key);
      _fireAmbient(room, target, "on_shield_broken", _log, _event);
    }
  }

  // Hit was fully soaked by shields — never touched HP.
  if (_incomingDmg > 0 && remaining <= 0) _fireAmbient(room, target, "on_absorb", _log, _event);

  const hpDmg = Math.max(0, remaining);
  const wasAlive = target.alive;
  target.hp = Math.max(0, target.hp - hpDmg);
  if (target.hp === 0) target.alive = false;
  if (hpDmg > 0) {
    for (const ally of _teammatesOf(room, target)) _fireAmbient(room, ally, "on_ally_damage_taken", _log, _event, target);
    _fireAmbient(room, target, "on_hp_threshold", _log, _event, null, { metricValue: _hpPct(target) });
  }
  if (wasAlive && !target.alive) {
    _fireAmbient(room, target, "on_death", _log, _event);
    target.killStreak = 0;
    for (const ally of _teammatesOf(room, target)) _fireAmbient(room, ally, "on_ally_death", _log, _event, target);
  }
  return hpDmg;
}

// HP as a 0-100 percentage, used by the on_hp_threshold trigger gate.
function _hpPct(entity) {
  if (!entity.maxHp) return 0;
  return Math.max(0, Math.min(100, (entity.hp / entity.maxHp) * 100));
}

/**
 * _getStatMod(room, uid, stat)
 * Sums all active stat_mod statuses for `uid` and the given stat name.
 * Used to apply temporary buffs/debuffs to dmgDealt, dmgReceived, acc, maxHp,
 * healDealt, healReceived, etc.
 * Note: "heal" and "dmg" are legacy aliases, still summed alongside
 * healReceived/dmgDealt respectively at their call sites, for backward
 * compatibility with effects saved before the dealt/received split.
 */
function _getStatMod(room, uid, stat) {
  if (!room.statuses) return 0;
  let total = 0;
  for (const st of room.statuses.values()) {
    if (st.uid === uid && st.type === "stat_mod" && st.stat === stat) {
      total += (st.value || 0);
    }
  }
  return total;
}

/**
 * _resolveTargets(room, caster, primaryTarget, mode)
 *
 * Central multi-target resolver, shared by both CombatRoom (PvE) and
 * PvPCombatRoom, and by both the action-targeting modes ("Action Type" /
 * ability.targeting) and the effect-targeting modes (atom.target).
 *
 *   room          : CombatRoom or PvPCombatRoom instance
 *   caster        : the member/enemy who used the ability
 *   primaryTarget : the single target already chosen (player selection, or
 *                   AI's chosen target) — used for "enemy"/"ally" and as the
 *                   anchor for "self" detection
 *   mode          : one of the 8 documented targeting values:
 *                   enemy, self, ally, area_enemy, area_ally, area_all,
 *                   random_enemy, random_ally
 *
 * Returns an array of distinct, currently-alive entities the ability/effect
 * should apply to. Always returns an array (possibly empty) so callers can
 * simply iterate — no special-casing needed at call sites.
 */
function _resolveTargets(room, caster, primaryTarget, mode) {
  const isPvP = !!room.isPvP;

  // "Allies" = same side as caster, excluding caster. "Enemies" = other side.
  let allies, enemies;
  if (isPvP) {
    const casterTeam = caster.team || null;
    const sameTeam = (m) => (casterTeam ? m.team === casterTeam : m.uid === caster.uid);
    allies  = room.members.filter(m => m.alive && m.uid !== caster.uid && sameTeam(m));
    enemies = room.members.filter(m => m.alive && !sameTeam(m));
  } else {
    // PvE: caster is either a player member or a hostile/enemy.
    const casterIsMember = (room.members || []).some(m => m.uid === caster.uid);
    if (casterIsMember) {
      allies  = (room.members || []).filter(m => m.alive && m.uid !== caster.uid);
      enemies = (room.enemies || []).filter(e => e.alive);
    } else {
      // Caster is a hostile: its "allies" are other hostiles, its "enemies" are the party.
      allies  = (room.enemies || []).filter(e => e.alive && e.uid !== caster.uid);
      enemies = (room.members || []).filter(m => m.alive);
    }
  }

  switch (mode) {
    case "self":
      return caster.alive ? [caster] : [];
    case "ally":
      return primaryTarget && primaryTarget.alive ? [primaryTarget] : [];
    case "enemy":
      return primaryTarget && primaryTarget.alive ? [primaryTarget] : [];
    case "area_ally":
      return [...(caster.alive ? [caster] : []), ...allies];
    case "area_enemy":
      return enemies;
    case "area_all":
      return [...(caster.alive ? [caster] : []), ...allies, ...enemies];
    case "random_enemy":
      return enemies.length > 0 ? [enemies[Math.floor(Math.random() * enemies.length)]] : [];
    case "random_ally": {
      const pool = [caster, ...allies].filter(e => e.alive);
      return pool.length > 0 ? [pool[Math.floor(Math.random() * pool.length)]] : [];
    }
    default:
      // Unknown/missing targeting — fall back to whatever single target was given.
      return primaryTarget && primaryTarget.alive ? [primaryTarget] : [];
  }
}

/**
 * _evalEffects(room, caster, target, triggerPhase, context, _log, _event)
 *
 * The central effect evaluator — called after a hit (or miss / use).
 *
 *   room        : CombatRoom or PvPCombatRoom instance
 *   caster      : member object that used the ability
 *   target      : enemy (PvE) or member (PvP) being targeted
 *   triggerPhase: "on_hit" | "on_miss" | "on_use"
 *   context     : { ability, actionId, wasHit, consecutiveHits }
 *   _log / _event: the room's local log/event helpers
 *
 * Mutates room.dots / .energyStops / .energyOvertime / .heals / .statuses
 * and generates log + event entries.
 */
const _THRESHOLD_TRIGGERS = new Set(["on_hp_threshold", "on_energy_threshold", "on_stack_threshold", "on_kill_streak", "on_interval"]);

/**
 * _passesThresholdGate(trigger, atom, caster, target, room, context, now)
 * Per-atom gate for the 5 trigger types that carry a configurable number —
 * atom.triggerValue, the dev panel's "Trigger Value" field. Different atoms
 * (even on the same piece of gear) can use different numbers, so this can't
 * be decided by the caller alone — it keeps its own state on
 * room._triggerGateState (a Map keyed by `${uid}:${atom.id}`) and decides,
 * per atom, whether THIS firing actually crosses/reaches/is-due-for that
 * atom's own configured value.
 */
function _passesThresholdGate(trigger, atom, caster, target, room, context, now) {
  if (!room._triggerGateState) room._triggerGateState = new Map();
  const key = `${target.uid}:${atom.id}`;
  const thr = atom.triggerValue != null ? atom.triggerValue : 0;

  if (trigger === "on_interval") {
    const intervalMs = Math.max(500, thr || 5000);
    const next = room._triggerGateState.get(key);
    if (next == null) { room._triggerGateState.set(key, now + intervalMs); return false; } // arm on first sighting, don't fire immediately
    if (now < next) return false;
    room._triggerGateState.set(key, now + intervalMs);
    return true;
  }

  // Everything else needs a "current value" supplied by the caller.
  if (context.metricValue == null) return false;
  const mv = context.metricValue;

  if (trigger === "on_hp_threshold" || trigger === "on_energy_threshold") {
    // Fire on CROSSING the threshold in EITHER direction — e.g. dropping
    // below it for a "panic" effect, or rising back above it for a
    // "recovered" effect. There's no separate direction field, so this
    // covers both without needing one.
    const prev = room._triggerGateState.get(key);
    room._triggerGateState.set(key, mv);
    if (prev == null) return false; // no baseline yet — don't fire on first observation
    const wasAbove = prev > thr, isAbove = mv > thr;
    return wasAbove !== isAbove;
  }

  if (trigger === "on_stack_threshold" || trigger === "on_kill_streak") {
    // Fire once on the transition INTO exactly the configured count — not
    // on every subsequent re-observation at that count, and not above it.
    const prev = room._triggerGateState.get(key);
    room._triggerGateState.set(key, mv);
    if (prev === mv) return false; // no change since last check
    return mv === thr;
  }

  return false;
}

function _evalEffects(room, caster, target, triggerPhase, context, _log, _event) {
  const ability = context.ability;
  if (!ability || !Array.isArray(ability._effects) || ability._effects.length === 0) return;

  // Ensure room has a statuses map (new field, not present on restored rooms)
  if (!room.statuses) room.statuses = new Map();

  const now = Date.now();

  for (const atom of ability._effects) {
    // ── Layer 1: trigger gate ──────────────────────────────────────────────
    // "none" (or absent) means "fires immediately when the action is used" — map to on_use
    const _atomTrigger = (!atom.trigger || atom.trigger === "none") ? "on_use" : atom.trigger;
    if (_atomTrigger !== triggerPhase) continue;

    // ── Layer 1b: trigger VALUE gate (for on_hp_threshold, on_energy_threshold,
    // on_stack_threshold, on_kill_streak, on_interval) ─────────────────────
    // These 5 trigger types carry a configurable number (atom.triggerValue,
    // the "Trigger Value" field in the dev panel) that the caller alone can't
    // gate on — different atoms on the same gear can use different numbers —
    // so the check happens here, per-atom, against state this function keeps
    // on the room.
    if (_THRESHOLD_TRIGGERS.has(_atomTrigger) && !_passesThresholdGate(_atomTrigger, atom, caster, target, room, context, now)) continue;

    // ── Layer 4: chance roll ───────────────────────────────────────────────
    const procChance = atom.chance != null ? atom.chance : 100;
    if (procChance < 100 && rng() > procChance) continue;

    // ── Resolve every entity this atom actually applies to ────────────────
    // Supports all 8 documented targeting modes (enemy, self, ally,
    // area_enemy, area_ally, area_all, random_enemy, random_ally) for both
    // player actions and hostile actions, fanning out to multiple targets
    // where appropriate instead of only ever hitting the single passed-in
    // `target`.
    const atomTargets = _resolveTargets(room, caster, target, atom.target);

    for (const effectTarget of atomTargets) {
      // ── Layer 2: conditions (checked per-target) ─────────────────────────
      if (!_checkConditions(atom.conditions, room, caster, effectTarget, context)) continue;

      // ── Layer 1: apply the atom ──────────────────────────────────────────
      const val = _resolveValue(atom, caster, effectTarget, room);
      const tickTimes = _buildTickTimes(atom).map(t => now + t);

      switch (atom.type) {
        case "damage": {
          // Instant damage (separate from the base hit — use for combo finishers etc.)
          if (!effectTarget.alive) break;
          const dmg = Math.max(1, val);
          const hpDmg = _applyDamage(room, effectTarget, dmg, _event, _log);
          _event({ k:"player_strike", au:caster.uid, vu:effectTarget.uid, d:hpDmg, h:1, an:atom.label || _DEFAULT_EFFECT_LABEL.damage, ef:"dmg" });
          _fireAmbient(room, effectTarget, "on_effect_applied", _log, _event);
          // Check chain: onExpire fires immediately for instant effects
          if (atom.onExpire) _evalChainEffect(atom.onExpire, room, caster, effectTarget, _log, _event);
          break;
        }

        case "dot": {
          if (!effectTarget.alive) break;
          const statusKey = `${effectTarget.uid}:${atom.id}`;
          let _dotPrevCount = 0;
          if (!atom.stackable) {
            // Not stackable: replace the single existing instance (refresh).
            _dotPrevCount = room.dots.some(d => d.uid === effectTarget.uid && d.effectId === atom.id) ? 1 : 0;
            room.dots = room.dots.filter(d => !(d.uid === effectTarget.uid && d.effectId === atom.id));
            room.statuses.delete(statusKey);
          } else {
            // Stackable: refresh every existing stack's tick schedule to the
            // fresh duration, then add a new stack on top — unless already
            // at the cap, in which case just the refresh happens (no new stack).
            const existing = room.dots.filter(d => d.uid === effectTarget.uid && d.effectId === atom.id);
            _dotPrevCount = existing.length;
            for (const d of existing) {
              // Pay out any tick that was already due before this refresh
              // overwrites its schedule, so a stack never silently loses a
              // tick of damage just because it got refreshed at the wrong moment.
              const due = d.ticks.filter(t => t <= now);
              for (let i = 0; i < due.length; i++) _payoutDotTick(room, d, effectTarget, _log, _event);
              d.ticks = tickTimes;
            }
            if (existing.length >= (atom.maxStacks || 1)) break; // at cap — refreshed above, don't add
          }
          room.dots.push({
            uid: effectTarget.uid, caster: caster.uid, name: atom.label || ability.name,
            effectId: atom.id, abilityId: context.actionId || null, ticks: tickTimes, atom,
          });
          room.statuses.set(statusKey, { uid: effectTarget.uid, id: atom.id, expiresAt: now + atom.duration, atom });
          _event({ k:"player_strike", au:caster.uid, vu:effectTarget.uid, d:0, h:1, ef:"debuff", tx:`${effectTarget.name} is affected by ${atom.label || ability.name || _DEFAULT_EFFECT_LABEL.dot}.` });
          _fireOnStackChain(atom, _dotPrevCount + 1, room, caster, effectTarget, _log, _event);
          _fireAmbient(room, effectTarget, "on_stack_gained", _log, _event);
          _fireAmbient(room, effectTarget, "on_stack_threshold", _log, _event, null, { metricValue: _dotPrevCount + 1 });
          _fireAmbient(room, effectTarget, "on_effect_applied", _log, _event);
          _fireAmbient(room, effectTarget, "on_status_self", _log, _event);
          _fireAmbient(room, caster, "on_status_applied_to_target", _log, _event, effectTarget);
          break;
        }

        case "hot": {
          // Heal over time — effectTarget already resolved (self/ally/area_ally/etc.)
          if (!effectTarget.alive) break;
          const hotKey = `${effectTarget.uid}:${atom.id}`;
          let _hotPrevCount = 0;
          if (!atom.stackable) {
            _hotPrevCount = room.heals.some(h => h.uid === effectTarget.uid && h.effectId === atom.id) ? 1 : 0;
            room.heals = room.heals.filter(h => !(h.uid === effectTarget.uid && h.effectId === atom.id));
            room.statuses.delete(hotKey);
          } else {
            const existing = room.heals.filter(h => h.uid === effectTarget.uid && h.effectId === atom.id);
            _hotPrevCount = existing.length;
            for (const h of existing) {
              const due = h.ticks.filter(t => t <= now);
              for (let i = 0; i < due.length; i++) _payoutHealTick(room, h, effectTarget, _event, _log);
              h.ticks = tickTimes;
            }
            if (existing.length >= (atom.maxStacks || 1)) break; // at cap — refreshed above, don't add
          }
          room.heals.push({
            uid: effectTarget.uid, casterUid: caster.uid,
            abilityName: ability.name, abilityId: context.actionId,
            healAmount: atom.value, ticks: tickTimes,
            effectId: atom.id, atom,
          });
          room.statuses.set(hotKey, { uid: effectTarget.uid, id: atom.id, expiresAt: now + atom.duration, atom });
          _fireOnStackChain(atom, _hotPrevCount + 1, room, caster, effectTarget, _log, _event);
          _fireAmbient(room, effectTarget, "on_stack_gained", _log, _event);
          _fireAmbient(room, effectTarget, "on_stack_threshold", _log, _event, null, { metricValue: _hotPrevCount + 1 });
          _fireAmbient(room, effectTarget, "on_effect_applied", _log, _event);
          _fireAmbient(room, effectTarget, "on_status_self", _log, _event);
          _fireAmbient(room, caster, "on_status_applied_to_target", _log, _event, effectTarget);
          break;
        }

        case "energy_stop": {
          if (!effectTarget.alive) break;
          // Remove existing energy stop for this target (refresh). Energy
          // lock is a binary on/off state, so unlike dot/hot/eot it does not
          // support real multi-stack behavior even if Max Stacks > 1 is set.
          room.energyStops = room.energyStops.filter(es => es.uid !== effectTarget.uid);
          room.energyStops.push({ uid: effectTarget.uid, caster: caster.uid, until: now + atom.duration, effectId: atom.id, atom });
          room.statuses.set(`${effectTarget.uid}:${atom.id}`, { uid: effectTarget.uid, id: atom.id, expiresAt: now + atom.duration, atom });
          _event({ k:"player_strike", au:caster.uid, vu:effectTarget.uid, ef:"debuff", tx:`${effectTarget.name}'s energy is locked.`, d:0 });
          _fireAmbient(room, effectTarget, "on_effect_applied", _log, _event);
          _fireAmbient(room, effectTarget, "on_status_self", _log, _event);
          _fireAmbient(room, caster, "on_status_applied_to_target", _log, _event, effectTarget);
          break;
        }

        case "eot": {
          if (!effectTarget.alive) break;
          const eotKey = `${effectTarget.uid}:${atom.id}`;
          let _eotPrevCount = 0;
          if (!atom.stackable) {
            _eotPrevCount = room.energyOvertime.some(b => b.uid === effectTarget.uid && b.effectId === atom.id) ? 1 : 0;
            room.energyOvertime = room.energyOvertime.filter(b => !(b.uid === effectTarget.uid && b.effectId === atom.id));
          } else {
            const existing = room.energyOvertime.filter(b => b.uid === effectTarget.uid && b.effectId === atom.id);
            _eotPrevCount = existing.length;
            for (const b of existing) {
              const due = b.ticks.filter(t => t <= now);
              for (let i = 0; i < due.length; i++) _payoutEnergyBoostTick(room, b, effectTarget, _event, _log);
              b.ticks = tickTimes;
            }
            if (existing.length >= (atom.maxStacks || 1)) {
              // At cap — duration refreshed above; keep the status entry alive too, no new stack.
              room.statuses.set(eotKey, { uid: effectTarget.uid, id: atom.id, expiresAt: now + atom.duration, atom });
              break;
            }
          }
          room.energyOvertime.push({
            uid: effectTarget.uid, caster: caster.uid, abilityName: ability.name,
            ticks: tickTimes, effectId: atom.id, boostAmt: val, atom,
          });
          room.statuses.set(eotKey, { uid: effectTarget.uid, id: atom.id, expiresAt: now + atom.duration, atom });
          _fireOnStackChain(atom, _eotPrevCount + 1, room, caster, effectTarget, _log, _event);
          _fireAmbient(room, effectTarget, "on_stack_gained", _log, _event);
          _fireAmbient(room, effectTarget, "on_stack_threshold", _log, _event, null, { metricValue: _eotPrevCount + 1 });
          _fireAmbient(room, effectTarget, "on_effect_applied", _log, _event);
          _fireAmbient(room, effectTarget, "on_status_self", _log, _event);
          _fireAmbient(room, caster, "on_status_applied_to_target", _log, _event, effectTarget);
          break;
        }

        case "heal": {
          // Instant heal — effectTarget already resolved (self/ally/area_ally/etc.)
          if (!effectTarget.alive) break;
          // "heal" is kept as a legacy alias of "healReceived" for old saved effects.
          const healReceivedMod = _getStatMod(room, effectTarget.uid, "healReceived") + _getStatMod(room, effectTarget.uid, "heal");
          const healDealtMod = _getStatMod(room, caster.uid, "healDealt");
          const maxHpMod = _getStatMod(room, effectTarget.uid, "maxHp");
          const effectiveMaxHp = Math.max(1, effectTarget.maxHp + maxHpMod);
          const amt = Math.max(1, val + healDealtMod + healReceivedMod);
          const _hpBefore = effectTarget.hp;
          effectTarget.hp = Math.max(0, Math.min(effectiveMaxHp, effectTarget.hp + amt));
          const _actualHealed = effectTarget.hp - _hpBefore;
          _event({ k:"heal_tick", au:caster.uid, vu:effectTarget.uid, an:context.actionId, d:amt });
          _fireAmbient(room, effectTarget, "on_effect_applied", _log, _event);
          _fireAmbient(room, effectTarget, "on_heal", _log, _event);
          _fireAmbient(room, caster, "on_heal_cast", _log, _event, effectTarget);
          if (_actualHealed < amt) _fireAmbient(room, effectTarget, "on_overheal", _log, _event);
          for (const ally of _teammatesOf(room, effectTarget)) _fireAmbient(room, ally, "on_ally_heal", _log, _event, effectTarget);
          if (_actualHealed > 0) _fireAmbient(room, effectTarget, "on_hp_threshold", _log, _event, null, { metricValue: _hpPct(effectTarget) });
          if (atom.onExpire) _evalChainEffect(atom.onExpire, room, caster, effectTarget, _log, _event);
          break;
        }

        case "shield": {
          // Absorbs incoming damage (drained by _applyDamage before HP is reduced).
          // Each stack is stored as its OWN status entry under
          // `${uid}:${id}:${tag}` so multiple stacks can each carry their own
          // value/expiry — _applyDamage already drains every "shield"-type
          // status entry for this uid in insertion (oldest-first) order
          // regardless of key, so no change needed there.
          if (!effectTarget.alive) break;
          const shieldBase = `${effectTarget.uid}:${atom.id}`;
          const shieldKeys = [...room.statuses.keys()].filter(k => k.startsWith(shieldBase + ':'));
          const _shieldPrevCount = shieldKeys.length;
          let _shieldAdded = false;
          if (!atom.stackable) {
            for (const k of shieldKeys) room.statuses.delete(k);
            room.statuses.set(`${shieldBase}:0`, {
              uid: effectTarget.uid, id: atom.id, type: "shield",
              value: Math.max(1, val), expiresAt: now + atom.duration, casterUid: caster.uid, atom,
            });
            _shieldAdded = true;
          } else {
            // Refresh every existing stack's duration, then add a new stack
            // on top — unless already at the cap, in which case just the
            // refresh happens (no new stack), matching dot/hot semantics.
            for (const k of shieldKeys) {
              const st = room.statuses.get(k);
              if (st) st.expiresAt = now + atom.duration;
            }
            if (shieldKeys.length < (atom.maxStacks || 1)) {
              room._stackSeq = (room._stackSeq || 0) + 1;
              room.statuses.set(`${shieldBase}:${room._stackSeq}`, {
                uid: effectTarget.uid, id: atom.id, type: "shield",
                value: Math.max(1, val), expiresAt: now + atom.duration, casterUid: caster.uid, atom,
              });
              _shieldAdded = true;
            }
          }
          _event({ k:"player_strike", au:caster.uid, vu:effectTarget.uid, d:0, h:1, ef:"buff",
                   tx:`${effectTarget.name} gains a shield of ${Math.max(1, val)}.` });
          if (_shieldAdded) {
            _fireOnStackChain(atom, _shieldPrevCount + 1, room, caster, effectTarget, _log, _event);
            _fireAmbient(room, effectTarget, "on_shield_gained", _log, _event);
            _fireAmbient(room, effectTarget, "on_stack_gained", _log, _event);
            _fireAmbient(room, effectTarget, "on_stack_threshold", _log, _event, null, { metricValue: _shieldPrevCount + 1 });
          }
          _fireAmbient(room, effectTarget, "on_effect_applied", _log, _event);
          _fireAmbient(room, effectTarget, "on_status_self", _log, _event);
          _fireAmbient(room, caster, "on_status_applied_to_target", _log, _event, effectTarget);
          break;
        }

        case "stat_mod": {
          // Temporary stat buff/debuff — read back by _getStatMod() during damage/accuracy/heal calculations.
          // Same one-entry-per-stack scheme as shield (see above) — _getStatMod
          // already sums every matching status entry regardless of key, so
          // multiple stacks of the same stat_mod now correctly add up.
          if (!effectTarget.alive) break;
          const smBase = `${effectTarget.uid}:${atom.id}`;
          const smKeys = [...room.statuses.keys()].filter(k => k.startsWith(smBase + ':'));
          const _smPrevCount = smKeys.length;
          let _smAdded = false;
          if (!atom.stackable) {
            for (const k of smKeys) room.statuses.delete(k);
            room.statuses.set(`${smBase}:0`, {
              uid: effectTarget.uid, id: atom.id, type: "stat_mod",
              stat: atom.stat || "dmgDealt", value: val, expiresAt: now + atom.duration, casterUid: caster.uid, atom,
            });
            _smAdded = true;
          } else {
            for (const k of smKeys) {
              const st = room.statuses.get(k);
              if (st) st.expiresAt = now + atom.duration;
            }
            if (smKeys.length < (atom.maxStacks || 1)) {
              room._stackSeq = (room._stackSeq || 0) + 1;
              room.statuses.set(`${smBase}:${room._stackSeq}`, {
                uid: effectTarget.uid, id: atom.id, type: "stat_mod",
                stat: atom.stat || "dmgDealt", value: val, expiresAt: now + atom.duration, casterUid: caster.uid, atom,
              });
              _smAdded = true;
            }
          }
          if (_smAdded) {
            _fireOnStackChain(atom, _smPrevCount + 1, room, caster, effectTarget, _log, _event);
            _fireAmbient(room, effectTarget, "on_stack_gained", _log, _event);
            _fireAmbient(room, effectTarget, "on_stack_threshold", _log, _event, null, { metricValue: _smPrevCount + 1 });
          }
          _fireAmbient(room, effectTarget, "on_effect_applied", _log, _event);
          _fireAmbient(room, effectTarget, "on_status_self", _log, _event);
          _fireAmbient(room, caster, "on_status_applied_to_target", _log, _event, effectTarget);
          break;
        }

        default:
          break;
      }
    }
  }
}

/**
 * _evalChainEffect(chainAtom, room, caster, target, _log, _event)
 * Recursively evaluates a chained effect (onExpire / onStack).
 * Only handles instant atom types to avoid infinite loops.
 */
function _evalChainEffect(chainAtom, room, caster, target, _log, _event) {
  if (!chainAtom || !chainAtom.type) return;
  if (!target || !target.alive) return;
  // Conditions must pass against current target state
  if (!_checkConditions(chainAtom.conditions || [], room, caster, target, {})) return;
  const val = _resolveValue(chainAtom, caster, target, room);
  switch (chainAtom.type) {
    case "damage": {
      const dmg = Math.max(1, val);
      const hpDmg = _applyDamage(room, target, dmg, _event, _log);
      _event({ k:"player_strike", au:caster.uid, vu:target.uid, d:hpDmg, h:1, ef:"dmg", tx:chainAtom.label||"Chain damage" });
      break;
    }
    case "heal": {
      const healTarget = chainAtom.target === "self" ? caster : target;
      if (!healTarget || !healTarget.alive) break;
      const _chainHealReceivedMod = _getStatMod(room, healTarget.uid, "healReceived") + _getStatMod(room, healTarget.uid, "heal");
      const _chainHealDealtMod = _getStatMod(room, caster.uid, "healDealt");
      const _chainMaxHpMod = _getStatMod(room, healTarget.uid, "maxHp");
      const _chainEffectiveMaxHp = Math.max(1, healTarget.maxHp + _chainMaxHpMod);
      const amt = Math.max(1, _resolveValue(chainAtom, caster, healTarget, room) + _chainHealDealtMod + _chainHealReceivedMod);
      healTarget.hp = Math.max(0, Math.min(_chainEffectiveMaxHp, healTarget.hp + amt));
      _event({ k:"heal_tick", au:caster.uid, vu:healTarget.uid, d:amt });
      break;
    }
    default:
      break;
  }
}

/**
 * _fireOnStackChain(atom, newStackCount, room, caster, target, _log, _event)
 * Fires atom.onStack.effect the moment a freshly-added stack brings the
 * stack count to exactly atom.onStack.threshold. Call this ONLY right after
 * actually adding a new stack (not on a refresh-only reapplication at cap),
 * so a chain configured for "fires at 3 stacks" fires once on the hit that
 * brings the target to 3 — not on every subsequent reapplication while
 * already at 3.
 */
function _fireOnStackChain(atom, newStackCount, room, caster, target, _log, _event) {
  if (!atom.onStack || !atom.onStack.effect || !atom.onStack.threshold) return;
  if (newStackCount !== atom.onStack.threshold) return;
  _evalChainEffect(atom.onStack.effect, room, caster, target, _log, _event);
}

/**
 * _fireAmbient(room, owner, trigger, _log, _event, primaryTarget)
 * Fires `trigger` against `owner`'s equipped Gear/Accessories ONLY
 * (_evalEquipEffects, not the action-effect pipeline). The new "ambient"
 * trigger types (on_death, on_heal, on_ally_damage_taken, etc.) describe
 * standing observers — "whenever X happens to me" — which only makes sense
 * for equipment that's continuously worn, not for a one-shot action effect
 * that only exists for the instant its own ability is cast. So these fire
 * through the SAME equipment-passive path as on_enter_combat, never through
 * an Action/Provision/Hostile ability's own _effects list.
 * `primaryTarget` lets atom.target="ally" etc. resolve correctly when the
 * triggering event happened to someone other than `owner` (e.g. an ally
 * taking damage) — see _resolveTargets.
 */
function _fireAmbient(room, owner, trigger, _log, _event, primaryTarget, extraContext) {
  _evalEquipEffects(room, owner, primaryTarget || owner, trigger, _log || _AMBIENT_NOOP, _event || _AMBIENT_NOOP, extraContext);
}
const _AMBIENT_NOOP = () => {};

/**
 * _registerKill(room, killer, _log, _event)
 * Bumps killer.killStreak and fires on_kill_streak with the new count.
 * Reset to 0 happens in _applyDamage when the killer themselves dies.
 */
function _registerKill(room, killer, _log, _event) {
  if (!killer) return;
  killer.killStreak = (killer.killStreak || 0) + 1;
  _fireAmbient(room, killer, "on_kill_streak", _log, _event, null, { metricValue: killer.killStreak });
}

/**
 * _teammatesOf(room, member)
 * Other members on the same side as `member`. PvE party members have no
 * .team field at all, so `member.team == null` naturally includes every
 * other party member as an ally. PvP members DO have .team set, so this
 * correctly excludes the opposing team even though both teams sit together
 * in the same room.members array.
 */
function _teammatesOf(room, member) {
  return (room.members || []).filter(m => m.uid !== member.uid && (member.team == null || m.team === member.team));
}

/**
 * _tickTimedEffects(room, now, _log, _event)
 * Processes dot/hot/energyOvertime ticks AND fires onExpire chains when
 * an effect runs out. Called from CombatRoom._tick() replacing the
 * inline forEach loops.
 */
/**
 * _payoutDotTick(room, dot, target, _log, _event)
 * Applies a single DoT tick's damage to target. Shared by the periodic
 * tick loop (_tickTimedEffects) and the pre-refresh drain (so a stack's
 * already-due tick is never silently dropped when a new hit refreshes it).
 */
function _payoutDotTick(room, dot, target, _log, _event) {
  let bdmg;
  if (dot.atom) {
    const dotCasterMember = (room.members || []).find(m => m.uid === dot.caster)
                         || { uid: dot.caster, equipmentDmg: 0 };
    bdmg = Math.max(1, _resolveValue(dot.atom, dotCasterMember, target, room));
  } else {
    bdmg = roll(1, 4);
  }
  const dotHpDmg = _applyDamage(room, target, bdmg, _event, _log);
  if (dot.caster && room.threat != null) {
    if (!room.threat[dot.uid]) room.threat[dot.uid] = {};
    room.threat[dot.uid][dot.caster] = (room.threat[dot.uid][dot.caster] || 0) + dotHpDmg;
  }
  _event({ k:"player_strike", au:dot.caster||"__dot__", vu:dot.uid, d:dotHpDmg, h:1, an:dot.name||"dot", dot:1 });
  const _dotTargetIsMember = (room.members || []).some(m => m.uid === dot.uid);
  if (_dotTargetIsMember) {
    _evalEquipEffects(room, target, target, "on_damage_taken", _log, _event);
  }
  _fireAmbient(room, target, "on_dot_tick", _log, _event);
  _fireAmbient(room, target, "on_effect_tick", _log, _event);
  if (!target.alive && dot.caster) {
    const _dotCasterMember = (room.members || []).find(m => m.uid === dot.caster);
    if (_dotCasterMember) {
      const _dotTargetIsEnemy = (room.enemies || []).some(e => e.uid === dot.uid);
      const _dotTargetIsOpponent = !_dotTargetIsEnemy && (room.members || []).some(m => m.uid === dot.uid);
      if (_dotTargetIsEnemy || _dotTargetIsOpponent) {
        if (dot.abilityId && ACTION_DB[dot.abilityId]) {
          const _dotAbility = ACTION_DB[dot.abilityId];
          _evalEffects(room, _dotCasterMember, target, "on_kill",
            { ability: _dotAbility, actionId: dot.abilityId, wasHit: true }, _log, _event);
        }
        _evalEquipEffects(room, _dotCasterMember, target, "on_kill", _log, _event);
        _registerKill(room, _dotCasterMember, _log, _event);
      }
    }
  }
}

/**
 * _payoutHealTick(room, heal, m, _event)
 * Applies a single HoT tick's healing to member m. Shared by the periodic
 * tick loop and the pre-refresh drain.
 */
function _payoutHealTick(room, heal, m, _event, _log) {
  const [mn, mx] = heal.healAmount || [1, 4];
  const healReceivedMod = _getStatMod(room, m.uid, "healReceived") + _getStatMod(room, m.uid, "heal");
  const healDealtMod = heal.casterUid ? _getStatMod(room, heal.casterUid, "healDealt") : 0;
  const maxHpMod = _getStatMod(room, m.uid, "maxHp");
  const effectiveMaxHp = Math.max(1, m.maxHp + maxHpMod);
  const amt = Math.max(1, roll(mn, mx) + healDealtMod + healReceivedMod);
  const _hpBefore = m.hp;
  m.hp = Math.max(0, Math.min(effectiveMaxHp, m.hp + amt));
  const _actualHealed = m.hp - _hpBefore;
  _event({ k:"heal_tick", au:heal.casterUid, vu:heal.uid, an:heal.abilityId||"hb", d:amt });
  _fireAmbient(room, m, "on_hot_tick", _log, _event);
  _fireAmbient(room, m, "on_effect_tick", _log, _event);
  _fireAmbient(room, m, "on_heal", _log, _event);
  if (heal.casterUid) {
    const _healCaster = (room.members || []).find(mm => mm.uid === heal.casterUid)
                      || (room.enemies || []).find(e => e.uid === heal.casterUid);
    if (_healCaster) _fireAmbient(room, _healCaster, "on_heal_cast", _log, _event, m);
  }
  if (_actualHealed < amt) _fireAmbient(room, m, "on_overheal", _log, _event);
  for (const ally of _teammatesOf(room, m)) _fireAmbient(room, ally, "on_ally_heal", _log, _event, m);
  if (_actualHealed > 0) _fireAmbient(room, m, "on_hp_threshold", _log, _event, null, { metricValue: _hpPct(m) });
}

/**
 * _payoutEnergyBoostTick(room, boost, m, _event)
 * Applies a single energy-boost tick's energy gain to member m (no-op if
 * their energy is currently locked). Shared by the periodic tick loop and
 * the pre-refresh drain.
 */
function _payoutEnergyBoostTick(room, boost, m, _event, _log) {
  if (room.energyStops.some(es => es.uid === m.uid)) return;
  const gain = boost.boostAmt != null ? boost.boostAmt : 20;
  m.energy = clamp(m.energy + gain, 0, 100);
  const evt = { k:"energy_gain", vu:m.uid, d:gain, ef: gain < 0 ? "debuff" : "buff" };
  if (gain < 0) evt.loss = 1;
  _event(evt);
  _fireAmbient(room, m, "on_effect_tick", _log, _event);
  if (gain > 0) _fireAmbient(room, m, "on_energy_gained", _log, _event);
  _fireAmbient(room, m, "on_energy_threshold", _log, _event, null, { metricValue: m.energy });
}

function _tickTimedEffects(room, now, _log, _event) {
  if (!room.statuses) room.statuses = new Map();

  // When a stackable dot/hot/eot has multiple stacks and they ALL expire in
  // this same tick pass, each stack's own "is anything else still going"
  // check can independently conclude "I'm the last one" (since neither
  // stack-being-checked counts itself, and ALL of them are equally expiring)
  // — without this, onExpire/on_effect_expired would fire once PER STACK
  // instead of once for the whole effect. Scoped to a single
  // _tickTimedEffects call, keyed by `${kind}:${uid}:${effectId}`.
  const _expiredThisPass = new Set();

  // ── Ability cooldowns coming off (on_ability_ready) ─────────────────────────
  // No dedicated "cooldown just finished" event exists anywhere else, so this
  // polls every member's cooldowns map each tick. _cdReadyNotified remembers
  // which cooldown TIMESTAMP we already fired for on a given actionId, so a
  // re-used ability that goes on cooldown again later still fires again
  // (new timestamp), but we don't refire every tick while it sits ready.
  for (const m of room.members || []) {
    if (!m.cooldowns) continue;
    for (const aId in m.cooldowns) {
      const cd = m.cooldowns[aId];
      if (cd == null || cd > now) continue;
      if (!m._cdReadyNotified) m._cdReadyNotified = {};
      if (m._cdReadyNotified[aId] === cd) continue;
      m._cdReadyNotified[aId] = cd;
      _fireAmbient(room, m, "on_ability_ready", _log, _event);
    }
  }

  // ── on_interval (Gear/Accessories standing timers) ─────────────────────────
  // on_interval doesn't piggyback on any other game event, so it needs its
  // own poll. The actual "is THIS atom's timer due yet" decision (each atom
  // can use a different ms interval) happens per-atom inside
  // _passesThresholdGate — this just gives every member with an on_interval
  // atom a chance to be checked, every tick.
  for (const m of room.members || []) {
    if (Array.isArray(m.equipmentEffects) && m.equipmentEffects.some(a => ((!a.trigger || a.trigger === "none") ? "on_use" : a.trigger) === "on_interval")) {
      _fireAmbient(room, m, "on_interval", _log, _event);
    }
  }

  // ── DoTs ───────────────────────────────────────────────────────────────────
  const keepDots = [];
  for (const dot of room.dots) {
    const pending = dot.ticks.filter(t => t <= now);
    const future  = dot.ticks.filter(t => t > now);
    for (let i = 0; i < pending.length; i++) {
      // Find the target — enemies in PvE, members in PvP
      const target = (room.enemies || []).find(e => e.uid === dot.uid && e.alive)
                  || (room.members || []).find(m => m.uid === dot.uid && m.alive);
      if (!target) continue;
      _payoutDotTick(room, dot, target, _log, _event);
    }
    if (future.length > 0) {
      keepDots.push({ ...dot, ticks: future });
    } else {
      // A stack just fully expired. With stacking, multiple stacks of the
      // same effect can expire in the same tick pass or in sequence — only
      // treat this as a true "effect expired" event (status cleanup +
      // onExpire chain) once no other stacks of it remain on the target.
      const stillStacked = keepDots.some(d => d.uid === dot.uid && d.effectId === dot.effectId)
        || room.dots.some(d => d !== dot && d.uid === dot.uid && d.effectId === dot.effectId && d.ticks.some(t => t > now));
      const _dotExpireTarget = (room.enemies || []).find(e => e.uid === dot.uid) || (room.members || []).find(m => m.uid === dot.uid);
      if (_dotExpireTarget) _fireAmbient(room, _dotExpireTarget, "on_stack_lost", _log, _event);
      if (!stillStacked) {
        const _dotExpireKey = `dot:${dot.uid}:${dot.effectId}`;
        if (!_expiredThisPass.has(_dotExpireKey)) {
          _expiredThisPass.add(_dotExpireKey);
          // Effect expired — fire onExpire chain if present
          if (dot.atom && dot.atom.onExpire) {
            // Find caster as member
            const caster = (room.members || []).find(m => m.uid === dot.caster);
            const target  = (room.enemies || []).find(e => e.uid === dot.uid)
                         || (room.members || []).find(m => m.uid === dot.uid);
            if (caster && target) _evalChainEffect(dot.atom.onExpire, room, caster, target, _log, _event);
          }
          if (_dotExpireTarget) _fireAmbient(room, _dotExpireTarget, "on_effect_expired", _log, _event);
        }
        if (dot.effectId) room.statuses.delete(`${dot.uid}:${dot.effectId}`);
      }
    }
  }
  room.dots = keepDots;

  // ── Energy stops (simple expiry — no ticks) ────────────────────────────────
  const expiredStops = room.energyStops.filter(es => es.until <= now);
  room.energyStops = room.energyStops.filter(es => es.until > now);
  for (const es of expiredStops) {
    const _esTarget = (room.enemies || []).find(e => e.uid === es.uid) || (room.members || []).find(m => m.uid === es.uid);
    if (es.atom && es.atom.onExpire) {
      const caster = (room.members || []).find(m => m.uid === es.caster);
      const target  = (room.enemies || []).find(e => e.uid === es.uid)
                   || (room.members || []).find(m => m.uid === es.uid);
      if (caster && target) _evalChainEffect(es.atom.onExpire, room, caster, target, _log, _event);
    }
    if (_esTarget) _fireAmbient(room, _esTarget, "on_effect_expired", _log, _event);
    if (es.effectId) room.statuses.delete(`${es.uid}:${es.effectId}`);
  }

  // ── Energy boosts ──────────────────────────────────────────────────────────
  const keepBoosts = [];
  for (const boost of room.energyOvertime) {
    const pending = boost.ticks.filter(t => t <= now);
    const future  = boost.ticks.filter(t => t > now);
    for (let i = 0; i < pending.length; i++) {
      const m = (room.members || []).find(m => m.uid === boost.uid && m.alive);
      if (!m) continue;
      _payoutEnergyBoostTick(room, boost, m, _event, _log);
    }
    if (future.length > 0) {
      keepBoosts.push({ ...boost, ticks: future });
    } else {
      // Same last-stack-only guard as DoTs above.
      const stillStacked = keepBoosts.some(b => b.uid === boost.uid && b.effectId === boost.effectId)
        || room.energyOvertime.some(b => b !== boost && b.uid === boost.uid && b.effectId === boost.effectId && b.ticks.some(t => t > now));
      const _eotExpireTarget = (room.enemies || []).find(e => e.uid === boost.uid) || (room.members || []).find(m => m.uid === boost.uid);
      if (_eotExpireTarget) _fireAmbient(room, _eotExpireTarget, "on_stack_lost", _log, _event);
      if (!stillStacked) {
        const _eotExpireKey = `eot:${boost.uid}:${boost.effectId}`;
        if (!_expiredThisPass.has(_eotExpireKey)) {
          _expiredThisPass.add(_eotExpireKey);
          if (boost.atom && boost.atom.onExpire) {
            const caster = (room.members || []).find(m => m.uid === boost.caster);
            const target  = (room.enemies || []).find(e => e.uid === boost.uid)
                         || (room.members || []).find(m => m.uid === boost.uid);
            if (caster && target) _evalChainEffect(boost.atom.onExpire, room, caster, target, _log, _event);
          }
          if (_eotExpireTarget) _fireAmbient(room, _eotExpireTarget, "on_effect_expired", _log, _event);
        }
        if (boost.effectId) room.statuses.delete(`${boost.uid}:${boost.effectId}`);
      }
    }
  }
  room.energyOvertime = keepBoosts;

  // ── HoTs ───────────────────────────────────────────────────────────────────
  const keepHeals = [];
  for (const heal of room.heals) {
    const pending = heal.ticks.filter(t => t <= now);
    const future  = heal.ticks.filter(t => t > now);
    for (let i = 0; i < pending.length; i++) {
      const m = (room.members || []).find(m => m.uid === heal.uid && m.alive);
      if (!m) continue;
      _payoutHealTick(room, heal, m, _event, _log);
    }
    if (future.length > 0) {
      keepHeals.push({ ...heal, ticks: future });
    } else {
      // Same last-stack-only guard as DoTs above.
      const stillStacked = keepHeals.some(h => h.uid === heal.uid && h.effectId === heal.effectId)
        || room.heals.some(h => h !== heal && h.uid === heal.uid && h.effectId === heal.effectId && h.ticks.some(t => t > now));
      const _hotExpireTarget = (room.members || []).find(m => m.uid === heal.uid);
      if (_hotExpireTarget) _fireAmbient(room, _hotExpireTarget, "on_stack_lost", _log, _event);
      if (!stillStacked) {
        const _hotExpireKey = `hot:${heal.uid}:${heal.effectId}`;
        if (!_expiredThisPass.has(_hotExpireKey)) {
          _expiredThisPass.add(_hotExpireKey);
          // HoT expired — fire onExpire chain if present
          if (heal.atom && heal.atom.onExpire) {
            const caster = (room.members || []).find(m => m.uid === heal.casterUid);
            const target  = (room.members || []).find(m => m.uid === heal.uid);
            if (caster && target) _evalChainEffect(heal.atom.onExpire, room, caster, target, _log, _event);
          }
          if (_hotExpireTarget) _fireAmbient(room, _hotExpireTarget, "on_effect_expired", _log, _event);
        }
        if (heal.effectId) room.statuses.delete(`${heal.uid}:${heal.effectId}`);
      }
    }
  }
  room.heals = keepHeals;

  // ── Prune expired statuses (shield, stat_mod) ──────────────────────────────
  // Collect first so firing a chain (which can itself touch room.statuses,
  // e.g. an onExpire that applies another stat_mod) doesn't mutate the Map
  // out from under this iteration.
  const expiredStatusEntries = [];
  for (const [key, st] of room.statuses.entries()) {
    if (st.expiresAt && st.expiresAt <= now) expiredStatusEntries.push([key, st]);
  }
  for (const [key, st] of expiredStatusEntries) {
    room.statuses.delete(key);
    if (st.type === "shield" || st.type === "stat_mod") {
      const target = (room.enemies || []).find(e => e.uid === st.uid) || (room.members || []).find(m => m.uid === st.uid);
      if (target) _fireAmbient(room, target, "on_stack_lost", _log, _event);
      // Stacks of the same effect live under separate keys (see shield/stat_mod
      // apply logic above) — only treat this as a true "effect expired" event
      // once no other stack of this exact (uid, id, type) remains active.
      const stillActive = [...room.statuses.values()].some(o => o.uid === st.uid && o.id === st.id && o.type === st.type);
      if (!stillActive) {
        if (target) _fireAmbient(room, target, "on_effect_expired", _log, _event);
        if (st.atom && st.atom.onExpire) {
          const caster = (room.members || []).find(m => m.uid === st.casterUid);
          if (caster && target) _evalChainEffect(st.atom.onExpire, room, caster, target, _log, _event);
        }
      }
    }
  }
}

// ── Gold tracking ────────────────────────────────────────────────────────────
const _expectedGold = new Map();
function _checkGold(uid, currentGold) {
  if (_bootGracePeriod) { _expectedGold.set(uid, currentGold); return; } // skip check post-restart
  if (!_expectedGold.has(uid)) { _expectedGold.set(uid, currentGold); return; }
  const expected = _expectedGold.get(uid);
  if (currentGold > expected) {
    flagAnomaly(uid, "gold_discrepancy", { expected, actual: currentGold, diff: currentGold - expected });
  }
  _expectedGold.set(uid, currentGold);
}
function _setExpectedGold(uid, gold) { _expectedGold.set(uid, gold); }

// ── Admin system ─────────────────────────────────────────────────────────────
// LEAD_ADMIN: only these usernames can use /makeadmin and /removeadmin
const LEAD_ADMINS = new Set(["viddle"]);

// ── Admin log forwarder — streams server logs to lead admin terminal ──────────
const _origLog   = console.log.bind(console);
const _origError = console.error.bind(console);
const _origWarn  = console.warn.bind(console);

// Ring buffer — last 200 log lines retained in memory for `serverlog tail`
const _logRing = [];
const _logRingMax = 200;
let   _logVerbose = false;  // when true, console.log lines also stream live

function _logRingPush(entry) {
  _logRing.push(entry);
  if (_logRing.length > _logRingMax) _logRing.shift();
}

function _fwdLog(prefix, args) {
  try {
    const parts = args.map(a => {
      if (typeof a === 'string') return a;
      if (a instanceof Error) return `${a.message} (${(a.stack||'').split('\n')[1]||''}`.trim()+')';
      return JSON.stringify(a);
    });
    const full = parts.join(' ');
    const lines = full.split('\n');
    const text = lines[0] + (lines[1] ? ' | ' + lines[1].trim() : '');
    const entry = { ts: Date.now(), prefix, text };
    _logRingPush(entry);
    // Stream live to any connected lead admins
    for (const [, c] of clients) {
      if (LEAD_ADMINS.has((c.username||'').toLowerCase()) && c.ws && c.ws.readyState === 1) {
        c.ws.send(JSON.stringify({ type:'zone_chat_msg', name:'SRV', msg: prefix+text, zone:'__srvlog__' }));
      }
    }
  } catch(e) {}
}

console.log   = (...a) => {
  _origLog(...a);
  const text = a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' ');
  _logRingPush({ ts: Date.now(), prefix: '[LOG] ', text });
  if (_logVerbose) {
    for (const [, c] of clients) {
      if (LEAD_ADMINS.has((c.username||'').toLowerCase()) && c.ws && c.ws.readyState === 1) {
        c.ws.send(JSON.stringify({ type:'zone_chat_msg', name:'SRV', msg: '[LOG] '+text, zone:'__srvlog__' }));
      }
    }
  }
};
console.error = (...a) => { _origError(...a); _fwdLog('[ERR] ', a); };
console.warn  = (...a) => { _origWarn(...a);  _fwdLog('[WRN] ', a); };
// Admin usernames: loaded from SQLite on boot + lead admins
const ADMIN_USERNAMES = new Set(["viddle"]);
// Load persisted admins from SQLite
try {
  db.exec("CREATE TABLE IF NOT EXISTS admins (username TEXT PRIMARY KEY)");
  const adminRows = db.prepare("SELECT username FROM admins").all();
  for (const row of adminRows) ADMIN_USERNAMES.add(row.username.toLowerCase());
} catch (e) { console.error("[ADMIN] failed to load admin table:", e.message); }
const stmtAddAdmin = db.prepare("INSERT OR IGNORE INTO admins (username) VALUES (?)");
const stmtRemoveAdmin = db.prepare("DELETE FROM admins WHERE username = ?");

// ── Developer usernames (persisted, separate from admins) ────────────────────
const DEV_USERNAMES = new Set();
try {
  db.exec("CREATE TABLE IF NOT EXISTS devs (username TEXT PRIMARY KEY)");
  const devRows = db.prepare("SELECT username FROM devs").all();
  for (const row of devRows) DEV_USERNAMES.add(row.username.toLowerCase());
} catch(e) { console.error("[DEV] failed to load devs table:", e.message); }
const stmtAddDev    = db.prepare("INSERT OR IGNORE INTO devs (username) VALUES (?)");
const stmtRemoveDev = db.prepare("DELETE FROM devs WHERE username = ?");

// ── Dev content tables ───────────────────────────────────────────────────────
try {
  db.exec(`
    CREATE TABLE IF NOT EXISTS dev_entities (
      id TEXT PRIMARY KEY,
      category TEXT NOT NULL,
      name TEXT NOT NULL,
      version INTEGER DEFAULT 1,
      status TEXT DEFAULT 'draft',
      flag TEXT DEFAULT 'Active',
      data TEXT NOT NULL,
      created_by TEXT,
      created_at INTEGER,
      updated_by TEXT,
      updated_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS dev_submissions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      creator TEXT NOT NULL,
      sub_type TEXT NOT NULL,
      category TEXT NOT NULL,
      entity_name TEXT NOT NULL,
      entity_id TEXT,
      version INTEGER DEFAULT 1,
      status TEXT DEFAULT 'pending',
      data TEXT NOT NULL,
      ai_analysis TEXT,
      submitted_at INTEGER,
      reviewed_by TEXT,
      reviewed_at INTEGER,
      reject_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS dev_audit (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      actor TEXT,
      action TEXT,
      category TEXT,
      entity_name TEXT,
      version INTEGER,
      ts INTEGER
    );
    CREATE TABLE IF NOT EXISTS dev_packages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT,
      creator TEXT NOT NULL,
      status TEXT DEFAULT 'draft',
      created_at INTEGER,
      updated_at INTEGER,
      submitted_at INTEGER,
      reviewed_by TEXT,
      reviewed_at INTEGER,
      notes TEXT
    );
    CREATE TABLE IF NOT EXISTS dev_package_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      package_id INTEGER NOT NULL,
      submission_id INTEGER,
      entity_name TEXT,
      category TEXT,
      status TEXT DEFAULT 'pending',
      reject_reason TEXT,
      reviewed_by TEXT,
      reviewed_at INTEGER
    );

    -- Tracks every catalog change so offline players can receive only the diff
    -- since their last session instead of the full catalog on version mismatch.
    -- action: 'upsert' | 'delete'
    -- catalog: 'items' | 'zones' | 'hostiles'
    -- catalog_version: the version counter value at the time of this change
    CREATE TABLE IF NOT EXISTS catalog_changelog (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_id   TEXT NOT NULL,
      category    TEXT NOT NULL,
      catalog     TEXT NOT NULL,
      action      TEXT NOT NULL,
      catalog_version INTEGER NOT NULL,
      ts          INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_changelog_catalog_version
      ON catalog_changelog(catalog, catalog_version);
  `);
} catch(e) { console.error("[DEV] failed to create dev content tables:", e.message); }

// Pre-compiled dev statements
let stmtDevSaveEntity, stmtDevGetEntities, stmtDevGetEntity,
    stmtDevInsertSub, stmtDevGetSubs, stmtDevGetSub,
    stmtDevUpdateSubStatus, stmtDevAudit, stmtDevUpdateEntityStatus,
    stmtDevGetEntityByName, stmtDevGetLiveSubs, stmtDevCountPending;
try {
  stmtDevSaveEntity       = db.prepare(`INSERT OR REPLACE INTO dev_entities (id,category,name,version,status,flag,data,created_by,created_at,updated_by,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
  stmtDevGetEntities      = db.prepare(`SELECT id,name,version,status,flag,updated_at,data FROM dev_entities WHERE category=? ORDER BY name`);
  stmtDevGetEntity        = db.prepare(`SELECT * FROM dev_entities WHERE id=?`);
  stmtDevGetEntityByName  = db.prepare(`SELECT * FROM dev_entities WHERE category=? AND name=? AND status='live' LIMIT 1`);
  stmtDevUpdateEntityStatus= db.prepare(`UPDATE dev_entities SET status=?,updated_by=?,updated_at=? WHERE id=?`);
  stmtDevInsertSub        = db.prepare(`INSERT INTO dev_submissions (creator,sub_type,category,entity_name,entity_id,version,status,data,ai_analysis,submitted_at) VALUES (?,?,?,?,?,?,?,?,?,?)`);
  stmtDevGetSubs          = db.prepare(`SELECT id,creator,sub_type,category,entity_name,entity_id,version,status,submitted_at,reviewed_by,reviewed_at,reject_reason,ai_analysis,data FROM dev_submissions ORDER BY id DESC LIMIT 100`);
  stmtDevGetSub           = db.prepare(`SELECT * FROM dev_submissions WHERE id=?`);
  stmtDevUpdateSubStatus  = db.prepare(`UPDATE dev_submissions SET status=?,reviewed_by=?,reviewed_at=?,reject_reason=? WHERE id=?`);
  stmtDevAudit            = db.prepare(`INSERT INTO dev_audit (actor,action,category,entity_name,version,ts) VALUES (?,?,?,?,?,?)`);
  stmtDevCountPending     = db.prepare(`SELECT COUNT(*) as c FROM dev_submissions WHERE status='pending'`);
  stmtDevGetLiveSubs      = db.prepare(`SELECT id,creator,sub_type,category,entity_name,status FROM dev_submissions WHERE status='pending' ORDER BY id DESC LIMIT 50`);
} catch(e) { console.error('[STMT] failed to compile dev statements:', e.message); }

// ── Pre-compiled statements — each in its own try so one missing table never blocks others ──
let stmtDeleteDevEntity, stmtDeleteDevSubByEntity, stmtUpdateSubEntityId,
    stmtUpdateSubEntityIdAndData, stmtDeleteDevSubById,
    stmtDeletePkgItems, stmtDeletePkg,
    stmtGetDevEntityCount, stmtGetDevEntityLiveCount,
    stmtGetRecentAudit, stmtGetAllDevEntities,
    stmtGetAccountByCharName, stmtGetAllSaves,
    stmtGetLiveHostilesAndBosses, stmtGetLiveZones;
try { stmtDeleteDevEntity          = db.prepare(`DELETE FROM dev_entities WHERE id=?`); } catch(e) { console.error('[STMT] stmtDeleteDevEntity:', e.message); }
try { stmtDeleteDevSubByEntity     = db.prepare(`DELETE FROM dev_submissions WHERE entity_id=?`); } catch(e) { console.error('[STMT] stmtDeleteDevSubByEntity:', e.message); }
try { stmtUpdateSubEntityId        = db.prepare(`UPDATE dev_submissions SET entity_id=? WHERE id=?`); } catch(e) { console.error('[STMT] stmtUpdateSubEntityId:', e.message); }
try { stmtUpdateSubEntityIdAndData = db.prepare(`UPDATE dev_submissions SET entity_id=?,data=? WHERE id=?`); } catch(e) { console.error('[STMT] stmtUpdateSubEntityIdAndData:', e.message); }
try { stmtDeleteDevSubById         = db.prepare(`DELETE FROM dev_submissions WHERE id=?`); } catch(e) { console.error('[STMT] stmtDeleteDevSubById:', e.message); }
try { stmtDeletePkgItems           = db.prepare(`DELETE FROM dev_package_items WHERE package_id=?`); } catch(e) { console.error('[STMT] stmtDeletePkgItems:', e.message); }
try { stmtDeletePkg                = db.prepare(`DELETE FROM dev_packages WHERE id=?`); } catch(e) { console.error('[STMT] stmtDeletePkg:', e.message); }
try { stmtGetDevEntityCount        = db.prepare(`SELECT COUNT(*) as c FROM dev_entities WHERE category=?`); } catch(e) { console.error('[STMT] stmtGetDevEntityCount:', e.message); }
try { stmtGetDevEntityLiveCount    = db.prepare(`SELECT COUNT(*) as c FROM dev_entities WHERE category=? AND status='live'`); } catch(e) { console.error('[STMT] stmtGetDevEntityLiveCount:', e.message); }
try { stmtGetRecentAudit           = db.prepare(`SELECT actor,action,category,entity_name,ts FROM dev_audit ORDER BY ts DESC LIMIT 10`); } catch(e) { console.error('[STMT] stmtGetRecentAudit:', e.message); }
try { stmtGetAllDevEntities        = db.prepare(`SELECT id,category,name,version,status,flag,data,created_by,created_at,updated_by,updated_at FROM dev_entities`); } catch(e) { console.error('[STMT] stmtGetAllDevEntities:', e.message); }
try { stmtGetAccountByCharName     = db.prepare(`SELECT uid,username FROM accounts WHERE lower(charName)=?`); } catch(e) { console.error('[STMT] stmtGetAccountByCharName:', e.message); }
try { stmtGetAllSaves              = db.prepare(`SELECT uid, data FROM saves`); } catch(e) { console.error('[STMT] stmtGetAllSaves:', e.message); }
try { stmtGetLiveHostilesAndBosses = db.prepare(`SELECT id, name, data FROM dev_entities WHERE (category='hostile' OR category='boss') AND status='live'`); } catch(e) { console.error('[STMT] stmtGetLiveHostilesAndBosses:', e.message); }
try { stmtGetLiveZones             = db.prepare(`SELECT id, name, data FROM dev_entities WHERE category='zone' AND status='live'`); } catch(e) { console.error('[STMT] stmtGetLiveZones:', e.message); }

// Changelog statements — populated after DB is ready
let stmtChangelogInsert, stmtChangelogSince;
try {
  stmtChangelogInsert = db.prepare(
    `INSERT INTO catalog_changelog (entity_id,category,catalog,action,catalog_version,ts) VALUES (?,?,?,?,?,?)`
  );
  // Fetch all changes for a catalog since a given version, ordered oldest-first.
  // Caller deduplicates by entity_id keeping the latest action.
  stmtChangelogSince  = db.prepare(
    `SELECT entity_id, category, catalog, action, catalog_version
     FROM catalog_changelog
     WHERE catalog=? AND catalog_version > ?
     ORDER BY id ASC`
  );
} catch(e) { console.error('[CHANGELOG] failed to prepare statements:', e.message); }

// Map each item/entity category to its parent catalog name
function _catalogFor(category) {
  if (category === 'zone') return 'zones';
  if (category === 'hostile' || category === 'boss') return 'hostiles';
  return 'items'; // equipment, action, material, provision, gears, accessories
}

// Record a catalog change. Call after the in-memory rebuild and version bump.
function _logCatalogChange(entityId, category, action) {
  if (!stmtChangelogInsert) return;
  try {
    const catalog = _catalogFor(category);
    const ver = catalog === 'zones' ? ZONE_DB_VERSION
              : catalog === 'hostiles' ? ENEMY_DB_VERSION
              : ITEM_DB_VERSION;
    stmtChangelogInsert.run(entityId, category, catalog, action, ver, Date.now());
  } catch(e) { console.error('[CHANGELOG] insert error:', e.message); }
}

// Package statements
let stmtPkgInsert, stmtPkgGet, stmtPkgGetAll, stmtPkgUpdate, stmtPkgUpdateStatus,
    stmtPkgItemInsert, stmtPkgItemsGet, stmtPkgItemUpdateStatus, stmtPkgItemGet;
try {
  stmtPkgInsert       = db.prepare(`INSERT INTO dev_packages (name,description,creator,status,created_at,updated_at) VALUES (?,?,?,?,?,?)`);
  stmtPkgGet          = db.prepare(`SELECT * FROM dev_packages WHERE id=?`);
  stmtPkgGetAll       = db.prepare(`SELECT * FROM dev_packages ORDER BY id DESC LIMIT 50`);
  stmtPkgUpdate       = db.prepare(`UPDATE dev_packages SET name=?,description=?,updated_at=? WHERE id=?`);
  stmtPkgUpdateStatus = db.prepare(`UPDATE dev_packages SET status=?,reviewed_by=?,reviewed_at=?,notes=?,updated_at=? WHERE id=?`);
  stmtPkgItemInsert   = db.prepare(`INSERT INTO dev_package_items (package_id,submission_id,entity_name,category,status) VALUES (?,?,?,?,?)`);
  stmtPkgItemGet      = db.prepare(`SELECT * FROM dev_package_items WHERE id=?`);
  stmtPkgItemsGet     = db.prepare(`SELECT * FROM dev_package_items WHERE package_id=? ORDER BY id`);
  stmtPkgItemUpdateStatus = db.prepare(`UPDATE dev_package_items SET status=?,reject_reason=?,reviewed_by=?,reviewed_at=? WHERE id=?`);
} catch(e) { console.error("[DEV] failed to prepare package statements:", e.message); }

// Boot: repair any dev_entities rows where row id doesn't match data.id (e.g. from typo-fix edits)
try {
  const allRows = stmtGetAllDevEntities.all();
  for (const row of allRows) {
    try {
      const d = JSON.parse(row.data || '{}');
      if (d.id && d.id !== row.id) {
        console.log(`[DB REPAIR] Fixing entity row id mismatch: "${row.id}" → "${d.id}"`);
        stmtDeleteDevEntity.run(row.id);
        db.prepare(`INSERT OR REPLACE INTO dev_entities (id,category,name,version,status,flag,data,created_by,created_at,updated_by,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
          .run(d.id, row.category, row.name, row.version, row.status, row.flag, row.data, row.created_by, row.created_at, row.updated_by, row.updated_at);
        // Fix any submission rows pointing to the old entity_id
        stmtUpdateSubEntityId.run(d.id, row.id);
      }
    } catch(er) { console.error('[DB REPAIR] row error:', row.id, er.message); }
  }
} catch(e) { console.error('[DB REPAIR] boot repair failed:', e.message); }

// Boot sequence: items first, then enemies, then zones, then hostile index
try { _rebuildItemsFromDB(); }      catch(e) { console.error("[ITEMS] boot rebuild failed:", e.message); }
try { _rebuildEnemiesFromDB(); }    catch(e) { console.error("[ENEMIES] boot rebuild failed:", e.message); }
try { _rebuildLiveZones(); }        catch(e) { console.error("[ZONE_DB] boot rebuild failed:", e.message); }
try { _rebuildZoneHostileIndex(); } catch(e) { console.error("[HOSTILE INDEX] boot rebuild failed:", e.message); }


function _isLeadAdmin(uid) {
  const client = clients.get(uid);
  return client && LEAD_ADMINS.has((client.username || "").toLowerCase());
}
// Keep ADMIN_UIDS for backward compat in existing checks - populated on auth
const ADMIN_UIDS = new Set();

// ── Connected clients ────────────────────────────────────────────────────────
const clients = new Map(); // uid → { ws, uid, username, zone }

// ── WS Presence ──────────────────────────────────────────────────────────────
const _usernameToUid = new Map();
const _uidToFriends = new Map();
// Reverse index: lowercase username → Set<uid> of connected players who have that username as a friend.
// Kept in sync with _uidToFriends at every set/delete/clear site.
// Turns the Redis presence fan-out from O(all clients) to O(friends of the broadcasting player).
const _friendsOfMe = new Map(); // username -> Set<uid>

// ── _friendsOfMe helpers ──────────────────────────────────────────────────────
function _fomAdd(ownerUid, friendUsername) {
  let s = _friendsOfMe.get(friendUsername);
  if (!s) { s = new Set(); _friendsOfMe.set(friendUsername, s); }
  s.add(ownerUid);
}
function _fomRemove(ownerUid, friendUsername) {
  const s = _friendsOfMe.get(friendUsername);
  if (!s) return;
  s.delete(ownerUid);
  if (!s.size) _friendsOfMe.delete(friendUsername);
}
// Replace the entire friend set for a uid, keeping _friendsOfMe in sync.
function _setFriends(uid, newFriendNames) {
  const old = _uidToFriends.get(uid);
  if (old) { for (const n of old) _fomRemove(uid, n); }
  _uidToFriends.set(uid, newFriendNames);
  for (const n of newFriendNames) _fomAdd(uid, n);
}
// Add a single friend name to an existing set (mutual-awareness fast path).
function _addFriend(uid, friendUsername) {
  const s = _uidToFriends.get(uid);
  if (s) { s.add(friendUsername); _fomAdd(uid, friendUsername); }
}
// Remove all friend entries for a uid on disconnect/wipe.
function _deleteFriends(uid) {
  const old = _uidToFriends.get(uid);
  if (old) { for (const n of old) _fomRemove(uid, n); }
  _uidToFriends.delete(uid);
}

function _notifyFriendsOfStatus(uid, username, online) {
  const friends = _uidToFriends.get(uid);
  if (!friends) return;
  let zone = null;
  const c = clients.get(uid);
  if (online) {
    // Online: prefer in-memory zone; fall back to accounts.zone which is now written
    // on every login and zone_update so it always reflects the current location.
    if (c && c.zone) {
      zone = c.zone;
    } else {
      const dbRow = stmt.getAccount.get(username.toLowerCase());
      if (dbRow && dbRow.zone) { zone = dbRow.zone; if (c) c.zone = zone; }
    }
  } else {
    // Offline: include last zone so friends see location immediately, not "?".
    // accounts.zone is authoritative — written on login and every zone_update.
    // Only hit the DB if zone isn't still in memory (e.g. high-turnover reconnect storms).
    if (c && c.zone) {
      zone = c.zone; // still in memory at disconnect moment — skip DB read
    } else {
      const dbRow = stmt.getAccount.get(username.toLowerCase());
      if (dbRow && dbRow.zone) {
        zone = dbRow.zone;
      } else {
        const save = _getCachedSave(uid);
        if (save && save["p/lz"]) zone = save["p/lz"];
        else if (save && save.player && save.player.lastZone) zone = save.player.lastZone;
      }
    }
  }
  const msg = { type: online ? "friend_online" : "friend_offline", name: username, zone };
  for (const friendName of friends) {
    const friendUid = _usernameToUid.get(friendName);
    if (!friendUid) continue;
    const friendClient = clients.get(friendUid);
    if (friendClient && friendClient.ws.readyState === 1) send(friendClient.ws, msg);
  }
}

function _sendOnlineFriends(ws, uid) {
  const friends = _uidToFriends.get(uid);
  if (!friends) return;
  const onlineNames = [];
  const zones = {};
  for (const friendName of friends) {
    const friendUid = _usernameToUid.get(friendName);
    if (friendUid && clients.has(friendUid)) {
      onlineNames.push(friendName);
      const fc = clients.get(friendUid);
      // Prefer in-memory zone. Fall back to accounts.zone (written on login + every
      // zone_update) so the correct zone shows even before zone_update fires.
      if (fc && fc.zone) {
        zones[friendName] = fc.zone;
      } else {
        const dbRow = stmt.getAccount.get(friendName);
        if (dbRow && dbRow.zone) { zones[friendName] = dbRow.zone; if (fc) fc.zone = dbRow.zone; }
      }
    }
  }
  if (onlineNames.length > 0) send(ws, { type: "friends_online", names: onlineNames, zones });
  // Send last-known zone for offline friends.
  // accounts.zone is the most reliable source — written on every login and zone_update.
  // Fall back to save file for accounts that haven't logged in since the fix was deployed.
  const offlineZones = {};
  for (const friendName of friends) {
    const onlineUid = _usernameToUid.get(friendName);
    if (onlineUid && clients.has(onlineUid)) continue; // online — handled above
    const dbRow = stmt.getAccount.get(friendName);
    if (dbRow && dbRow.zone) {
      offlineZones[friendName] = dbRow.zone;
      continue;
    }
    // Fallback: load save and read lastZone directly
    let friendUid = onlineUid;
    if (!friendUid && dbRow && dbRow.uid) friendUid = dbRow.uid;
    if (!friendUid) continue;
    const save = _getCachedSave(friendUid);
    if (save && save["p/lz"]) offlineZones[friendName] = save["p/lz"];
    else if (save && save.player && save.player.lastZone) offlineZones[friendName] = save.player.lastZone;
  }
  if (Object.keys(offlineZones).length > 0) send(ws, { type: "friends_offline_zones", zones: offlineZones });
}

// ── Save cache (session-persistent, write-through) ───────────────────────────
// ── Save cache with LRU eviction ─────────────────────────────────────────────
// Caps at 500 entries. On overflow, evicts the oldest inserted entry.
// Map insertion order is guaranteed in JS — oldest key = first key.
// Without this, long-running servers accumulate stale saves for every player
// who ever logged in, turning the cache into a memory leak.
const _SAVE_CACHE_MAX = 500;
const _saveCache = new Map();

function _saveCacheSet(uid, save) {
  if (_saveCache.has(uid)) _saveCache.delete(uid); // refresh insertion order
  _saveCache.set(uid, { save });
  if (_saveCache.size > _SAVE_CACHE_MAX) {
    // Evict the oldest entry (first key in insertion order)
    _saveCache.delete(_saveCache.keys().next().value);
  }
}

// One-time migration: rename equipment.weapon→gear and inventory.weapons→gears
// in a player save. Returns true if the save was mutated.
function _migrateSaveWeaponToGear(save) {
  if (!save || !save.player) return false;
  let dirty = false;
  const p = save.player;
  // equipment.weapon → equipment.gear
  if (p.equipment && p.equipment.weapon !== undefined && p.equipment.gear === undefined) {
    p.equipment.gear = p.equipment.weapon;
    delete p.equipment.weapon;
    dirty = true;
  }
  // inventory.weapons → inventory.gears
  if (p.inventory && p.inventory.weapons !== undefined && p.inventory.gears === undefined) {
    p.inventory.gears = p.inventory.weapons;
    delete p.inventory.weapons;
    dirty = true;
  }
  // Patch type:'weapon' on equipped gear and inventory items
  if (p.equipment?.gear?.type === 'weapon') { p.equipment.gear.type = 'gear'; dirty = true; }
  if (Array.isArray(p.inventory?.gears)) {
    p.inventory.gears.forEach(item => { if (item?.type === 'weapon') { item.type = 'gear'; dirty = true; } });
  }
  return dirty;
}

function _getCachedSave(uid) {
  const cached = _saveCache.get(uid);
  if (cached) return cached.save;
  const save = dbGetSave(uid);
  if (save) {
    if (_migrateSaveWeaponToGear(save)) {
      // Mark dirty so the migrated save is persisted on next flush
      _saveCacheSet(uid, save);
      _saveDirty.add(uid);
    } else {
      _saveCacheSet(uid, save);
    }
  }
  return save;
}
function _invalidateSaveCache(uid) { _saveCache.delete(uid); _saveDirty.delete(uid); }

// ── Dirty-write batch for saves ──────────────────────────────────────────────
// _writeSave marks the uid dirty and updates the cache. A 2.5s interval flushes
// all dirty saves in a single transaction. High-value paths (buy/sell/equip/craft/
// combat end) still write synchronously inside their own db.transaction() blocks —
// those call _saveCacheSet directly and never touch _saveDirty.
// On shutdown, _flushSaveDirty() is called explicitly before db.close().
const _saveDirty = new Set();

function _flushSaveDirty() {
  if (!_saveDirty.size) return;
  const batch = [..._saveDirty];
  _saveDirty.clear();
  try {
    db.transaction(() => {
      for (const uid of batch) {
        const cached = _saveCache.get(uid);
        if (cached) stmt.upsertSave.run(uid, JSON.stringify(cached.save));
      }
    })();
  } catch(e) { console.error("[SAVE FLUSH] error:", e.message); }
}
setInterval(_flushSaveDirty, 2500);

// Write save to cache and mark dirty for batch flush.
// Re-seeds ownership index unless skipOwnerSeed is set.
// Pass { sync: true } for paths that must hit SQLite immediately (e.g. admin writes
// to offline players whose save may not be flushed before next read).
function _writeSave(uid, save, { skipOwnerSeed = false, sync = false } = {}) {
  _saveCacheSet(uid, save);
  if (sync) {
    dbSetSave(uid, save);
    _saveDirty.delete(uid);
  } else {
    _saveDirty.add(uid);
  }
  // Only re-seed if this player is online — avoids work for offline admin writes
  if (!skipOwnerSeed && clients.has(uid)) _seedItemOwners(uid, save);
}

// Apply nested path fix to a save object (e.g. "player/gold" → save.player.gold = value)
function _applyFixes(save, fixes) {
  for (const [path, value] of Object.entries(fixes)) {
    const parts = path.split("/");
    let obj = save;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!obj[parts[i]]) obj[parts[i]] = {};
      obj = obj[parts[i]];
    }
    obj[parts[parts.length - 1]] = value;
  }
  return save;
}

// ── Client version — increment before every deploy ───────────────────────────
// IMPORTANT: kill_timeout in fly.toml must be >= 25 seconds for safe shutdown
const CLIENT_VERSION = 1;

// Fly.io injects FLY_REGION at runtime — read it and build a human-readable label
const FLY_REGION = process.env.FLY_REGION || null;
const REGION_LABELS = {
  lax:"LAX · Los Angeles", iad:"IAD · Ashburn", ord:"ORD · Chicago",
  dfw:"DFW · Dallas", mia:"MIA · Miami", sea:"SEA · Seattle",
  ewr:"EWR · Newark", sjc:"SJC · San Jose", cdg:"CDG · Paris",
  lhr:"LHR · London", ams:"AMS · Amsterdam", fra:"FRA · Frankfurt",
  mad:"MAD · Madrid", waw:"WAW · Warsaw", sto:"STO · Stockholm",
  nrt:"NRT · Tokyo", sin:"SIN · Singapore", hkg:"HKG · Hong Kong",
  bom:"BOM · Mumbai", syd:"SYD · Sydney", gru:"GRU · São Paulo",
};
const SERVER_REGION = FLY_REGION ? (REGION_LABELS[FLY_REGION] || FLY_REGION.toUpperCase()) : "Local";
let _serverNeedsReload = false; // set true after SIGTERM so auth_ok includes needsReload
let _bootGracePeriod = true;    // true for 60s after boot — skip gold discrepancy checks
setTimeout(() => { _bootGracePeriod = false; }, 60000);

// ── Rejoin queue: holds rejoin_combat requests that arrive before boot restore ─
const _rejoinQueue = []; // { uid, ws, roomId }
let _bootRestoreComplete = false;

// ── Combat rooms ─────────────────────────────────────────────────────────────
const rooms = new Map();

// ── Boot restore: load combat rooms saved on previous SIGTERM ────────────────
async function _restoreCombatRoomsFromSQLite() {
  try {
    const rows = stmt.getAllCombatRooms.all();
    if (rows.length === 0) { _bootRestoreComplete = true; _processRejoinQueue(); return; }
    console.log(`[BOOT] Restoring ${rows.length} combat room(s) from SQLite...`);
    const now = Date.now();
    for (const row of rows) {
      try {
        // Skip rooms that were already restored once and are older than 10 minutes
        // (indicates a hard crash after restore — don't restore stale state again)
        if (row.restored_at && (now - row.restored_at) > 600000) {
          console.log(`[BOOT] Skipping stale room ${row.party_id} (restored ${Math.floor((now-row.restored_at)/1000)}s ago)`);
          stmt.deleteCombatRoom.run(row.party_id);
          continue;
        }
        const data = JSON.parse(row.room_data);
        // Check if berserk timer already expired during downtime
        const berserksAt = data.berserksAt || (data.combatAt + 1800000);
        const berserksIn = berserksAt - now;
        if (berserksIn <= 0) {
          console.log(`[BOOT] Room ${row.party_id} berserk timer expired during downtime — skipping restore`);
          stmt.deleteCombatRoom.run(row.party_id);
          continue;
        }
        // Reconstruct the CombatRoom with restored state
        const room = new CombatRoom(data.partyId, data.members, data.enemies);
        // Recompile equipmentEffects for each member from the live ITEM_DB —
        // the serialized save only stores id/name/stats, never _effects arrays,
        // so passive equipment triggers (on_hit, on_kill, on_damage_taken etc.)
        // would be silently dead after a restart without this step.
        for (const m of room.members) {
          const mSave = _getCachedSave(m.uid);
          if (mSave?.player?.equipment) {
            m.equipmentEffects = _compileEquipmentEffects(mSave.player.equipment);
          } else {
            m.equipmentEffects = [];
          }
        }
        room.dots = (data.dots || []).filter(b => b.ticks && b.ticks.some(t => t > now));
        room.energyStops = (data.energyStops || []).filter(es => es.until > now);
        // Rebuild timed effects with remaining durations
        room.energyOvertime = (data.energyOvertime || []).filter(b => b.ticks && b.ticks.some(t => t > now));
        room.heals = (data.heals || []).filter(h => h.ticks && h.ticks.some(t => t > now));
        // Rebuild the statuses Map from surviving dots and heals so condition checks
        // (caster_has_status, target_has_status) work correctly after restart.
        room.statuses = new Map();
        for (const dot of room.dots) {
          if (dot.effectId && dot.atom && dot.ticks.length > 0) {
            const expiresAt = Math.max(...dot.ticks);
            room.statuses.set(`${dot.uid}:${dot.effectId}`, { uid: dot.uid, id: dot.effectId, expiresAt, atom: dot.atom });
          }
        }
        for (const heal of room.heals) {
          if (heal.effectId && heal.atom && heal.ticks.length > 0) {
            const expiresAt = Math.max(...heal.ticks);
            room.statuses.set(`${heal.uid}:${heal.effectId}`, { uid: heal.uid, id: heal.effectId, expiresAt, atom: heal.atom });
          }
        }
        room.lgSeq = data.lgSeq || 0;
        room.evSeq = data.evSeq || 0;
        room.combatAt = data.combatAt;
        // Restore subzone progression state so dungeon/raid/trial levels continue correctly
        if (data.subzone) {
          const _buildLevelEnemies = (lvl) => {
            return (lvl._hostiles || []).map((h, i) => {
              const hid = (h.id || '').trim();
              const def = ENEMY_DB[hid];
              if (!def) return null;
              return { uid: `${hid}_${i}_${Date.now()}`, type: hid };
            }).filter(Boolean);
          };
          room._subzone = { ...data.subzone, buildLevelEnemies: _buildLevelEnemies };
        }
        // Restore cleared enemies so their loot is included in final payout
        room._clearedEnemies = (data.clearedEnemies || []);
        // Adjust combatAt so berserk fires at the correct wall-clock time
        // (combatAt is used to check if 30 min has elapsed — keep it original)
        room.ended = false;
        room.ticker = null;
        // Put room in waiting state — ticks paused until all members reconnect
        room._waitingForMembers = true;
        room._reconnectedUids = new Set();
        room._queuedActions = [];
        // Set a 30-second timer from now — if not all reconnect, resume anyway
        const memberCount = data.members.length;
        room._waitTimeout = setTimeout(() => {
          if (!room._waitingForMembers) return; // already resolved
          console.log(`[BOOT] Room ${row.party_id} — wait timeout, resuming with ${room._reconnectedUids.size}/${memberCount} members`);
          room._waitingForMembers = false;
          // Check berserk immediately in case it expired during wait
          if (Date.now() - room.combatAt > 1800000) {
            room._tick(); // will trigger berserk
            return;
          }
          room.ticker = setInterval(() => room._tick(), TICK_MS);
        }, 30000);
        rooms.set(data.partyId, room);
        stmt.markCombatRoomRestored.run(now, row.party_id);
        console.log(`[BOOT] Restored room ${row.party_id} members=${memberCount} berserksIn=${Math.floor(berserksIn/1000)}s`);
      } catch(e) {
        console.error(`[BOOT] Failed to restore room ${row.party_id}:`, e.message);
        try { stmt.deleteCombatRoom.run(row.party_id); } catch(e2) {}
      }
    }
  } catch(e) {
    console.error("[BOOT] Combat room restore error:", e.message);
  }
  _bootRestoreComplete = true;
  _processRejoinQueue();
}

function _processRejoinQueue() {
  if (_rejoinQueue.length === 0) return;
  console.log(`[BOOT] Processing ${_rejoinQueue.length} queued rejoin request(s)`);
  for (const { uid, ws: qws, roomId } of _rejoinQueue) {
    try {
      const room = rooms.get(roomId);
      if (room && !room.ended) {
        const member = room.members.find(m => m.uid === uid);
        if (member) {
          if (room._waitingForMembers) {
            room._reconnectedUids.add(uid);
            room.sendFullState(uid);
            const allReconnected = room.members.every(m => room._reconnectedUids.has(m.uid));
            if (allReconnected) {
              room._waitingForMembers = false;
              if (room._waitTimeout) clearTimeout(room._waitTimeout);
              for (const qa of room._queuedActions || []) {
                if (!room.ended) room.handleAction(qa.uid, qa.msg);
              }
              room._queuedActions = [];
              room.ticker = setInterval(() => room._tick(), TICK_MS);
              console.log(`[BOOT] All members reconnected, resumed room=${roomId}`);
            }
          } else {
            room.sendFullState(uid);
          }
          // Notify client of combat rejoin
          try { if (qws.readyState === 1) qws.send(JSON.stringify({ type:"combat_rejoin", roomId })); } catch(e) {}
        }
      } else {
        try { if (qws.readyState === 1) qws.send(JSON.stringify({ type:"no_active_combat" })); } catch(e) {}
      }
    } catch(e) { console.error("[BOOT] rejoin queue error:", e.message); }
  }
  _rejoinQueue.length = 0;
}

// ── Arena queues ─────────────────────────────────────────────────────────────
const arenaQueues = { "1v1": [], "2v2": [], "4v4": [] };
// O(1) membership test — kept in sync with arenaQueues at every push/shift/filter/unshift site.
const _arenaQueueUids = new Set();
// Pending match confirmations — matchId → { mode, p1e?, p2e?, team1?, team2?, allUids, accepted, declined, timer }
const _pendingMatches = new Map();

// ── Push helpers: notify connected clients of data changes ───────────────────
// Server pushes data changes to connected clients via WebSocket

// ── Local delivery helpers (deliver to clients on THIS machine only) ──────────
function _pushInbox(targetUid) {
  const client = clients.get(targetUid);
  if (!client || client.ws.readyState !== 1) return;
  const inbox = dbGetInboxObject(targetUid);
  send(client.ws, { type: "inbox_update", data: inbox });
}

function _pushDms(targetUid) {
  const client = clients.get(targetUid);
  if (!client || client.ws.readyState !== 1) return;
  const rows = stmt.getDms.all(targetUid);
  const dms = {};
  for (const row of rows) {
    dms[row.entry_key] = { f: row.sender, n: row.sender_name, m: row.message, t: row.created_at };
  }
  send(client.ws, { type: "dm_update", data: dms });
}

function _pushPartyToMembers(partyId, excludeUid) {
  const partyData = dbGetParty(partyId);
  if (!partyData || !partyData.members) return;
  // Serialize once and reuse the string for every member — avoids N JSON.stringify calls
  const str = JSON.stringify({ type: "party_update", partyId, data: partyData });
  for (const m of partyData.members) {
    if (!m.uid) continue;
    if (excludeUid && m.uid === excludeUid) continue;
    const client = clients.get(m.uid);
    if (client && client.ws.readyState === 1) client.ws.send(str);
  }
}

function _pushZoneChatToZone(zoneId, chatMsg) {
  const zoneUids = _zoneClients.get(zoneId);
  if (!zoneUids) return;
  for (const uid of zoneUids) {
    const client = clients.get(uid);
    if (client && client.ws.readyState === 1) {
      send(client.ws, { type: "zone_chat_msg", ...chatMsg, zone: zoneId });
    }
  }
}

function _pushGuildChatToMembers(guildId, chatMsg, excludeUid) {
  const members = stmt.getGuildMembers.all(guildId);
  const str = JSON.stringify({ type: "guild_chat_msg", ...chatMsg });
  for (const m of members) {
    if (!m.uid || m.uid === excludeUid) continue;
    const client = clients.get(m.uid);
    if (client && client.ws.readyState === 1) client.ws.send(str);
  }
}

function _pushPartyChatToMembers(partyId, chatMsg, excludeUid) {
  const partyData = dbGetParty(partyId);
  if (!partyData || !partyData.members) return;
  const str = JSON.stringify({ type: "party_chat_msg", ...chatMsg });
  for (const m of partyData.members) {
    if (!m.uid || m.uid === excludeUid) continue;
    const client = clients.get(m.uid);
    if (client && client.ws.readyState === 1) client.ws.send(str);
  }
}

// ── Cross-machine delivery: local + Redis publish ─────────────────────────────
function _broadcastInbox(targetUid) {
  _pushInbox(targetUid);
  _pub({ t: "inbox", uid: targetUid });
}
function _broadcastDms(targetUid) {
  _pushDms(targetUid);
  _pub({ t: "dms", uid: targetUid });
}
function _broadcastParty(partyId, excludeUid) {
  _pushPartyToSubscribers(partyId, excludeUid);
  // _pub already called inside _pushPartyToSubscribers — no double-publish needed
}
// Lightweight HP-only push — sends ~60 bytes instead of the full party doc.
// Use for provision use and equip changes; reserve _broadcastParty for structural changes.
const _lastHpBroadcast = new Map(); // uid -> "hp:maxHp" last sent

// ── Combat-active UID index ───────────────────────────────────────────────────
// Tracks which UIDs are currently in an active combat room. Used by the idle
// interval to check combat status in O(1) instead of O(players × rooms).
// Updated when rooms start (add members) and end (remove members).
const _combatActiveUids = new Set();
function _broadcastPartyHpPatch(partyId, uid, hp, maxHp) {
  // Skip if HP hasn't changed since last broadcast for this uid
  const key = `${hp}:${maxHp}`;
  if (_lastHpBroadcast.get(uid) === key) return;
  _lastHpBroadcast.set(uid, key);
  const partyData = dbGetParty(partyId);
  if (!partyData || !partyData.members) return;
  // Update HP in-place so future full broadcasts have correct HP
  let docDirty = false;
  for (const m of partyData.members) {
    if (m.uid === uid) { m.hp = hp; m.maxHp = maxHp; docDirty = true; break; }
  }
  if (docDirty) dbSetParty(partyId, { ...partyData, ts: Date.now() });
  // Push to all subscribers except the sender (use subscription map, not member list)
  const str = JSON.stringify({ type: "party_hp_patch", partyId, uid, hp, maxHp });
  for (const [cUid, pid] of partySubscriptions.entries()) {
    if (pid !== partyId || cUid === uid) continue;
    const client = clients.get(cUid);
    if (client && client.ws.readyState === 1) client.ws.send(str);
  }
  _pub({ t: "party_hp_patch", partyId, uid, hp, maxHp, excl: uid });
}
function _broadcastZoneChat(zoneId, chatMsg) {
  _pushZoneChatToZone(zoneId, chatMsg);
  _pub({ t: "zone_chat", zone: zoneId, msg: chatMsg });
}
function _broadcastGuildChat(guildId, chatMsg, excludeUid) {
  _pushGuildChatToMembers(guildId, chatMsg, excludeUid);
  _pub({ t: "guild_chat", guildId, msg: chatMsg, excl: excludeUid || null });
}
function _broadcastPartyChat(partyId, chatMsg, excludeUid) {
  _pushPartyChatToMembers(partyId, chatMsg, excludeUid);
  _pub({ t: "party_chat", partyId, msg: chatMsg, excl: excludeUid || null });
}
function _broadcastPresence(uid, username, online, zone) {
  _notifyFriendsOfStatus(uid, username, online);
  _pub({ t: "presence", uid, username, online, zone: zone || null });
}
function _broadcastAll(obj) {
  _broadcastToAll(obj);
  _pub({ t: "broadcast", obj });
}

// ── Ownership-aware item delta broadcast ─────────────────────────────────────
// Uses _itemOwners reverse index for O(1) per-item lookup instead of scanning
// all connected players' saves. Complementary to _broadcastItemDelta:
//   _broadcastItemDelta  → zone-relevant players (market browsing, zone catalog)
//   _broadcastItemToOwners → inventory/equipped/learned owners anywhere
function _broadcastItemToOwners(slimItems, deletedIds) {
  if (!slimItems.length && !deletedIds.length) return;

  // Pre-compute which item IDs are already covered by zone-scope for each zone,
  // so we don't re-fetch _zoneItemIndex on every (uid, item) pair in the inner loop.
  const globalItemIds = _zoneItemIndex.get('__global__') || new Set();
  // zoneCoveredIds: zoneId -> Set<itemId> that zone-scope already delivers
  // We build this lazily per zone as we encounter owners below.
  const zoneCoveredCache = new Map(); // zoneId -> Set<itemId>
  function _zoneCovered(zone, itemId) {
    if (globalItemIds.has(itemId)) return true;
    if (!zone) return false;
    let covered = zoneCoveredCache.get(zone);
    if (!covered) {
      covered = _zoneItemIndex.get(zone) || new Set();
      zoneCoveredCache.set(zone, covered);
    }
    return covered.has(itemId);
  }

  // Collect per-uid what to send, using the O(1) reverse index
  const perUid = new Map(); // uid -> { toSend: [], toDelete: [] }

  for (const slim of slimItems) {
    const owners = _itemOwners.get(slim.id);
    if (!owners) continue;
    for (const uid of owners) {
      const client = clients.get(uid);
      if (!client?.ws || client.ws.readyState !== 1) continue;
      // Skip if _broadcastItemDelta already covered this player for this item
      if (_zoneCovered(client.zone, slim.id)) continue;
      if (!perUid.has(uid)) perUid.set(uid, { toSend: [], toDelete: [] });
      perUid.get(uid).toSend.push(slim);
    }
  }

  for (const deletedId of deletedIds) {
    const owners = _itemOwners.get(deletedId);
    if (!owners) continue;
    for (const uid of owners) {
      const client = clients.get(uid);
      if (!client?.ws || client.ws.readyState !== 1) continue;
      if (!perUid.has(uid)) perUid.set(uid, { toSend: [], toDelete: [] });
      perUid.get(uid).toDelete.push(deletedId);
    }
  }

  for (const [uid, { toSend, toDelete }] of perUid) {
    if (!toSend.length && !toDelete.length) continue;
    const client = clients.get(uid);
    if (!client?.ws || client.ws.readyState !== 1) continue;
    client.ws.send(JSON.stringify({
      type: "catalog_delta",
      itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
      upsertedItems: toSend, deletedItems: toDelete,
      upsertedZones: [], deletedZones: [], upsertedHostiles: [], deletedHostiles: [],
      _slim: true,
    }));
  }

  // Cross-machine: remote machines run the same ownership filter for their own clients
  _pub({ t: "item_delta_owned", slimItems, deletedIds,
    itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION });
}

// ── Zone-scoped hostile delta broadcast ──────────────────────────────────────
// Upserts only reach players in zones containing the hostile (O(1) reverse index).
// Deletions go to all clients so they can evict the hostile from their cache.
function _broadcastHostileDelta(upsertedHostiles, deletedHostiles) {
  if (!upsertedHostiles.length && !deletedHostiles.length) return;

  // Build zoneId -> [hostiles] using the reverse index — O(hostiles), not O(hostiles×zones)
  const byZone = new Map();
  for (const hostile of upsertedHostiles) {
    const zones = _hostileToZones.get(hostile.id);
    if (!zones) continue;
    for (const zid of zones) {
      if (!byZone.has(zid)) byZone.set(zid, []);
      byZone.get(zid).push(hostile);
    }
  }

  const basePayload = {
    type: "catalog_delta",
    itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
    deletedHostiles, upsertedZones: [], deletedZones: [], upsertedItems: [], deletedItems: [],
  };

  // Pre-serialize one JSON string per distinct zone payload and one for delete-only.
  // Players sharing the same zone reuse the same string — no redundant JSON.stringify.
  const serializedByZone = new Map();
  for (const [zid, hostiles] of byZone) {
    serializedByZone.set(zid, JSON.stringify({ ...basePayload, upsertedHostiles: hostiles }));
  }
  const deleteOnlyStr = deletedHostiles.length
    ? JSON.stringify({ ...basePayload, upsertedHostiles: [] })
    : null;

  // Iterate only zones that have relevant clients, using the _zoneClients reverse index
  const visitedUids = new Set();
  for (const [zid, zoneStr] of serializedByZone) {
    const uids = _zoneClients.get(zid);
    if (!uids) continue;
    for (const uid of uids) {
      const client = clients.get(uid);
      if (!client?.ws || client.ws.readyState !== 1) continue;
      client.ws.send(zoneStr);
      visitedUids.add(uid);
    }
  }
  // For deletions: any client not already sent a zone-specific payload also needs the eviction
  if (deleteOnlyStr) {
    for (const client of clients.values()) {
      if (!client.ws || client.ws.readyState !== 1) continue;
      if (!visitedUids.has(client.uid)) client.ws.send(deleteOnlyStr);
    }
  }

  // Cross-machine Redis delivery
  if (upsertedHostiles.length) {
    _pub({ t: "hostile_delta_zoned", upsertedHostiles, deletedHostiles: [],
      itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION });
  }
  if (deletedHostiles.length) {
    _pub({ t: "broadcast", obj: { ...basePayload, upsertedHostiles: [] } });
  }
}

// ── Viewer-aware hostile delta broadcast ─────────────────────────────────────
// Sends hostile updates to players who have opened that hostile's detail popup
// this session, regardless of their current zone. Deduplicates against
// _broadcastHostileDelta's zone delivery to avoid double-sends.
function _broadcastHostileToViewers(upsertedHostiles, deletedHostiles) {
  if (!upsertedHostiles.length && !deletedHostiles.length) return;

  const perUid = new Map(); // uid -> { toSend: [], toDelete: [] }

  for (const hostile of upsertedHostiles) {
    const viewers = _hostileViewers.get(hostile.id);
    if (!viewers) continue;
    for (const uid of viewers) {
      const client = clients.get(uid);
      if (!client?.ws || client.ws.readyState !== 1) continue;
      // Skip if _broadcastHostileDelta already covered this player (they're in the zone)
      const zoneHostileIds = client.zone ? (_zoneHostileIndex.get(client.zone) || new Set()) : new Set();
      if (zoneHostileIds.has(hostile.id)) continue;
      if (!perUid.has(uid)) perUid.set(uid, { toSend: [], toDelete: [] });
      perUid.get(uid).toSend.push(hostile);
    }
  }

  for (const hostileId of deletedHostiles) {
    const viewers = _hostileViewers.get(hostileId);
    if (!viewers) continue;
    for (const uid of viewers) {
      const client = clients.get(uid);
      if (!client?.ws || client.ws.readyState !== 1) continue;
      if (!perUid.has(uid)) perUid.set(uid, { toSend: [], toDelete: [] });
      perUid.get(uid).toDelete.push(hostileId);
    }
  }

  for (const [uid, { toSend, toDelete }] of perUid) {
    if (!toSend.length && !toDelete.length) continue;
    const client = clients.get(uid);
    if (!client?.ws || client.ws.readyState !== 1) continue;
    client.ws.send(JSON.stringify({
      type: "catalog_delta",
      itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
      upsertedHostiles: toSend, deletedHostiles: toDelete,
      upsertedZones: [], deletedZones: [], upsertedItems: [], deletedItems: [],
    }));
  }
}

// ── Zone-scoped item delta broadcast ─────────────────────────────────────────
// For item upserts: only send to players currently in a zone where the item
// exists, or to everyone if it's a global item (zones:[]) or a deletion.
// Actions are global — no zone filter applied.
// Deletions always broadcast to everyone so clients can evict from LIVE_ITEMS.
// This mirrors the zone-scoped login path for online players.
function _broadcastItemDelta(upsertedItems, deletedItems) {
  if (!upsertedItems.length && !deletedItems.length) return;

  // Separate global items (no zone restriction) from zone-specific ones
  const globalItems  = upsertedItems.filter(i => !i.zones || !i.zones.length || i.category === 'action');
  const zonedItems   = upsertedItems.filter(i => i.zones && i.zones.length && i.category !== 'action');

  // Build a map of zoneId -> [items] for quick per-player lookup
  const byZone = new Map();
  for (const item of zonedItems) {
    for (const zid of item.zones) {
      if (!byZone.has(zid)) byZone.set(zid, []);
      byZone.get(zid).push(item);
    }
  }

  const basePayload = {
    type: "catalog_delta",
    itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
    deletedItems, upsertedZones: [], deletedZones: [], upsertedHostiles: [], deletedHostiles: [],
  };

  // Pre-serialize one string per distinct zone payload (global+zone items combined)
  // and one for global-only (clients in zones with no zoned updates, or zone unknown).
  const serializedByZone = new Map();
  for (const [zid, zItems] of byZone) {
    serializedByZone.set(zid, JSON.stringify({ ...basePayload, upsertedItems: [...globalItems, ...zItems] }));
  }
  const globalOnlyStr = (globalItems.length || deletedItems.length)
    ? JSON.stringify({ ...basePayload, upsertedItems: globalItems })
    : null;

  // Use _zoneClients to visit only occupied zones, then fall back to globalOnlyStr
  // for clients whose zone has no specific updates.
  const sentUids = new Set();
  for (const [zid, zStr] of serializedByZone) {
    const uids = _zoneClients.get(zid);
    if (!uids) continue;
    for (const uid of uids) {
      const client = clients.get(uid);
      if (!client?.ws || client.ws.readyState !== 1) continue;
      client.ws.send(zStr);
      sentUids.add(uid);
    }
  }
  // Clients not in a zone with zoned items still need global items + deletions
  if (globalOnlyStr) {
    for (const [uid, client] of clients.entries()) {
      if (!client.ws || client.ws.readyState !== 1) continue;
      if (sentUids.has(uid)) continue;
      client.ws.send(globalOnlyStr);
    }
  }

  // Also publish via Redis for cross-machine delivery.
  // Global items and deletions go to all machines as a full broadcast.
  // Zone-specific items per-machine are handled by each machine's own client loop above.
  if (globalItems.length || deletedItems.length) {
    _pub({ t: "broadcast", obj: { ...basePayload, upsertedItems: globalItems } });
  }
  // Zone-specific items: publish the full set — remote machines filter for their own players
  if (zonedItems.length) {
    _pub({ t: "item_delta_zoned", upsertedItems: zonedItems, deletedItems: [],
      itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION });
  }
}

// ── HTTP server ──────────────────────────────────────────────────────────────
const MIME_TYPES = {
  ".html": "text/html", ".css": "text/css", ".js": "application/javascript",
  ".json": "application/json", ".png": "image/png", ".jpg": "image/jpeg",
  ".svg": "image/svg+xml", ".ico": "image/x-icon",
};
// Unified cache: stores { raw, gzip, etag } per resolved path
const _fileCache = {};

function _loadFileCache(resolved) {
  let mtimeMs;
  try { mtimeMs = fs.statSync(resolved).mtimeMs; } catch { return null; }
  const cached = _fileCache[resolved];
  if (cached && cached.mtimeMs === mtimeMs) return cached; // unchanged on disk, safe to reuse
  try {
    const raw = fs.readFileSync(resolved);
    const hash = crypto.createHash("md5").update(raw).digest("hex").slice(0, 12);
    const etag = `"${hash}"`;
    const ext = path.extname(resolved).toLowerCase();
    let gzipped = null;
    let brotli = null;
    if ([".html",".js",".css",".json",".svg"].includes(ext)) {
      gzipped = zlib.gzipSync(raw, { level: 9 });
      try { brotli = zlib.brotliCompressSync(raw, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11 } }); } catch(e) {}
    }
    _fileCache[resolved] = { raw, gzip: gzipped, br: brotli, etag, mtimeMs };
    console.log(`[HTTP] cached ${path.basename(resolved)}: ${raw.length}B raw, ${gzipped ? gzipped.length+"B gz" : "no gz"}, ${brotli ? brotli.length+"B br" : "no br"}, etag=${etag}`);
    return _fileCache[resolved];
  } catch { return null; }
}
// Pre-cache client.html and pv-devguide.html on startup
const _clientPath   = path.resolve(path.join(__dirname, "client.html"));
const _guidePath    = path.resolve(path.join(__dirname, "pv-devguide.html"));
_loadFileCache(_clientPath);
try { _loadFileCache(_guidePath); } catch(e) { console.warn("[HTTP] pv-devguide.html not found — /devguide will return 404"); }

// ── Delta-patch delivery for client.html ──────────────────────────────────────
// Lets returning players fetch only the diff between the client.html version
// their browser already has cached (in localStorage, via the bootloader) and
// whatever is currently on disk, instead of re-downloading the whole file.
// Falls back to a full transfer whenever anything is inconsistent — a bug here
// should degrade to "downloads the whole file" (today's behavior), never to a
// broken/corrupted client.

// cyrb53: fast, well-distributed, pure-JS string hash. Used (not for security,
// just integrity-checking our own diff/patch math) because it's trivial to
// keep byte-identical between this Node server and the browser bootloader —
// unlike crypto.createHash, which the browser can't run synchronously.
function _cyrb53(str, seed) {
  seed = seed || 0;
  let h1 = 0xdeadbeef ^ seed, h2 = 0x41c6ce57 ^ seed;
  for (let i = 0, ch; i < str.length; i++) {
    ch = str.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return 4294967296 * (2097151 & h2) + (h1 >>> 0);
}
function _hashStr(s) { return _cyrb53(s).toString(36); }

// Myers line-diff. maxD caps the work done (diff is O(N*D)) — if the two
// versions differ too much to make a small patch worthwhile, bail out (null)
// so the caller falls back to a full transfer instead of burning CPU/memory.
function _diffLines(oldLines, newLines, maxD) {
  const N = oldLines.length, M = newLines.length;
  const max = maxD != null ? Math.min(maxD, N + M) : (N + M);
  const offset = max;
  const v = new Array(2 * max + 2).fill(0);
  const trace = [];
  for (let d = 0; d <= max; d++) {
    trace.push(v.slice());
    for (let k = -d; k <= d; k += 2) {
      let x;
      if (k === -d || (k !== d && v[offset + k - 1] < v[offset + k + 1])) x = v[offset + k + 1];
      else x = v[offset + k - 1] + 1;
      let y = x - k;
      while (x < N && y < M && oldLines[x] === newLines[y]) { x++; y++; }
      v[offset + k] = x;
      if (x >= N && y >= M) return _diffBacktrack(trace, oldLines, newLines, d, offset);
    }
  }
  return null;
}
function _diffBacktrack(trace, oldLines, newLines, D, offset) {
  let x = oldLines.length, y = newLines.length;
  const rawOps = [];
  for (let d = D; d > 0; d--) {
    const v = trace[d];
    const k = x - y;
    let prevK = (k === -d || (k !== d && v[offset + k - 1] < v[offset + k + 1])) ? k + 1 : k - 1;
    const prevX = v[offset + prevK], prevY = prevX - prevK;
    while (x > prevX && y > prevY) { rawOps.push(["=", oldLines[x - 1]]); x--; y--; }
    if (x === prevX) { rawOps.push(["+", newLines[y - 1]]); y--; }
    else { rawOps.push(["-", oldLines[x - 1]]); x--; }
  }
  while (x > 0 && y > 0) { rawOps.push(["=", oldLines[x - 1]]); x--; y--; }
  rawOps.reverse();
  const ops = [];
  let i = 0;
  while (i < rawOps.length) {
    const type = rawOps[i][0];
    if (type === "=" || type === "-") {
      let n = 0;
      while (i < rawOps.length && rawOps[i][0] === type) { n++; i++; }
      ops.push([type, n]);
    } else {
      const lines = [];
      while (i < rawOps.length && rawOps[i][0] === "+") { lines.push(rawOps[i][1]); i++; }
      ops.push(["+", lines]);
    }
  }
  return ops;
}

const _clientHistory = []; // [{hash, lines}] — old versions kept around so late-arriving clients can still diff
const CLIENT_HISTORY_MAX = 8;
let _lastClientHash = null;
let _lastClientLines = null;

// Returns { hash, raw, lines } for the current client.html, re-reading from
// disk (via _loadFileCache's mtime check) and rotating version history
// whenever the file has actually changed.
function _getClientPayload() {
  const cached = _loadFileCache(_clientPath);
  if (!cached) return null;
  const text = cached.raw.toString("utf8");
  const hash = _hashStr(text);
  if (hash !== _lastClientHash) {
    if (_lastClientHash && _lastClientLines) {
      _clientHistory.push({ hash: _lastClientHash, lines: _lastClientLines });
      if (_clientHistory.length > CLIENT_HISTORY_MAX) _clientHistory.shift();
    }
    _lastClientHash = hash;
    _lastClientLines = text.split("\n");
  }
  return { hash, raw: cached.raw, lines: _lastClientLines };
}
_getClientPayload(); // prime on startup

// Minimal bootloader served at "/" — fetches/reconstructs the real game
// payload (old client.html content, now served from /payload) and hands off
// via document.write. Kept intentionally tiny and dependency-free.
const BOOTLOADER_HTML = `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Project Void</title>
<style>html,body{margin:0;padding:0;background:#000;height:100%;}
#pv-boot{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;color:#666;font-family:'Courier New',monospace;font-size:13px;letter-spacing:2px;text-align:center;padding:20px;box-sizing:border-box;}
</style></head><body>
<div id="pv-boot">LOADING...</div>
<script>
(function(){
function cyrb53(str,seed){seed=seed||0;var h1=0xdeadbeef^seed,h2=0x41c6ce57^seed;for(var i=0,ch;i<str.length;i++){ch=str.charCodeAt(i);h1=Math.imul(h1^ch,2654435761);h2=Math.imul(h2^ch,1597334677);}h1=Math.imul(h1^(h1>>>16),2246822507)^Math.imul(h2^(h2>>>13),3266489909);h2=Math.imul(h2^(h2>>>16),2246822507)^Math.imul(h1^(h1>>>13),3266489909);return 4294967296*(2097151&h2)+(h1>>>0);}
function hashStr(s){return cyrb53(s).toString(36);}
function applyPatch(oldLines,ops){
  var out=[],oi=0;
  for(var idx=0;idx<ops.length;idx++){
    var op=ops[idx];
    if(op[0]==="="){for(var i=0;i<op[1];i++)out.push(oldLines[oi++]);}
    else if(op[0]==="-"){oi+=op[1];}
    else if(op[0]==="+"){var l=op[1];for(var j=0;j<l.length;j++)out.push(l[j]);}
    else{throw new Error("bad op");}
  }
  if(oi!==oldLines.length)throw new Error("patch mismatch");
  return out;
}
var LS_PAYLOAD="pv_boot_payload",LS_HASH="pv_boot_hash";
function boot(text){
  try{document.open();document.write(text);document.close();}
  catch(e){fail("LOAD ERROR — PLEASE REFRESH");}
}
function fail(msg){var el=document.getElementById("pv-boot");if(el)el.textContent=msg;}
function fetchFull(){
  fetch("/payload",{cache:"no-store"}).then(function(r){
    if(!r.ok)throw new Error("payload fetch failed");
    return r.text();
  }).then(function(text){
    try{localStorage.setItem(LS_PAYLOAD,text);localStorage.setItem(LS_HASH,hashStr(text));}catch(e){}
    boot(text);
  }).catch(function(){fail("CONNECTION ERROR — PLEASE REFRESH");});
}
function main(){
  var cachedPayload=null,cachedHash=null;
  try{cachedPayload=localStorage.getItem(LS_PAYLOAD);cachedHash=localStorage.getItem(LS_HASH);}catch(e){}
  fetch("/version",{cache:"no-store"}).then(function(r){return r.json();}).then(function(v){
    if(!v||!v.hash)throw new Error("bad version response");
    if(cachedPayload&&cachedHash===v.hash){boot(cachedPayload);return;}
    if(cachedPayload&&cachedHash){
      fetch("/payload-diff?from="+encodeURIComponent(cachedHash),{cache:"no-store"})
        .then(function(r){return r.json();})
        .then(function(d){
          if(!d||!d.ok){fetchFull();return;}
          var newText;
          try{
            var newLines=applyPatch(cachedPayload.split("\\n"),d.patch);
            newText=newLines.join("\\n");
            if(hashStr(newText)!==d.newHash)throw new Error("hash mismatch");
          }catch(e){fetchFull();return;}
          try{localStorage.setItem(LS_PAYLOAD,newText);localStorage.setItem(LS_HASH,d.newHash);}catch(e){}
          boot(newText);
        })
        .catch(function(){fetchFull();});
    }else{fetchFull();}
  }).catch(function(){fetchFull();});
}
main();
})();
</script>
</body></html>`;

// Allowed static files — only these can be served over HTTP
const ALLOWED_FILES = new Set(["client.html", "pv-devguide.html"]);

// Helper: serve a cached file with compression negotiation
function _serveFile(req, res, resolved) {
  const cached = _loadFileCache(resolved);
  if (!cached) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not Found");
    return;
  }
  const ext = path.extname(resolved).toLowerCase();
  const contentType = MIME_TYPES[ext] || "text/html";
  const ifNoneMatch = req.headers["if-none-match"];
  if (ifNoneMatch && ifNoneMatch === cached.etag) {
    res.writeHead(304, { "ETag": cached.etag, "Clear-Site-Data": '"cache"' });
    res.end();
    return;
  }
  const acceptEncoding = req.headers["accept-encoding"] || "";
  const headers = {
    "Content-Type": contentType,
    "Cache-Control": "no-store",
    // Tell any browser that still contacts us to purge its HTTP cache for this origin.
    // Scoped to "cache" only — NOT "storage" or "cookies" — since client.html keeps a
    // persistent device token + reload-session data in localStorage that must survive this.
    "Clear-Site-Data": '"cache"',
    "ETag": cached.etag,
  };
  if (cached.br && acceptEncoding.includes("br")) {
    headers["Content-Encoding"] = "br"; res.writeHead(200, headers); res.end(cached.br);
  } else if (cached.gzip && acceptEncoding.includes("gzip")) {
    headers["Content-Encoding"] = "gzip"; res.writeHead(200, headers); res.end(cached.gzip);
  } else {
    res.writeHead(200, headers); res.end(cached.raw);
  }
}

const server = http.createServer((req, res) => {
  const rawUrl = (req.url || "/").split("?")[0];

  // ── /devguide — serve the developer guidebook (no auth required) ────────────
  if (rawUrl === "/devguide" || rawUrl === "/devguide/") {
    _serveFile(req, res, _guidePath);
    return;
  }

  // ── / — serve the tiny bootloader; it fetches/reconstructs the real game ───
  if (rawUrl === "/") {
    res.writeHead(200, { "Content-Type": "text/html", "Cache-Control": "no-store" });
    res.end(BOOTLOADER_HTML);
    return;
  }

  // ── /version — tiny hash the bootloader polls to decide if it needs anything
  if (rawUrl === "/version") {
    const payload = _getClientPayload();
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    res.end(JSON.stringify({ hash: payload ? payload.hash : null }));
    return;
  }

  // ── /payload — full client.html content (first visit / fallback path) ──────
  if (rawUrl === "/payload") {
    if (!_getClientPayload()) { res.writeHead(404); res.end("Not Found"); return; }
    _serveFile(req, res, _clientPath);
    return;
  }

  // ── /payload-diff — delta patch from an old cached version to current ──────
  if (rawUrl === "/payload-diff") {
    const from = new URL(req.url, "http://x").searchParams.get("from");
    const payload = _getClientPayload();
    if (!payload) { res.writeHead(404); res.end("Not Found"); return; }
    let body;
    if (from === payload.hash) {
      // A no-op patch must still be a *valid* patch against a non-empty file — an
      // empty ops array only reconstructs correctly when the file has zero lines.
      // Not reachable through the bootloader's normal flow today (it already
      // short-circuits locally when hashes match before ever calling this route),
      // but kept correct here as a safety net against future callers/races.
      body = JSON.stringify({ ok: true, patch: [["=", payload.lines.length]], newHash: payload.hash });
    } else {
      const hist = _clientHistory.find(h => h.hash === from);
      if (!hist) {
        body = JSON.stringify({ ok: false });
      } else {
        const ops = _diffLines(hist.lines, payload.lines, 5000);
        body = ops ? JSON.stringify({ ok: true, patch: ops, newHash: payload.hash })
                    : JSON.stringify({ ok: false });
      }
    }
    const acceptEncoding = req.headers["accept-encoding"] || "";
    if (acceptEncoding.includes("gzip")) {
      res.writeHead(200, { "Content-Type": "application/json", "Content-Encoding": "gzip", "Cache-Control": "no-store" });
      res.end(zlib.gzipSync(Buffer.from(body)));
    } else {
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(body);
    }
    return;
  }

  // ── everything else (e.g. /client.html direct access, for debugging) ───────
  let filePath = rawUrl;
  const resolved = path.resolve(path.join(__dirname, filePath));

  // Directory traversal guard
  if (!resolved.startsWith(path.resolve(__dirname))) {
    res.writeHead(403); res.end("Forbidden"); return;
  }

  const basename = path.basename(resolved);
  if (!ALLOWED_FILES.has(basename)) {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Project Void Server OK");
    return;
  }

  _serveFile(req, res, resolved);
});

const wss = new WebSocketServer({ 
  server,
  maxPayload: 16 * 1024, // 16KB - largest legitimate message is well under 1KB
  perMessageDeflate: {
    zlibDeflateOptions: { level: 6 },
    threshold: 64,
    concurrencyLimit: 10,
    // Keep compression context alive across messages on the same connection.
    // The compressor learns repeated field names (t, k, uid, h, en, al) and
    // compresses subsequent combat ticks significantly better. ~32KB RAM per
    // connection - negligible cost at any stage of the scaling plan.
    serverNoContextTakeover: false,
    clientNoContextTakeover: false,
  }
});

// ══════════════════════════════════════════════════════════════════════
//  GUILD HELPERS
// ══════════════════════════════════════════════════════════════════════

const GUILD_MAX_MEMBERS = 24;
const GUILD_MAX_OFFICERS = 3;
const GUILD_MAX_NAME_LEN = 32;
const GUILD_INVITE_TTL_MS = 24 * 3600 * 1000; // guild invites expire after 24 hours
const GUILD_MIN_NAME_LEN = 2;

// Blocked guild name fragments (case-insensitive)
const GUILD_BANNED_WORDS = [
  "admin","admins","administrator","mod","moderator","staff","gm","gamemaster",
  "game master","system","official","support","developer","dev team",
  "project void","void team","void staff","void admin","pvoid"
];

function _guildNameForbidden(name) {
  const lower = name.toLowerCase().trim();
  for (const w of GUILD_BANNED_WORDS) {
    if (lower.includes(w)) return true;
  }
  return false;
}

function _toTitleCase(str) {
  return str.toLowerCase().replace(/(?:^|\s)\S/g, c => c.toUpperCase());
}

function _guildNameValid(name) {
  const trimmed = name.trim();
  if (trimmed.length < GUILD_MIN_NAME_LEN || trimmed.length > GUILD_MAX_NAME_LEN) return false;
  if (_guildNameForbidden(trimmed)) return false;
  // Allow letters and spaces only — no numbers or symbols
  if (!/^[a-zA-Z ]+$/.test(trimmed)) return false;
  // Every space must have at least one letter on both sides
  if (/ /.test(trimmed) && !/^[a-zA-Z]+ ([a-zA-Z]+ )*[a-zA-Z]+$/.test(trimmed)) return false;
  return true;
}

function _genGuildId() {
  return "g_" + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}

// Push guild_update to all online members of a guild
function _broadcastGuildUpdate(guildId) {
  const gRow = stmt.getGuild.get(guildId);
  const members = stmt.getGuildMembers.all(guildId);
  if (!gRow) return;
  const payload = {
    type: "guild_update",
    guild: {
      id: gRow.id,
      name: gRow.name,
      leaderUid: gRow.leader_uid,
      members: members.map(m => ({
        uid: m.uid,
        username: m.username,
        charName: m.char_name,
        role: m.role,
        joinedAt: m.joined_at
      }))
    }
  };
  members.forEach(m => {
    const mc = clients.get(m.uid);
    if (mc && mc.ws && mc.ws.readyState === 1) {
      send(mc.ws, payload);
    }
  });
}

// Push guild_left to a specific uid (they were kicked or guild dissolved)
function _sendGuildLeft(uid, reason) {
  const mc = clients.get(uid);
  if (mc && mc.ws && mc.ws.readyState === 1) {
    send(mc.ws, { type: "guild_left", reason: reason || "removed" });
  }
}


// Get guild info for a uid (null if not in any guild)
function _getUidGuild(uid) {
  return stmt.getMemberGuild.get(uid) || null;
}

// ── WebSocket connection handler ─────────────────────────────────────────────
wss.on("connection", (ws, req) => {
  let clientUid = null;
  const clientIp = (req.headers["x-forwarded-for"] || "").split(",")[0].trim() || req.socket.remoteAddress || "unknown";
  ws.isAlive = true;
  ws.lastActivity = Date.now();
  ws._ip = clientIp;
  ws.on("pong", () => { ws.isAlive = true; ws.lastActivity = Date.now(); });

  ws.on("message", async (raw) => {
    ws.isAlive = true; // Any message counts as proof of life
    ws.lastActivity = Date.now(); // Track for idle disconnect
    // Minimal ping — single byte "p" string, reply with "q" to save bandwidth
    if (raw === "p" || raw.toString() === "p") { ws.send("q"); return; }
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    try {
    if (msg.type && msg.type.startsWith("admin_")) {
      console.log(`[WS] admin msg type="${msg.type}" clientUid=${clientUid} isAdmin=${clientUid ? ADMIN_UIDS.has(clientUid) : false}`);
    }
    switch (msg.type) {

      // ══════════════════════════════════════════════════════════════════════
      //  AUTH - native JWT verification
      // ══════════════════════════════════════════════════════════════════════

      // ── Register: create new account ───────────────────────────────────
      case "register": {
        const { username: regUser, password: regPw } = msg;
        if (!regUser || !regPw) { send(ws, { type:"register_fail", reason:"missing_fields" }); break; }
        const uLower = regUser.toLowerCase().trim();
        if (uLower.length < 3 || !/^[a-z0-9_]+$/.test(uLower)) { send(ws, { type:"register_fail", reason:"invalid_username" }); break; }
        if (RESERVED_NAMES.has(uLower)) { send(ws, { type:"register_fail", reason:"username_taken" }); break; }
        if (regPw.length < 6) { send(ws, { type:"register_fail", reason:"weak_password" }); break; }
        // Check if username exists
        const existing = stmt.getAuthByUsername.get(uLower);
        if (existing) { send(ws, { type:"register_fail", reason:"username_taken" }); break; }
        // Generate UID and create account
        const newUid = crypto.randomUUID();
        const hashedPw = _hashPassword(regPw);
        try {
          stmt.insertAuth.run(newUid, uLower, hashedPw, Date.now());
          // Also create the accounts table entry
          stmt.upsertAccount.run({ username: uLower, uid: newUid, zone: null, charName: null, hash: null, created: Date.now() });
          const token = _jwtSign({ uid: newUid, username: uLower });
          send(ws, { type:"register_ok", uid: newUid, token, username: uLower });
          console.log(`[REGISTER] username=${uLower} uid=${newUid} ip=${clientIp}`);
          try { stmt.insertTxLog.run(newUid, "register", JSON.stringify({ ip: clientIp, deviceId: msg.deviceId || "", fingerprint: msg.fingerprint || "" }), Date.now()); } catch(e){}
        } catch (e) {
          console.error("[REGISTER] error:", e.message);
          if (e.message.includes("UNIQUE")) send(ws, { type:"register_fail", reason:"username_taken" });
          else send(ws, { type:"register_fail", reason:"server_error" });
        }
        break;
      }

      // ── Login: authenticate existing account ───────────────────────────
      case "login": {
        const { username: loginUser, password: loginPw } = msg;
        const loginDeviceId = msg.deviceId || "";
        const loginFingerprint = msg.fingerprint || "";
        if (!loginUser || !loginPw) { send(ws, { type:"login_fail", reason:"missing_fields" }); break; }
        const loginLower = loginUser.toLowerCase().trim();
        const cred = stmt.getAuthByUsername.get(loginLower);
        if (!cred) { send(ws, { type:"login_fail", reason:"invalid_credentials" }); break; }
        if (!_verifyPassword(loginPw, cred.password)) { send(ws, { type:"login_fail", reason:"invalid_credentials" }); break; }
        // Check ban before issuing token
        const loginSave = _getCachedSave(cred.uid);
        if (loginSave && loginSave.banned === true) { send(ws, { type:"login_fail", reason:"banned" }); break; }
        const token = _jwtSign({ uid: cred.uid, username: loginLower });
        send(ws, { type:"login_ok", uid: cred.uid, token, username: loginLower, mustReset: cred.must_reset ? true : undefined });
        // Store device info on WS for auth handler to pick up
        ws._loginDeviceId = loginDeviceId;
        ws._loginFingerprint = loginFingerprint;
        // Log login with IP + device info
        try { stmt.insertTxLog.run(cred.uid, "login", JSON.stringify({ ip: clientIp, deviceId: loginDeviceId, fingerprint: loginFingerprint }), Date.now()); } catch(e){}
        console.log(`[LOGIN] username=${loginLower} uid=${cred.uid} ip=${clientIp}`);
        break;
      }

      // ── Auth: set new password (after admin restore invalidated the old one) ──
      case "set_new_password": {
        if (!clientUid) { send(ws, { type:"set_pw_fail", reason:"not_authed" }); break; }
        const { newPassword } = msg;
        if (!newPassword || typeof newPassword !== "string" || newPassword.length < 6) {
          send(ws, { type:"set_pw_fail", reason:"Password must be at least 6 characters." }); break;
        }
        try {
          const hashedNewPw = _hashPassword(newPassword);
          stmt.updateAuthPassword.run(hashedNewPw, clientUid);
          _logTx(clientUid, "password_reset", { method: "in_game" });
          send(ws, { type:"set_pw_ok" });
          console.log(`[AUTH] Password reset by uid=${clientUid}`);
        } catch (e) {
          console.error("[AUTH] set_new_password error:", e.message);
          send(ws, { type:"set_pw_fail", reason:"Server error." });
        }
        break;
      }

      // ── Auth: verify token and establish session ───────────────────────
      case "auth": {
        try {
          let authUid = null;
          const token = msg.idToken || msg.token;
          
          // Verify native JWT
          const jwtPayload = _jwtVerify(token);
          if (jwtPayload && jwtPayload.uid) {
            authUid = jwtPayload.uid;
          }
          
          if (!authUid) {
            send(ws, { type:"auth_fail", reason: "invalid_token" });
            break;
          }
          
          clientUid = authUid;
          // Use username from JWT (trusted), NOT from client message (untrusted)
          const authUsername = (jwtPayload.username || "").toLowerCase();
          // Single-session enforcement
          const existingClient = clients.get(clientUid);
          if (existingClient && existingClient.ws !== ws && existingClient.ws.readyState === 1) {
            send(existingClient.ws, { type:"kicked", reason:"logged_in_elsewhere" });
            existingClient.ws.close();
            console.log(`[AUTH] kicked previous session for uid=${clientUid}`);
          }
          clients.set(clientUid, { ws, uid: clientUid, username: authUsername, zone: null, ip: clientIp, deviceId: msg.deviceId || ws._loginDeviceId || "", fingerprint: msg.fingerprint || ws._loginFingerprint || "", loginAt: Date.now() });
          ws._uid = clientUid; // Store for idle disconnect combat check
          
          // Load save into cache and seed ownership index
          const _authSave = _getCachedSave(clientUid);
          _seedItemOwners(clientUid, _authSave);
          // Seed client.zone from persisted save so safe_zone_heal validates correctly
          // even if zone_update hasn't arrived yet this session.
          // Also write it to accounts.zone immediately — this backfills the column for
          // existing accounts and ensures offline friends always see the correct last zone
          // without waiting for zone_update to fire (which only writes on zone change).
          const _authLastZone = (_authSave && (_authSave["p/lz"] || (_authSave.player && _authSave.player.lastZone))) || null;
          if (_authLastZone) {
            _setClientZone(clientUid, _authLastZone);
            try { stmt.updateAccountZone.run(_authLastZone, authUsername); } catch(e) {}
          }
          if (_authSave && _authSave.banned === true) {
            send(ws, { type:"banned" });
            console.log(`[AUTH] BANNED uid=${clientUid}`);
            clients.delete(clientUid);
            clientUid = null;
            ws.close();
            return;
          }
          const isAdmin = ADMIN_USERNAMES.has(authUsername);
          const isLeadAdmin = LEAD_ADMINS.has(authUsername);
          if (isAdmin || isLeadAdmin) ADMIN_UIDS.add(clientUid);
          if (isLeadAdmin) ADMIN_USERNAMES.add(authUsername);
          // Check if client is outdated (missed client_update broadcast)
          const clientVersion = msg.clientVersion || 0;
          const clientNeedsReload = _serverNeedsReload || (clientVersion < CLIENT_VERSION);
          const isDev = DEV_USERNAMES.has(authUsername) || isLeadAdmin;
          send(ws, { type:"auth_ok", uid: clientUid, admin: isAdmin, leadAdmin: isLeadAdmin,
            isDev, needsReload: clientNeedsReload || undefined,
            serverVersion: CLIENT_VERSION, region: SERVER_REGION });
          // ── Catalog sync on login ─────────────────────────────────────────
          // Strategy:
          //   version == 0  → cold client (new device / cleared storage) → send full catalog
          //   version stale → query changelog for exact diff since client's version → send delta
          //   version match → nothing to send
          //
          // This means offline players only download what actually changed since
          // their last session, not the entire catalog, no matter how long they
          // were away or how many monthly updates happened while they were gone.
          const clientItemsV    = parseInt(msg.itemsV)    || 0;
          const clientZonesV    = parseInt(msg.zonesV)    || 0;
          const clientHostilesV = parseInt(msg.hostilesV) || 0;
          const clientActionsV  = parseInt(msg.actionsV)  || 0;
          // Always tell client the current versions so it can cache them
          try { send(ws, { type:"catalog_versions", itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION }); } catch(ecv) {}

          // Helper: query changelog and build a delta for one catalog.
          // Returns null if nothing changed, or { upserted, deleted } arrays.
          function _buildCatalogDiff(catalog, clientV, currentV) {
            if (clientV === currentV) return null; // already up to date
            if (clientV === 0) return 'full';       // cold client — caller sends full
            if (!stmtChangelogSince) return 'full'; // changelog not ready — fallback
            try {
              const rows = stmtChangelogSince.all(catalog, clientV);
              if (!rows.length) return null; // version bumped but no rows (boot bump) — nothing to send
              // Deduplicate: keep only the LATEST row per entity_id
              const latest = new Map();
              for (const r of rows) latest.set(r.entity_id, r);
              const upserted = [], deleted = [];
              for (const r of latest.values()) {
                if (r.action === 'delete') deleted.push(r.entity_id);
                else upserted.push(r.entity_id);
              }
              return { upserted, deleted };
            } catch(e) {
              console.error('[CHANGELOG] diff error:', e.message);
              return 'full';
            }
          }

          // ── Zones ──────────────────────────────────────────────────────────
          try {
            const _zd = _buildCatalogDiff('zones', clientZonesV, ZONE_DB_VERSION);
            if (_zd === 'full') {
              send(ws, { type:"live_zones", zones: _buildZoneCatalog(), _version: ZONE_DB_VERSION });
            } else if (_zd) {
              const upsertedZones = _zd.upserted.map(id => {
                const z = ZONE_DB[id]; if (!z) return null;
                return { id, name:z.name, safe:!!z.safe, hasFishing:!!z.hasFishing,
                  hasDungeon:!!z.hasDungeon, hasRaid:!!z.hasRaid, hasTrial:!!z.hasTrial,
                  subzones:z.subzones||[], market:z.market||null, rarity:z.rarity||'common',
                  description:z.description||'', enemies:z.enemies||[],
                  zoneNumber:z.zoneNumber!=null?Number(z.zoneNumber):null,
                  music:z.music||'',
                  background:z.background||'',
                  exploreMusic:z.exploreMusic||'',
                  arenaMusic1v1:z.arenaMusic1v1||'',
                  arenaMusic2v2:z.arenaMusic2v2||'',
                  arenaMusic4v4:z.arenaMusic4v4||'',
                  arenaBackground:z.arenaBackground||'' };
              }).filter(Boolean);
              if (upsertedZones.length || _zd.deleted.length) {
                send(ws, { type:"catalog_delta", itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
                  upsertedZones, deletedZones: _zd.deleted,
                  upsertedItems:[], deletedItems:[], upsertedHostiles:[], deletedHostiles:[] });
              }
            }
          } catch(ezc) { console.error('[AUTH] zones sync error:', ezc.message); }

          // ── Items + Hostiles (zone-scoped, merged for cold clients) ──────────
          // Cold clients receive one live_catalog message with both items and hostiles
          // for their login zone — halves messages vs sending them separately.
          // Stale clients receive catalog_delta (already a unified format).
          // Global items (zones:[]) are always included. Zone travel handled by zone_update.
          try {
            const _loginZone = _authLastZone || _defaultRespawnZone();
            const _id = _buildCatalogDiff('items',    clientItemsV,    ITEM_DB_VERSION);
            const _hd = _buildCatalogDiff('hostiles', clientHostilesV, ENEMY_DB_VERSION);

            if (_id === 'full' || _hd === 'full') {
              // Cold on at least one catalog — merge both into one live_catalog message
              const coldItems    = _id === 'full'
                ? _buildZoneItems(_loginZone).map(i => _buildSlimItem(i)).filter(Boolean)
                : [];
              const coldHostiles = _hd === 'full'
                ? _buildZoneHostiles(_loginZone)
                : [];
              send(ws, { type:"live_catalog", items: coldItems, hostiles: coldHostiles,
                itemsV: ITEM_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION, _slim: true, _zone: _loginZone });
            }

            // Items delta — for stale items (even if hostiles were cold)
            if (_id && _id !== 'full') {
              const _loginZoneSet = _zoneItemIndex.get(_loginZone) || new Set();
              const _globalSet    = _zoneItemIndex.get('__global__') || new Set();
              const upsertedItems = _id.upserted
                .filter(id => _loginZoneSet.has(id) || _globalSet.has(id))
                .map(id => _buildSlimItem(ITEM_DB[id])).filter(Boolean);
              if (upsertedItems.length || _id.deleted.length) {
                send(ws, { type:"catalog_delta", itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
                  upsertedItems, deletedItems: _id.deleted,
                  upsertedZones:[], deletedZones:[], upsertedHostiles:[], deletedHostiles:[], _slim: true });
              }
            }

            // Hostiles delta — for stale hostiles (even if items were cold)
            if (_hd && _hd !== 'full') {
              const _loginZoneHostileSet = _zoneHostileIndex.get(_loginZone) || new Set();
              const upsertedHostiles = _hd.upserted
                .filter(id => _loginZoneHostileSet.has(id))
                .map(id => { const h = ENEMY_DB[id]; if (!h) return null; const { _live, ...pub } = h; return { id, ...pub }; })
                .filter(Boolean);
              // Only send deletions for hostiles the client could have received
              const deletedHostiles = _hd.deleted.filter(id => _loginZoneHostileSet.has(id));
              if (upsertedHostiles.length || deletedHostiles.length) {
                send(ws, { type:"catalog_delta", itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
                  upsertedHostiles, deletedHostiles,
                  upsertedItems:[], deletedItems:[], upsertedZones:[], deletedZones:[] });
              }
            }
          } catch(ezi) { console.error('[AUTH] catalog sync error:', ezi.message); }
          console.log(`[AUTH] uid=${clientUid} username=${authUsername} clientVersion=${clientVersion} serverVersion=${CLIENT_VERSION}`);

          // WS Presence — load friends from social table (authoritative source)
          const _uname = authUsername;
          if (_uname) {
            _usernameToUid.set(_uname, clientUid);
            try {
              // Social table has current friends list; save table does not
              const _social = dbGetSocial(clientUid);
              const friendsList = (_social && _social.friends) || [];
              const friendsArr = Array.isArray(friendsList) ? friendsList : Object.values(friendsList);
              const friendNames = new Set(friendsArr.map(f => (f.name || "").toLowerCase()).filter(Boolean));
              _setFriends(clientUid, friendNames);
              _sendOnlineFriends(ws, clientUid);
              _broadcastPresence(clientUid, _uname, true, clients.get(clientUid)?.zone || null);
              // Ensure mutual awareness: add this player to each online friend's _uidToFriends
              // so that when this player disconnects, friends get notified even if they haven't
              // sent friends_update yet this session
              for (const friendName of friendNames) {
                const friendUid = _usernameToUid.get(friendName);
                if (!friendUid) continue;
                _addFriend(friendUid, _uname.toLowerCase());
              }
            } catch (e) { console.error("[PRESENCE] friends load error:", e.message); }
          }

          // Arena queue reconnect
          let inArenaQueue = false;
          let inArenaQueueTeam = false;
          for (const [mode, queue] of Object.entries(arenaQueues)) {
            // 1v1: solo entry with uid field
            const soloEntry = queue.find(q => q.uid === clientUid);
            if (soloEntry) { soloEntry.ws = ws; inArenaQueue = mode; inArenaQueueTeam = false; break; }
            // 2v2/4v4: team entry with members array
            const teamEntry = queue.find(q => q.members && q.members.some(m => m.uid === clientUid));
            if (teamEntry) {
              const m = teamEntry.members.find(m => m.uid === clientUid);
              if (m) { m.ws = ws; inArenaQueue = mode; inArenaQueueTeam = true; break; }
            }
          }
          if (inArenaQueue) send(ws, { type:"arena_queue_status", inQueue: true, mode: inArenaQueue, team: inArenaQueueTeam });

          // Combat rejoin check
          for (const [roomId, room] of rooms) {
            if (room.ended) continue;
            const member = room.members.find(m => m.uid === clientUid);
            if (member) {
              console.log(`[REJOIN] uid=${clientUid} rejoining room=${roomId}`);
              room._allOfflineSince = null; // member is back — cancel any orphan grace clock
              send(ws, { type:"combat_rejoin", roomId });
              break;
            }
          }

          // Push pending inbox/DMs on connect
          const inbox = dbGetInboxObject(clientUid);
          if (Object.keys(inbox).length > 0) send(ws, { type: "inbox_update", data: inbox });
          const dmRows = stmt.getDms.all(clientUid);
          if (dmRows.length > 0) {
            const dms = {};
            for (const row of dmRows) dms[row.entry_key] = { f: row.sender, n: row.sender_name, m: row.message, t: row.created_at };
            send(ws, { type: "dm_update", data: dms });
          }
          // Re-push party doc if client is subscribed (reconnect case).
          // Dissolve ghost solo parties so the player doesn't log in stuck in a party-of-one.
          const _rePartyId = partySubscriptions.get(clientUid);
          if (_rePartyId) {
            const _reParty = dbGetParty(_rePartyId);
            if (_reParty) {
              const _reMembers = _reParty.members || [];
              if (_reMembers.length === 1 && _reMembers[0].uid === clientUid) {
                console.log(`[PARTY] dissolving ghost solo party on auth_ok partyId=${_rePartyId} uid=${clientUid}`);
                _dissolveParty(_rePartyId);
                partySubscriptions.delete(clientUid);
                send(ws, { type: "party_update", partyId: _rePartyId, data: null });
              } else {
                send(ws, { type: "party_update", partyId: _rePartyId, data: _reParty });
              }
            } else {
              partySubscriptions.delete(clientUid);
              send(ws, { type: "party_update", partyId: _rePartyId, data: null });
            }
          }

          // Push guild data on connect
          try {
            const _lgMyGuild = stmt.getMemberGuild.get(clientUid);
            if (_lgMyGuild) {
              const _lgSave = _getCachedSave(clientUid);
              const _lgName = _lgSave?.player?.name;
              if (_lgName) stmt.updateGuildCharName.run(_lgName, _lgMyGuild.guild_id, clientUid);
              const _lgRow = stmt.getGuild.get(_lgMyGuild.guild_id);
              const _lgMembers = stmt.getGuildMembers.all(_lgMyGuild.guild_id);
              if (_lgRow) {
                send(ws, {
                  type: "guild_update",
                  guild: {
                    id: _lgRow.id,
                    name: _lgRow.name,
                    leaderUid: _lgRow.leader_uid,
                    members: _lgMembers.map(m => ({
                      uid: m.uid,
                      username: m.username,
                      charName: m.char_name,
                      role: m.role,
                      joinedAt: m.joined_at
                    }))
                  }
                });
              }
            } else {
              send(ws, { type: "guild_update", guild: null });
            }
          } catch(guildConnErr) { console.error("[GUILD] connect push error:", guildConnErr.message); }

        } catch (e) {
          console.error("[AUTH] error:", e.message);
          send(ws, { type:"auth_fail", reason: "server_error" });
        }
        break;
      }

      // ══════════════════════════════════════════════════════════════════════
      //  DATA ACCESS: Accounts, Saves, Charnames, Social, Profiles, Presence
      //  (accounts, saves, charnames, social, profiles, presence)
      // ══════════════════════════════════════════════════════════════════════

      // ── Get account by username ────────────────────────────────────────
      case "get_account": {
        if (!clientUid) return;
        const { username } = msg;
        if (!username) { send(ws, { type:"account_data", username, data: null }); break; }
        const row = stmt.getAccount.get(username.toLowerCase());
        send(ws, { type:"account_data", username: username.toLowerCase(), data: row ? { uid: row.uid, zone: row.zone, charName: row.charName, created: row.created } : null });
        break;
      }

      // ── Set/update account ─────────────────────────────────────────────
      case "set_account": {
        if (!clientUid) return;
        const { username: setAccUser, entry } = msg;
        if (!setAccUser || !entry) break;
        const uLower = setAccUser.toLowerCase();
        // Only allow setting your own account
        const setAccClient = clients.get(clientUid);
        const setAccUsername = setAccClient ? setAccClient.username.toLowerCase() : "";
        if (uLower !== setAccUsername && !ADMIN_UIDS.has(clientUid)) {
          flagAnomaly(clientUid, "set_account_other_user", { targetUser: uLower });
          break;
        }
        stmt.upsertAccount.run({
          username: uLower, uid: entry.uid || null, zone: entry.zone || null,
          charName: entry.charName || null, hash: null, created: entry.created || 0
        });
        break;
      }

      // ── Update account zone ────────────────────────────────────────────
      case "set_account_zone": {
        if (!clientUid) return;
        const { username: zoneUser, zone: zoneVal } = msg;
        if (!zoneUser) break;
        // Only allow setting your own zone
        const zoneClient = clients.get(clientUid);
        const zoneUsername = zoneClient ? zoneClient.username.toLowerCase() : "";
        if (zoneUser.toLowerCase() !== zoneUsername) break;
        stmt.updateAccountZone.run(zoneVal || null, zoneUser.toLowerCase());
        if (zoneClient) zoneClient.zone = zoneVal || null;
        break;
      }

      // ── Get save ───────────────────────────────────────────────────────
      case "get_save": {
        if (!clientUid) return;
        const { uid: saveUid } = msg;
        const targetUid = saveUid || clientUid;
        // Only allow reading your own save (admins can read others)
        if (targetUid !== clientUid && !ADMIN_UIDS.has(clientUid)) {
          flagAnomaly(clientUid, "get_save_other_uid", { targetUid });
          send(ws, { type:"save_data", uid: targetUid, data: null });
          break;
        }
        const save = _getCachedSave(targetUid);
        const _ep = save && save.player ? {
          name:save.player.name, hp:save.player.hp, maxHp:save.player.maxHp,
          baseMaxHp:save.player.baseMaxHp, gold:save.player.gold, energy:save.player.energy,
          isAlive:save.player.isAlive, respawnZone:save.player.respawnZone,
          lastZone:save.player.lastZone, actionSlots:save.player.actionSlots,
          learnedActions:save.player.learnedActions, cooldowns:save.player.cooldowns,
          equipment:save.player.equipment, title:save.player.title,
        } : null;
        const _es = save ? { charCreated:save.charCreated, partyId:save.partyId||null,
          provisionCooldownEnd:save.provisionCooldownEnd||null, exploreCooldownEnd:save.exploreCooldownEnd||null,
          fishCooldownEnd:save.fishCooldownEnd||null, travelCooldownEnd:save.travelCooldownEnd||null,
          savedCombat:save.savedCombat||null, player:_ep } : null;
        send(ws, { type:"save_data", uid: targetUid, data: _es });
        break;
      }

      // ── Patch save (client-safe fields only) ───────────────────────────
      case "patch_save": {
        if (!clientUid) return;
        const { fields } = msg;
        if (!fields || typeof fields !== "object") break;
        let save = _getCachedSave(clientUid);
        if (!save) break;
        // Expand short keys to full save paths
        const SHORT_KEY_MAP = {
          "p/nm":"player/name","p/mhp":"player/baseMaxHp","p/as":"player/actionSlots",
          "p/la":"player/learnedActions","p/rz":"player/respawnZone","p/cd":"player/cooldowns",
          "p/lz":"player/lastZone","p/ls":"player/lastSeen","p/en":"player/energy",
          "p/ia":"player/isAlive","p/pt":"player/totalPlaytime","pce":"provisionCooldownEnd","ece":"exploreCooldownEnd",
          "fce":"fishCooldownEnd","tce":"travelCooldownEnd","pid":"partyId",
          "cc":"charCreated","sc":"savedCombat",
        };
        // Whitelist: only these paths can be written by the client (hoisted to module level)
        let changed = false;
        for (let [pathStr, value] of Object.entries(fields)) {
          // Expand short key if present
          if (SHORT_KEY_MAP[pathStr]) pathStr = SHORT_KEY_MAP[pathStr];
          // Handle short sub-paths like p/cd/somekey
          else if (pathStr.startsWith("p/cd/")) pathStr = "player/cooldowns/" + pathStr.slice(5);
          // Check exact match or if it's a sub-path of an allowed field
          let allowed = SAFE_FIELDS.has(pathStr);
          if (!allowed) {
            for (const sf of SAFE_FIELDS) {
              if (pathStr.startsWith(sf + "/")) { allowed = true; break; }
            }
          }
          if (!allowed) {
            flagAnomaly(clientUid, "patch_save_blocked", { path: pathStr, value: typeof value === "object" ? "[object]" : String(value).slice(0, 100) });
            continue;
          }
          const parts = pathStr.split("/");
          // Block prototype pollution
          if (parts.some(p => DANGEROUS_KEYS.has(p))) {
            flagAnomaly(clientUid, "patch_save_proto_pollution", { path: pathStr });
            continue;
          }
          let obj = save;
          for (let i = 0; i < parts.length - 1; i++) {
            if (!obj[parts[i]]) obj[parts[i]] = {};
            obj = obj[parts[i]];
          }
          obj[parts[parts.length - 1]] = value;
          changed = true;
        }
        if (changed) _writeSave(clientUid, save);
        break;
      }

      // ── Get charname ───────────────────────────────────────────────────
      case "get_charname": {
        if (!clientUid) return;
        const { name } = msg;
        if (!name) { send(ws, { type:"charname_data", name, data: null }); break; }
        const row = stmt.getCharname.get(name.toLowerCase());
        send(ws, { type:"charname_data", name: name.toLowerCase(), data: row ? { username: row.username, uid: row.uid } : null });
        break;
      }

      // ── Register charname ──────────────────────────────────────────────
      case "set_charname": {
        if (!clientUid) return;
        const { name: cnName } = msg;
        if (!cnName || typeof cnName !== "string") break;
        // Force username from authenticated session
        const cnClient = clients.get(clientUid);
        const cnUser = cnClient ? cnClient.username : null;
        if (!cnUser) break;
        // Prevent overwriting charnames owned by other UIDs
        const existingCn = stmt.getCharname.get(cnName.toLowerCase());
        if (existingCn && existingCn.uid && existingCn.uid !== clientUid) {
          flagAnomaly(clientUid, "charname_hijack_attempt", { name: cnName, existingUid: existingCn.uid });
          break;
        }
        stmt.upsertCharname.run(cnName.toLowerCase(), cnUser, clientUid);
        send(ws, { type:"charname_saved", name: cnName.toLowerCase() });
        break;
      }

      // ── Get social data ────────────────────────────────────────────────
      case "get_social": {
        if (!clientUid) return;
        // Only allow reading your own social data
        const data = dbGetSocial(clientUid);
        send(ws, { type:"social_data", uid: clientUid, data });
        break;
      }

      // ── Save social data ───────────────────────────────────────────────
      case "set_social": {
        if (!clientUid) return;
        const { data: socialData } = msg;
        if (!socialData || typeof socialData !== "object") break;
        // Whitelist allowed social fields - prevent injecting arbitrary data
        const safeSocial = {};
        const arrFields = ["friends","pendingIn","pendingOut","declinedBy","blocked"];
        for (const f of arrFields) {
          if (Array.isArray(socialData[f])) safeSocial[f] = socialData[f].slice(0, 200); // cap array size
          else if (socialData[f]) safeSocial[f] = [];
        }
        // Guard against stale set_social re-adding friends removed server-side by unfriend_user
        // or block_user. A set_social that was in-flight when the server processed the unfriend
        // can race and overwrite the authoritative removal. Fix: only keep friends that already
        // exist in the DB — set_social cannot re-add someone who was removed server-side.
        // pendingIn/pendingOut/blocked/declinedBy pass through unchanged (client is authoritative).
        try {
          const _dbSocial = dbGetSocial(clientUid);
          if (_dbSocial && Array.isArray(_dbSocial.friends) && Array.isArray(safeSocial.friends)) {
            const _dbFriendUids  = new Set(_dbSocial.friends.map(f => f.uid).filter(Boolean));
            const _dbFriendNames = new Set(_dbSocial.friends.map(f => (f.name || "").toLowerCase()).filter(Boolean));
            safeSocial.friends = safeSocial.friends.filter(f =>
              (f.uid && _dbFriendUids.has(f.uid)) ||
              (!f.uid && f.name && _dbFriendNames.has((f.name || "").toLowerCase()))
            );
          }
        } catch(e) { /* non-fatal: fall through with unfiltered list */ }
        stmt.upsertSocial.run(clientUid, JSON.stringify(safeSocial));
        break;
      }

      // ── Get profile ────────────────────────────────────────────────────
      case "get_profile": {
        if (!clientUid) return;
        const { uid: profUid, hash: clientHash } = msg;
        if (!profUid) break;
        const row = stmt.getProfile.get(profUid);
        if (!row) { send(ws, { type:"profile_data", uid: profUid, data: null }); break; }
        if (clientHash && clientHash === row.hash) {
          send(ws, { type:"profile_data", uid: profUid, data: null, unchanged: true });
          break;
        }
        let data = null;
        try { data = JSON.parse(row.data); } catch {}
        send(ws, { type:"profile_data", uid: profUid, data, hash: row.hash });
        break;
      }

      // ── Set profile ────────────────────────────────────────────────────
      case "set_profile": {
        if (!clientUid) return;
        const { data: profData } = msg;
        if (!profData || typeof profData !== "object") break;
        // Whitelist allowed profile fields and cap sizes
        const safeProfile = {};
        const strFields = ["name","title","actionSlots"];
        const numFields = ["hp","maxHp"];
        const objFields = ["equipment","stats"];
        for (const f of strFields) { if (profData[f] != null) safeProfile[f] = profData[f]; }
        for (const f of numFields) { if (typeof profData[f] === "number") safeProfile[f] = profData[f]; }
        for (const f of objFields) { if (profData[f] && typeof profData[f] === "object") safeProfile[f] = profData[f]; }
        const json = JSON.stringify(safeProfile);
        if (json.length > 5000) break; // cap total size
        stmt.upsertProfile.run(clientUid, json, _profileHash(json));
        break;
      }

      // ══════════════════════════════════════════════════════════════════════
      //  INBOX: Friend requests, acks, removals, cancels, party invites
      //  (inbox rows stored in SQLite inbox table per uid)
      // ══════════════════════════════════════════════════════════════════════

      // ── Request inbox (client pull on login) ───────────────────────────
      // Client sends this on every startGame() as a belt-and-suspenders pull
      // to ensure friend requests sent while offline are never missed, even
      // if the auth-time inbox_update push was dropped or arrived too early.
      case "request_inbox": {
        if (!clientUid) break;
        const riInbox = dbGetInboxObject(clientUid);
        // Always send — even if empty — so client knows the pull completed.
        send(ws, { type: "inbox_update", data: riInbox });
        break;
      }

      // ── Send friend request ────────────────────────────────────────────
      case "send_friend_req": {
        if (!clientUid) return;
        const { targetUid } = msg;
        if (!targetUid) break;
        if (targetUid === clientUid) break; // can't friend yourself
        // Force sender to be the authenticated user
        const frClient = clients.get(clientUid);
        const frSender = frClient ? frClient.username.toLowerCase() : "";
        if (!frSender) break;
        // Self-block guard: reject if target has blocked sender
        const frTargetSocial = dbGetSocial(targetUid);
        if (frTargetSocial) {
          const frBlocked = (frTargetSocial.blocked || []).map(b => (typeof b === "string" ? b : (b.name || "")).toLowerCase());
          if (frBlocked.includes(frSender)) break;
        }
        stmt.upsertInbox.run(targetUid, "freqs", frSender, JSON.stringify(Date.now()), Date.now());
        // Clean up any stale facks or fcancels so re-sending after cancel/decline works cleanly
        stmt.deleteInbox.run(targetUid, "fcancels", frSender);
        _broadcastInbox(targetUid);
        break;
      }

      // ── Delete friend request ──────────────────────────────────────────
      case "delete_friend_req": {
        if (!clientUid) return;
        const { targetUid: dfrUid, senderUsername: dfrUser } = msg;
        if (!dfrUid || !dfrUser) break;
        // Only allow: deleting from own inbox, or cancelling a request you sent
        const dfrClient = clients.get(clientUid);
        const dfrMyUser = dfrClient ? dfrClient.username.toLowerCase() : "";
        const isOwnInbox = dfrUid === clientUid;
        const isOwnRequest = dfrUser.toLowerCase() === dfrMyUser;
        if (!isOwnInbox && !isOwnRequest) {
          flagAnomaly(clientUid, "delete_freq_other_inbox", { targetUid: dfrUid, sender: dfrUser });
          break;
        }
        stmt.deleteInbox.run(dfrUid, "freqs", dfrUser.toLowerCase());
        break;
      }

      // ── Send friend ack ────────────────────────────────────────────────
      case "send_friend_ack": {
        if (!clientUid) return;
        const { targetUid: faUid, data: faData } = msg;
        if (!faUid) break;
        if (faUid === clientUid) break;
        // Force key to be the authenticated user's username
        const faClient = clients.get(clientUid);
        const faUser = faClient ? faClient.username.toLowerCase() : "";
        if (!faUser) break;
        // Belt-and-suspenders: also delete the freqs entry from our own inbox
        // In case client's delete_friend_req was dropped
        stmt.deleteInbox.run(clientUid, "freqs", faUser);
        stmt.upsertInbox.run(faUid, "facks", faUser, JSON.stringify(faData), Date.now());
        _broadcastInbox(faUid);

        // ── Persistence fix: if this is an accepted ack, write the friendship
        // directly into both players' social DB rows right now.
        // This prevents the set_social guard from stripping the new friend because
        // the entry didn't exist in the DB yet when the client's set_social arrives.
        if (faData && faData.accepted) {
          const faTs = faData.ts || Date.now();
          // Resolve target username (the original requester)
          const faTargetClient = clients.get(faUid);
          const faTargetUser = faTargetClient
            ? faTargetClient.username.toLowerCase()
            : (() => { const r = stmt.getAccountByUid.get(faUid); return r ? r.username.toLowerCase() : null; })();

          // Write accepter (clientUid) into requester's (faUid) social row
          if (faTargetUser) {
            const faTargetSocial = dbGetSocial(faUid) || { friends: [], pendingIn: [], pendingOut: [] };
            const alreadyInTarget = (faTargetSocial.friends || []).find(f => (f.name || "").toLowerCase() === faUser);
            if (!alreadyInTarget) {
              faTargetSocial.friends = [...(faTargetSocial.friends || []), { name: faUser, since: faTs }];
            }
            // Clear pendingOut entry for this accepter
            faTargetSocial.pendingOut = (faTargetSocial.pendingOut || []).filter(r => (r.name || "").toLowerCase() !== faUser);
            stmt.upsertSocial.run(faUid, JSON.stringify(faTargetSocial));
            // Push live update if requester is online
            if (faTargetClient && faTargetClient.ws.readyState === 1) {
              send(faTargetClient.ws, { type: "social_update", data: faTargetSocial });
            }
          }

          // Write requester (faUid) into accepter's (clientUid) social row
          const faAccepterSocial = dbGetSocial(clientUid) || { friends: [], pendingIn: [], pendingOut: [] };
          const alreadyInAccepter = (faAccepterSocial.friends || []).find(f => (f.name || "").toLowerCase() === (faTargetUser || ""));
          if (!alreadyInAccepter && faTargetUser) {
            faAccepterSocial.friends = [...(faAccepterSocial.friends || []), { name: faTargetUser, since: faTs }];
          }
          // Clear pendingIn entry for the requester
          faAccepterSocial.pendingIn = (faAccepterSocial.pendingIn || []).filter(r => (r.from || "").toLowerCase() !== (faTargetUser || ""));
          stmt.upsertSocial.run(clientUid, JSON.stringify(faAccepterSocial));
          // Push live update to accepter if online (they usually are since they just sent this)
          if (faClient && faClient.ws.readyState === 1) {
            send(faClient.ws, { type: "social_update", data: faAccepterSocial });
          }
        }
        break;
      }

      // ── Delete friend ack ──────────────────────────────────────────────
      case "delete_friend_ack": {
        if (!clientUid) return;
        const { key: dfaKey } = msg;
        if (!dfaKey) break;
        // Only delete from your own inbox
        stmt.deleteInbox.run(clientUid, "facks", dfaKey);
        break;
      }

      // ── Send friend removal ────────────────────────────────────────────
      // ── Unfriend: atomically remove both players from each other's friends lists ──
      case "unfriend_user": {
        if (!clientUid) return;
        let { targetUid: ufTargetUid, targetUsername: ufTargetUsernameMsg } = msg;

        // Client sends targetUsername directly so no async uid lookup is needed client-side.
        // Resolve uid here using the accounts table.
        if (!ufTargetUid && ufTargetUsernameMsg) {
          const row = stmt.getAccount.get(ufTargetUsernameMsg.toLowerCase());
          if (row) ufTargetUid = row.uid;
        }

        if (!ufTargetUid || ufTargetUid === clientUid) break;

        const ufInitClient = clients.get(clientUid);
        // Resolve initiator username from live client or accounts table as fallback
        const ufInitUsername = ufInitClient
          ? ufInitClient.username.toLowerCase()
          : (() => { const r = stmt.getAccountByUid.get(clientUid); return r ? r.username.toLowerCase() : null; })();

        // Resolve target's username from online clients or their social row
        const ufTargetClient = clients.get(ufTargetUid);
        const ufTargetUsername = ufTargetClient
          ? ufTargetClient.username.toLowerCase()
          : (() => {
              const row = stmt.getAccountByUid.get(ufTargetUid);
              return row ? row.username.toLowerCase() : null;
            })();

        // ── Remove initiator from TARGET's social row ──
        const ufTargetSocial = dbGetSocial(ufTargetUid) || { friends: [], pendingIn: [], pendingOut: [] };
        if (ufTargetSocial) {
          // Filter by name if known, always also filter by uid (belt-and-suspenders)
          if (ufInitUsername) {
            ufTargetSocial.friends    = (ufTargetSocial.friends    || []).filter(f => (f.name || "").toLowerCase() !== ufInitUsername);
            ufTargetSocial.pendingIn  = (ufTargetSocial.pendingIn  || []).filter(r => (r.from || "").toLowerCase() !== ufInitUsername);
            ufTargetSocial.pendingOut = (ufTargetSocial.pendingOut || []).filter(r => (r.name || "").toLowerCase() !== ufInitUsername);
          }
          // Always filter by uid so removal works even if username lookup failed
          ufTargetSocial.friends    = (ufTargetSocial.friends    || []).filter(f => f.uid !== clientUid);
          ufTargetSocial.pendingIn  = (ufTargetSocial.pendingIn  || []).filter(r => r.uid !== clientUid);
          ufTargetSocial.pendingOut = (ufTargetSocial.pendingOut || []).filter(r => r.uid !== clientUid);
          stmt.upsertSocial.run(ufTargetUid, JSON.stringify(ufTargetSocial));
          // Push live update to target if online
          if (ufTargetClient && ufTargetClient.ws.readyState === 1) {
            send(ufTargetClient.ws, { type: "social_update", data: ufTargetSocial });
          }
          // Inbox signal so target sees removal message
          stmt.upsertInbox.run(ufTargetUid, "frems", ufInitUsername, JSON.stringify(1), Date.now());
          _broadcastInbox(ufTargetUid);
        }

        // ── Remove target from INITIATOR's social row ──
        // Resolve ufTargetUsername from initiator's own social data as a fallback
        // so fack cleanup works even when the target has no accounts row.
        let _resolvedTargetUsername = ufTargetUsername;
        if (!_resolvedTargetUsername) {
          const ufInitSocialForLookup = dbGetSocial(clientUid);
          if (ufInitSocialForLookup) {
            const matchFriend = (ufInitSocialForLookup.friends || []).find(f => f.uid === ufTargetUid);
            const matchPending = !matchFriend && (ufInitSocialForLookup.pendingOut || []).find(r => r.uid === ufTargetUid);
            const match = matchFriend || matchPending;
            if (match) _resolvedTargetUsername = (match.name || "").toLowerCase() || null;
          }
        }

        const ufInitSocial = dbGetSocial(clientUid);
        if (ufInitSocial) {
          const _filterName = _resolvedTargetUsername;
          if (_filterName) {
            ufInitSocial.friends    = (ufInitSocial.friends    || []).filter(f => (f.name || "").toLowerCase() !== _filterName);
            ufInitSocial.pendingIn  = (ufInitSocial.pendingIn  || []).filter(r => (r.from || "").toLowerCase() !== _filterName);
            ufInitSocial.pendingOut = (ufInitSocial.pendingOut || []).filter(r => (r.name || "").toLowerCase() !== _filterName);
          }
          // Always filter by uid as a belt-and-suspenders approach
          ufInitSocial.friends    = (ufInitSocial.friends    || []).filter(f => f.uid !== ufTargetUid);
          ufInitSocial.pendingIn  = (ufInitSocial.pendingIn  || []).filter(r => r.uid !== ufTargetUid);
          ufInitSocial.pendingOut = (ufInitSocial.pendingOut || []).filter(r => r.uid !== ufTargetUid);
          stmt.upsertSocial.run(clientUid, JSON.stringify(ufInitSocial));
          // Push updated social back to initiator so their UI stays in sync
          if (ufInitClient && ufInitClient.ws.readyState === 1) {
            send(ufInitClient.ws, { type: "social_update", data: ufInitSocial });
          }
        }

        // Delete any stale facks between the two players from BOTH inboxes.
        // A lingering fack (friend-accept ack) will re-add the unfriended player
        // on the next login when _processInboxData runs and sees them missing.
        // Fast path: delete by resolved username key (covers the common case).
        if (_resolvedTargetUsername) {
          stmt.deleteInbox.run(clientUid,   "facks", _resolvedTargetUsername);
          stmt.deleteInbox.run(ufTargetUid, "facks", ufInitUsername || "");
        }
        // Unconditional scan: delete ALL facks in both inboxes whose data.from
        // matches the other party — catches mismatched keys and the case where
        // username resolution failed entirely. This is the authoritative cleanup.
        try {
          const _ufInitFacks = db.prepare(
            "SELECT entry_key, data FROM inbox WHERE target_uid=? AND category='facks'"
          ).all(clientUid);
          for (const row of _ufInitFacks) {
            try {
              const d = row.data ? JSON.parse(row.data) : null;
              const rowFrom = (d && (d.from || "")).toLowerCase();
              if (
                (_resolvedTargetUsername && (row.entry_key === _resolvedTargetUsername || rowFrom === _resolvedTargetUsername))
              ) {
                stmt.deleteInbox.run(clientUid, "facks", row.entry_key);
              }
            } catch(e) {}
          }
        } catch(e) {}
        // Also scan target's inbox for facks referencing the initiator.
        if (ufInitUsername) {
          try {
            const _ufTargetFacks = db.prepare(
              "SELECT entry_key, data FROM inbox WHERE target_uid=? AND category='facks'"
            ).all(ufTargetUid);
            for (const row of _ufTargetFacks) {
              try {
                const d = row.data ? JSON.parse(row.data) : null;
                const rowFrom = (d && (d.from || "")).toLowerCase();
                if (row.entry_key === ufInitUsername || rowFrom === ufInitUsername) {
                  stmt.deleteInbox.run(ufTargetUid, "facks", row.entry_key);
                }
              } catch(e) {}
            }
          } catch(e) {}
        }
        break;
      }

      case "send_friend_removal": {
        if (!clientUid) return;
        const { targetUid: frUid } = msg;
        if (!frUid) break;
        if (frUid === clientUid) break;
        const frClient = clients.get(clientUid);
        const frUser = frClient ? frClient.username.toLowerCase() : "";
        if (!frUser) break;
        // Directly update target's social data so removal persists even if they're offline
        const frSocial = dbGetSocial(frUid);
        if (frSocial) {
          let frChanged = false;
          const frBefore = (frSocial.friends || []).length;
          frSocial.friends = (frSocial.friends || []).filter(f => (f.name || "").toLowerCase() !== frUser);
          if (frSocial.friends.length !== frBefore) frChanged = true;
          frSocial.pendingIn = (frSocial.pendingIn || []).filter(r => (r.from || "").toLowerCase() !== frUser);
          frSocial.pendingOut = (frSocial.pendingOut || []).filter(r => (r.name || "").toLowerCase() !== frUser);
          if (frChanged) {
            stmt.upsertSocial.run(frUid, JSON.stringify(frSocial));
            const frTargetClient = clients.get(frUid);
            if (frTargetClient && frTargetClient.ws.readyState === 1) {
              send(frTargetClient.ws, { type: "social_update", data: frSocial });
            }
          }
        }
        // Also signal via inbox so the client shows a removal log message
        stmt.upsertInbox.run(frUid, "frems", frUser, JSON.stringify(1), Date.now());
        _broadcastInbox(frUid);
        break;
      }

      // ── Block user: remove blocker from blocked player's social data directly ──
      case "block_user": {
        if (!clientUid) return;
        const { targetUid: buTargetUid } = msg;
        if (!buTargetUid) break;
        if (buTargetUid === clientUid) break;
        // blockerUsername MUST come from the authenticated session, never from
        // the client message — accepting it from the client would let any user
        // impersonate another player's block action.
        const blockerClient = clients.get(clientUid);
        if (!blockerClient) break;
        const blockerLower = blockerClient.username.toLowerCase();
        if (!blockerLower) break;

        // Remove blocker from the BLOCKED player's social row (unconditionally)
        const buSocial = dbGetSocial(buTargetUid);
        if (buSocial) {
          buSocial.friends    = (buSocial.friends    || []).filter(f => (f.name || "").toLowerCase() !== blockerLower);
          buSocial.pendingIn  = (buSocial.pendingIn  || []).filter(r => (r.from || "").toLowerCase() !== blockerLower);
          buSocial.pendingOut = (buSocial.pendingOut || []).filter(r => (r.name || "").toLowerCase() !== blockerLower);
          stmt.upsertSocial.run(buTargetUid, JSON.stringify(buSocial));
          const buClient = clients.get(buTargetUid);
          if (buClient && buClient.ws.readyState === 1) {
            send(buClient.ws, { type: "social_update", data: buSocial });
          }
        }
        // Inbox signal so blocked player sees a removal message
        const frUser2 = blockerLower;
        stmt.upsertInbox.run(buTargetUid, "frems", frUser2, JSON.stringify(1), Date.now());
        _broadcastInbox(buTargetUid);

        // Remove blocked target from BLOCKER's own social row (unconditionally)
        const blockerSocial = dbGetSocial(clientUid);
        if (blockerSocial) {
          const buTargetUsername = (() => {
            const buC = clients.get(buTargetUid);
            if (buC) return buC.username.toLowerCase();
            // Try friends/pending lists first (no DB hit)
            const f = (blockerSocial.friends || []).find(fr => fr.uid === buTargetUid)
                   || (blockerSocial.pendingOut || []).find(fr => fr.uid === buTargetUid)
                   || (blockerSocial.pendingIn  || []).find(fr => fr.uid === buTargetUid);
            if (f) return (f.name || f.from || "").toLowerCase();
            // Last resort: accounts table (target offline, not in lists)
            const row = stmt.getAccountByUid.get(buTargetUid);
            return row ? row.username.toLowerCase() : null;
          })();
          // Always filter by uid so cleanup works even if username resolution failed
          blockerSocial.friends    = (blockerSocial.friends    || []).filter(f => f.uid !== buTargetUid && (!buTargetUsername || (f.name || "").toLowerCase() !== buTargetUsername));
          blockerSocial.pendingIn  = (blockerSocial.pendingIn  || []).filter(r => r.uid !== buTargetUid && (!buTargetUsername || (r.from || "").toLowerCase() !== buTargetUsername));
          blockerSocial.pendingOut = (blockerSocial.pendingOut || []).filter(r => r.uid !== buTargetUid && (!buTargetUsername || (r.name || "").toLowerCase() !== buTargetUsername));
          stmt.upsertSocial.run(clientUid, JSON.stringify(blockerSocial));
          // Push social_update back to blocker so their client stays in sync with DB
          if (blockerClient && blockerClient.ws.readyState === 1) {
            send(blockerClient.ws, { type: "social_update", data: blockerSocial });
          }
          // Delete stale facks between the two players from BOTH inboxes so a
          // lingering accept-ack can never re-add the blocked player on next login.
          if (buTargetUsername) {
            stmt.deleteInbox.run(clientUid,   "facks", buTargetUsername);
            stmt.deleteInbox.run(buTargetUid, "facks", blockerLower);
          }
        }
        break;
      }

      // ── Delete friend removal ──────────────────────────────────────────
      case "delete_friend_removal": {
        if (!clientUid) return;
        const { senderUsername: dfrUser2 } = msg;
        if (!dfrUser2) break;
        stmt.deleteInbox.run(clientUid, "frems", dfrUser2.toLowerCase());
        break;
      }

      // ── Send cancel request ────────────────────────────────────────────
      case "send_cancel_req": {
        if (!clientUid) return;
        const { targetUid: crUid } = msg;
        if (!crUid) break;
        if (crUid === clientUid) break;
        const crClient = clients.get(clientUid);
        const crUser = crClient ? crClient.username.toLowerCase() : "";
        if (!crUser) break;
        stmt.upsertInbox.run(crUid, "fcancels", crUser, JSON.stringify(1), Date.now());
        _broadcastInbox(crUid);
        break;
      }

      // ── Delete cancel request ──────────────────────────────────────────
      case "delete_cancel_req": {
        if (!clientUid) return;
        const { senderUsername: dcrUser } = msg;
        if (!dcrUser) break;
        stmt.deleteInbox.run(clientUid, "fcancels", dcrUser.toLowerCase());
        break;
      }

      // ── Delete inbox category (bulk) ───────────────────────────────────
      case "delete_inbox_category": {
        if (!clientUid) return;
        const { category: dicCat } = msg;
        if (!dicCat) break;
        stmt.deleteInboxCat.run(clientUid, dicCat);
        break;
      }

      // ── Send party invite ──────────────────────────────────────────────
      case "send_party_invite": {
        const _piDbg = (m) => { try { send(ws, { type:"zone_chat_msg", name:"[SRV]", msg:m, zone:"system" }); } catch(e){} };
        if (!clientUid) { _piDbg("DROP: no clientUid"); return; }
        if (!_rateOk(clientUid, "invite")) { _piDbg("DROP: rate limited"); return; }
        const { targetUid: piUid, key: piKey, data: piData } = msg;
        _piDbg(`recv targetUid=${piUid} partyId=${piData&&piData.p}`);
        if (!piUid || !piKey || !piData) { _piDbg("DROP: missing fields"); break; }
        if (piUid === clientUid) { _piDbg("DROP: self-invite"); break; }
        // Block if sender is in combat
        if (findRoomForUid(clientUid)) { send(ws, { type:"zone_chat_msg", name:"System", msg:"You cannot send party invites while in combat.", zone:"system" }); break; }
        // Block if target is in combat
        if (findRoomForUid(piUid)) { send(ws, { type:"zone_chat_msg", name:"System", msg:"That player is currently in combat.", zone:"system" }); break; }

        const piPartyId = piData.p;
        if (!piPartyId) { _piDbg("DROP: no partyId in data"); break; }
        const piParty = dbGetParty(piPartyId);
        if (!piParty) { _piDbg(`DROP: party not found partyId=${piPartyId}`); break; }
        if (!(piParty.members||[]).some(m => m.uid === clientUid)) { _piDbg("DROP: sender not in party"); break; }

        if (piData.t && Date.now() - piData.t > 120000) { _piDbg("DROP: stale timestamp"); break; }

        const piSenderClient = clients.get(clientUid);
        const piSenderUsername = (piSenderClient && piSenderClient.username || "").toLowerCase();
        if (!piSenderUsername) { _piDbg("DROP: sender username not found"); break; }
        const piTargetClient = clients.get(piUid);
        const piTargetRow = !piTargetClient ? stmt.getAccountByUid.get(piUid) : null;
        const piTargetUsername = (piTargetClient && piTargetClient.username || (piTargetRow && piTargetRow.username) || "").toLowerCase();
        if (!piTargetUsername) { _piDbg(`DROP: target username not found uid=${piUid}`); break; }
        const piMemFriends = _uidToFriends.get(clientUid);
        const piMemHasFriend = piMemFriends && piMemFriends.has(piTargetUsername);
        if (!piMemHasFriend) {
          const piSenderSocial = dbGetSocial(clientUid);
          const piDbFriends = ((piSenderSocial && piSenderSocial.friends) || []).map(f => (f.name||"").toLowerCase());
          if (!piDbFriends.includes(piTargetUsername)) { _piDbg(`DROP: not friends sender=${piSenderUsername} target=${piTargetUsername}`); break; }
        }

        _piDbg(`OK writing inbox uid=${piUid}`);
        stmt.upsertInbox.run(piUid, "invites", piKey, JSON.stringify(piData), Date.now());
        _broadcastInbox(piUid);
        break;
      }

      // ── Delete party invite ────────────────────────────────────────────
      case "delete_party_invite": {
        if (!clientUid) return;
        const { key: dpiKey } = msg;
        if (!dpiKey) break;
        // Only delete from your own inbox
        stmt.deleteInbox.run(clientUid, "invites", dpiKey);
        break;
      }

      // ── Send party ping ────────────────────────────────────────────────
      case "send_party_ping": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const { targetUid: ppUid } = msg;
        if (!ppUid) break;
        if (ppUid === clientUid) break;
        stmt.upsertInbox.run(ppUid, "ppings", "ping_" + Date.now(), JSON.stringify(1), Date.now());
        _broadcastInbox(ppUid);
        break;
      }

      // ══════════════════════════════════════════════════════════════════════
      //  DIRECT MESSAGES
      // ══════════════════════════════════════════════════════════════════════

      case "send_dm": {
        if (!clientUid) return;
        const { targetUid: dmTargetUid, message: dmMsg } = msg;
        if (!dmTargetUid || !dmMsg) break;
        if (dmTargetUid === clientUid) break; // can't DM yourself
        if (typeof dmMsg !== "string" || dmMsg.length > 500) break; // length limit
        // Force sender to be the authenticated user
        const dmClient = clients.get(clientUid);
        const dmSender = dmClient ? dmClient.username : "";
        const dmSave = _getCachedSave(clientUid);
        const dmSenderName = dmSave?.player?.name || dmSender;
        const dmKey = dmSender + "_" + Date.now();
        stmt.insertDm.run(dmTargetUid, dmKey, dmSender, dmSenderName, dmMsg, Date.now());
        _broadcastDms(dmTargetUid);
        break;
      }

      case "delete_dm": {
        if (!clientUid) return;
        const { key: delDmKey } = msg;
        if (!delDmKey) break;
        stmt.deleteDm.run(clientUid, delDmKey);
        break;
      }

      // ══════════════════════════════════════════════════════════════════════
      //  PARTIES
      // ══════════════════════════════════════════════════════════════════════

      // ══════════════════════════════════════════════════════════════════════
      //  AUTHORITATIVE PARTY HANDLERS
      //  Server owns all party state. No client writes directly to party doc.
      //  All changes go through these handlers; server pushes to all subscribers.
      // ══════════════════════════════════════════════════════════════════════

      // Subscribe to party updates — call on connect/reconnect when already in a party
      case "party_subscribe": {
        if (!clientUid) return;
        const { partyId: psId } = msg;
        if (!psId) break;
        const psSave = _getCachedSave(clientUid);
        const psDoc = dbGetParty(psId);
        const alreadyMember = psSave && psSave.partyId === psId;
        const inDoc = psDoc && (psDoc.members||[]).some(m => m.uid === clientUid);
        if (!alreadyMember && !inDoc) break;
        if (!psDoc) {
          send(ws, { type: "party_update", partyId: psId, data: null });
          break;
        }
        // Ghost solo party: player is the only member. Dissolve so they start clean.
        const psMembers = psDoc.members || [];
        if (psMembers.length === 1 && psMembers[0].uid === clientUid) {
          console.log(`[PARTY] dissolving ghost solo party on subscribe partyId=${psId} uid=${clientUid}`);
          _dissolveParty(psId);
          send(ws, { type: "party_update", partyId: psId, data: null });
          break;
        }
        partySubscriptions.set(clientUid, psId);
        send(ws, { type: "party_update", partyId: psId, data: psDoc });
        break;
      }

      // Create a new party (leader sends this when inviting the first member)
      case "party_create": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const pcClient = clients.get(clientUid);
        const pcUsername = pcClient ? pcClient.username : "";
        if (!pcUsername) break;
        const pcSave = _getCachedSave(clientUid);
        if (!pcSave || !pcSave.player) break;
        const partyId = msg.partyId || (pcUsername + "_" + Date.now());
        // Don't create if already in a real party — but auto-clear stale subscriptions
        // (e.g. previous solo party was cleaned up server-side without party_leave)
        const pcExistingSub = partySubscriptions.get(clientUid);
        if (pcExistingSub) {
          const pcExistingDoc = dbGetParty(pcExistingSub);
          const pcStillIn = pcExistingDoc && (pcExistingDoc.members||[]).some(m => m.uid === clientUid);
          if (pcStillIn) break; // genuinely in a party — don't create
          partySubscriptions.delete(clientUid);
          console.log(`[PARTY] cleared stale sub before party_create uid=${clientUid} oldPartyId=${pcExistingSub}`);
        }
        const partyDoc = {
          leader: pcUsername, leaderUid: clientUid,
          members: [{ username: pcUsername, charName: pcSave.player.name, uid: clientUid,
            hp: pcSave.player.hp, maxHp: pcSave.player.maxHp }],
          zone: pcSave.player.lastZone || _defaultRespawnZone(), ts: Date.now(),
        };
        dbSetParty(partyId, partyDoc);
        partySubscriptions.set(clientUid, partyId);
        send(ws, { type: "party_created", partyId, data: partyDoc });
        // Also push as party_update so same handler applies on client
        send(ws, { type: "party_update", partyId, data: partyDoc });
        console.log(`[PARTY] created partyId=${partyId} leader=${pcUsername}`);
        break;
      }

      // Join a party (invitee sends this after accepting invite)
      case "party_join": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const pjClient = clients.get(clientUid);
        const pjUsername = pjClient ? pjClient.username : "";
        if (!pjUsername) break;
        const pjSave = _getCachedSave(clientUid);
        if (!pjSave || !pjSave.player) break;
        const { partyId: pjId } = msg;
        if (!pjId) break;
        // Check not already in a real party — auto-clear stale subscriptions.
        // A ghost entry can linger when a solo party is cleaned up server-side
        // before the client's party_leave message arrives.
        const pjExistingSub = partySubscriptions.get(clientUid);
        if (pjExistingSub) {
          const pjExistingDoc = dbGetParty(pjExistingSub);
          const pjStillInExisting = pjExistingDoc && (pjExistingDoc.members||[]).some(m => m.uid === clientUid);
          if (!pjStillInExisting) {
            // Ghost subscription — player is no longer in that party doc; clear it
            partySubscriptions.delete(clientUid);
            console.log(`[PARTY] cleared stale subscription uid=${clientUid} oldPartyId=${pjExistingSub}`);
          } else {
            send(ws, { type: "party_join_fail", reason: "already_in_party" }); break;
          }
        }
        const pjDoc = dbGetParty(pjId);
        if (!pjDoc) { send(ws, { type: "party_join_fail", reason: "not_found" }); break; }
        if ((pjDoc.members||[]).length >= 4) { send(ws, { type: "party_join_fail", reason: "full" }); break; }
        // Joiner must have a valid inbox invite for this specific party
        const pjInboxRows = stmt.getInbox.all(clientUid);
        const pjHasInvite = pjInboxRows.some(r => {
          if (r.category !== "invites") return false;
          try { const d = JSON.parse(r.data); return d && d.p === pjId; } catch { return false; }
        });
        if (!pjHasInvite) { send(ws, { type: "party_join_fail", reason: "not_invited" }); break; }
        // Add member
        const newMember = { username: pjUsername, charName: pjSave.player.name, uid: clientUid,
          hp: pjSave.player.hp, maxHp: pjSave.player.maxHp };
        pjDoc.members = [...(pjDoc.members||[]), newMember];
        pjDoc.ts = Date.now();
        dbSetParty(pjId, pjDoc);
        partySubscriptions.set(clientUid, pjId);
        // Clear all pending party invites - joiner is now in a party
        stmt.deleteInboxCat.run(clientUid, "invites");
        // Push updated doc to joiner and all existing subscribers
        _pushPartyToSubscribers(pjId, null);
        console.log(`[PARTY] join partyId=${pjId} uid=${clientUid} username=${pjUsername}`);
        break;
      }

      // Leave party voluntarily
      case "party_leave": {
        if (!clientUid) return;
        const plId = partySubscriptions.get(clientUid);
        if (!plId) break;
        const plDoc = dbGetParty(plId);
        if (!plDoc) { partySubscriptions.delete(clientUid); break; }
        const plClient = clients.get(clientUid);
        const plUsername = plClient ? plClient.username : "";
        partySubscriptions.delete(clientUid);
        const remaining = (plDoc.members||[]).filter(m => m.uid !== clientUid);
        if (remaining.length === 0) {
          // Last member — dissolve
          _dissolveParty(plId);
        } else {
          // Promote new leader if needed
          let newLeader = plDoc.leader;
          let newLeaderUid = plDoc.leaderUid;
          if (plDoc.leader === plUsername) {
            newLeader = remaining[0].username;
            newLeaderUid = remaining[0].uid || null;
          }
          const updated = { ...plDoc, leader: newLeader, leaderUid: newLeaderUid,
            members: remaining, ts: Date.now(), combatSignal: null, voteRequest: null, travelSignal: null };
          dbSetParty(plId, updated);
          _pushPartyToSubscribers(plId, null);
        }
        // Tell the leaving player they're out
        send(ws, { type: "party_update", partyId: plId, data: null });
        console.log(`[PARTY] leave partyId=${plId} uid=${clientUid}`);
        break;
      }

      // Kick a member (leader only)
      case "party_kick": {
        if (!clientUid) return;
        const pkId = partySubscriptions.get(clientUid);
        if (!pkId) break;
        const pkDoc = dbGetParty(pkId);
        if (!pkDoc) break;
        const pkClient = clients.get(clientUid);
        const pkUsername = pkClient ? pkClient.username : "";
        // Only leader can kick
        if (pkDoc.leader !== pkUsername) { flagAnomaly(clientUid, "party_kick_not_leader", { partyId: pkId }); break; }
        const { targetUid: pkTargetUid } = msg;
        if (!pkTargetUid || pkTargetUid === clientUid) break;
        const remaining = (pkDoc.members||[]).filter(m => m.uid !== pkTargetUid);
        const updated = { ...pkDoc, members: remaining, ts: Date.now() };
        dbSetParty(pkId, updated);
        // Notify kicked player
        partySubscriptions.delete(pkTargetUid);
        const pkKickedC = clients.get(pkTargetUid);
        if (pkKickedC && pkKickedC.ws.readyState === 1) pkKickedC.ws.send(JSON.stringify({ type: "party_update", partyId: pkId, data: null }));
        // Push updated doc to remaining subscribers
        _pushPartyToSubscribers(pkId, null);
        console.log(`[PARTY] kick partyId=${pkId} kicked=${pkTargetUid} by=${pkUsername}`);
        break;
      }

      // Send a signal (combat, travel) through the party doc
      case "party_signal": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const psigId = partySubscriptions.get(clientUid);
        if (!psigId) break;
        const psigDoc = dbGetParty(psigId);
        if (!psigDoc) break;
        const psigClient = clients.get(clientUid);
        const psigUsername = psigClient ? psigClient.username : "";
        // Only leader can signal
        if (psigDoc.leader !== psigUsername) break;
        const { signal, value, zoneValue } = msg;
        if (!signal) break;
        const validSignals = new Set(["combatSignal","travelSignal","voteRequest","zone","arenaQueueSignal"]);
        if (!validSignals.has(signal)) break;
        // If zoneValue is provided alongside travelSignal, update both atomically in one write
        const updated = (signal === "travelSignal" && zoneValue)
          ? { ...psigDoc, travelSignal: value, zone: zoneValue, ts: Date.now() }
          : { ...psigDoc, [signal]: value, ts: Date.now() };
        dbSetParty(psigId, updated);
        _pushPartyToSubscribers(psigId, null);
        break;
      }

      // Broadcast HP — lightweight, no full doc write for HP changes
      case "broadcast_hp":
      case "party_hp": {
        if (!clientUid) return;
        const bhPartyId = msg.partyId || partySubscriptions.get(clientUid);
        if (!bhPartyId) break;
        // Sender must be subscribed to this specific party
        if (partySubscriptions.get(clientUid) !== bhPartyId) break;
        const { hp: bhHp, maxHp: bhMaxHp } = msg;
        if (bhHp == null || bhMaxHp == null) break;
        _broadcastPartyHpPatch(bhPartyId, clientUid, bhHp, bhMaxHp);
        break;
      }

      // Vote on a combat/travel request
      case "party_vote": {
        if (!clientUid) return;
        const pvId = partySubscriptions.get(clientUid);
        if (!pvId) break;
        const pvClient = clients.get(clientUid);
        const pvUser = pvClient ? pvClient.username.toLowerCase() : "";
        if (!pvUser) break;
        const { vote: pvVote } = msg;
        stmt.setVote.run(pvId, pvUser, JSON.stringify(pvVote));
        // Push votes to all subscribers
        const pvParty = dbGetParty(pvId);
        if (pvParty && pvParty.members) {
          const pvRows = stmt.getVotes.all(pvId);
          const allVotes = {};
          for (const r of pvRows) { try { allVotes[r.username] = JSON.parse(r.vote); } catch { allVotes[r.username] = r.vote; } }
          for (const m of pvParty.members) {
            if (!m.uid) continue;
            const mc = clients.get(m.uid);
            if (mc && mc.ws.readyState === 1) send(mc.ws, { type: "votes_update", partyId: pvId, data: allVotes });
          }
        }
        break;
      }

      // Clear votes (leader after vote resolves)
      case "party_clear_votes": {
        if (!clientUid) return;
        const pcvId = partySubscriptions.get(clientUid);
        if (!pcvId) break;
        stmt.deleteVotes.run(pcvId);
        break;
      }

      // ── Legacy party handlers (kept for backward compat during transition) ───
      case "get_party": {
        if (!clientUid) return;
        const { partyId: gpId } = msg;
        if (!gpId) break;
        // Must actually be a member of this party to read its data — otherwise
        // any authenticated user who learns/guesses a partyId could read other
        // players' party composition, HP, etc.
        if (partySubscriptions.get(clientUid) !== gpId) break;
        const data = dbGetParty(gpId);
        send(ws, { type:"party_data", partyId: gpId, data });
        break;
      }

      case "set_party": {
        if (!clientUid) return;
        const { partyId: spId, data: spData } = msg;
        if (!spId) break;
        if (partySubscriptions.get(clientUid) !== spId) break;
        const spExisting = dbGetParty(spId);
        // Leader-only — this overwrites the ENTIRE party doc (membership,
        // leadership, signals, everything), so it needs the same authorization
        // as party_kick/party_signal, not just "currently subscribed to this party".
        const spClient = clients.get(clientUid);
        const spUsername = spClient ? spClient.username : "";
        if (!spExisting || spExisting.leader !== spUsername) { flagAnomaly(clientUid, "set_party_not_leader", { partyId: spId }); break; }
        if (spData === null) {
          _dissolveParty(spId);
        } else {
          dbSetParty(spId, spData);
          _pushPartyToSubscribers(spId, clientUid);
          send(ws, { type: "party_update", partyId: spId, data: spData });
        }
        break;
      }

      case "patch_party": {
        if (!clientUid) return;
        const { partyId: ppId, fields: ppFields } = msg;
        if (!ppId || !ppFields) break;
        if (partySubscriptions.get(clientUid) !== ppId) break;
        let existing = dbGetParty(ppId) || {};
        const ppClient = clients.get(clientUid);
        const ppUsername = ppClient ? ppClient.username : "";
        if (existing.leader !== ppUsername) { flagAnomaly(clientUid, "patch_party_not_leader", { partyId: ppId }); break; }
        // leader/leaderUid/members are deliberately NOT patchable here, even by
        // the leader — those have dedicated, properly-vetted flows (party_kick,
        // party_leave's auto-promotion) that also handle side effects like
        // notifying a kicked player. Allowing them through this generic patch
        // path would let a leader silently remove members without those
        // members ever being told they're out.
        const ppBlockedFields = new Set(["leader", "leaderUid", "members"]);
        const ppSafeFields = {};
        for (const k of Object.keys(ppFields)) { if (!ppBlockedFields.has(k)) ppSafeFields[k] = ppFields[k]; }
        const ppOldMembers = (existing.members || []).map(m => ({ uid: m.uid }));
        Object.assign(existing, ppSafeFields, { ts: Date.now() });
        dbSetParty(ppId, existing);
        const ppNewUids = new Set((existing.members || []).map(m => m.uid).filter(Boolean));
        _pushPartyToSubscribers(ppId, clientUid);
        send(ws, { type: "party_update", partyId: ppId, data: existing });
        // Notify removed members
        const ppRemovedStr = JSON.stringify({ type: "party_update", partyId: ppId, data: null });
        for (const old of ppOldMembers) {
          if (!old.uid || ppNewUids.has(old.uid)) continue;
          partySubscriptions.delete(old.uid);
          const mc = clients.get(old.uid);
          if (mc && mc.ws.readyState === 1) mc.ws.send(ppRemovedStr);
        }
        break;
      }

      case "delete_party": {
        if (!clientUid) return;
        const { partyId: dpId } = msg;
        if (!dpId) break;
        if (partySubscriptions.get(clientUid) !== dpId) break;
        const dpExisting = dbGetParty(dpId);
        const dpClient = clients.get(clientUid);
        const dpUsername = dpClient ? dpClient.username : "";
        // Leader-only — dissolving a party out from under everyone else is a
        // destructive leadership action, same authorization level as party_kick.
        if (!dpExisting || dpExisting.leader !== dpUsername) { flagAnomaly(clientUid, "delete_party_not_leader", { partyId: dpId }); break; }
        _dissolveParty(dpId);
        break;
      }

      case "set_vote": {
        if (!clientUid) return;
        const { partyId: svPid, vote: svVote } = msg;
        if (!svPid) break;
        const svClient = clients.get(clientUid);
        const svUser = svClient ? svClient.username.toLowerCase() : "";
        if (!svUser) break;
        stmt.setVote.run(svPid, svUser, JSON.stringify(svVote));
        const svParty = dbGetParty(svPid);
        if (svParty && svParty.members) {
          const rows = stmt.getVotes.all(svPid);
          const allVotes = {};
          for (const r of rows) { try { allVotes[r.username] = JSON.parse(r.vote); } catch { allVotes[r.username] = r.vote; } }
          for (const m of svParty.members) {
            if (!m.uid) continue;
            const mc = clients.get(m.uid);
            if (mc && mc.ws.readyState === 1) send(mc.ws, { type: "votes_update", partyId: svPid, data: allVotes });
          }
        }
        break;
      }

      case "get_votes": {
        if (!clientUid) return;
        const { partyId: gvPid } = msg;
        if (!gvPid) break;
        const rows = stmt.getVotes.all(gvPid);
        const votes = {};
        for (const r of rows) { try { votes[r.username] = JSON.parse(r.vote); } catch { votes[r.username] = r.vote; } }
        send(ws, { type:"votes_data", partyId: gvPid, data: votes });
        break;
      }

      case "delete_votes": {
        if (!clientUid) return;
        const { partyId: dvPid } = msg;
        if (!dvPid) break;
        stmt.deleteVotes.run(dvPid);
        break;
      }

      // ══════════════════════════════════════════════════════════════════════
      //  ZONE CHAT
      // ══════════════════════════════════════════════════════════════════════



      // ══════════════════════════════════════════════════════════════════════
      //  EXISTING COMBAT/GAME HANDLERS (unchanged logic, SQLite backend)
      // ══════════════════════════════════════════════════════════════════════

      case "start_combat": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (_inArenaQueue(clientUid)) { send(ws, { type:"error", reason:"in_arena_queue" }); return; }
        if (rooms.size >= ROOMS_WARN) console.warn(`[ROOMS] high room count: ${rooms.size} (pve=${rooms.size - _pvpRoomCount} pvp=${_pvpRoomCount}) — possible leak`);
        if (!_rateOk(clientUid, "start_combat")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        const cdCheck = _cooldownOk(clientUid, "explore");
        if (!cdCheck.ok) {
          flagAnomaly(clientUid, "explore_cooldown_bypass", { remaining: cdCheck.remaining });
          send(ws, { type:"error", reason:"explore_cooldown", remaining: cdCheck.remaining });
          return;
        }
        const { memberUids, zoneId } = msg;
        const zone = ZONE_DB[zoneId];
        const validZoneEnemies = (zone?.enemies || []).filter(e => ENEMY_DB[e]);
        if (!zone || zone.safe || !validZoneEnemies.length) {
          send(ws, { type:"error", reason:"invalid_zone" }); return;
        }
        const count = roll(1, 4);
        // Each slot rolls independently — same weighted table, fresh roll per slot
        const enemyList = Array.from({length: count}, (_, i) => ({
          uid: `e_${i}_${Date.now()}`, type: _pickZoneEnemy(zone)
        }));
        // Guard: if any slot has no type, the zone has no valid enemies
        if (enemyList.some(e => !e.type)) {
          send(ws, { type:"error", reason:"invalid_zone" }); return;
        }
        const partyId = msg.partyId || `solo_${clientUid}_${Date.now()}`;
        // Validate memberUids - only allow self for solo, or verify actual party membership
        let memberUidList;
        if (memberUids?.length > 1) {
          // Verify all UIDs are in the same party by checking the party doc
          const partyDoc = msg.partyId ? dbGetParty(msg.partyId) : null;
          if (!partyDoc) {
            memberUidList = [clientUid]; // no valid party, solo only
          } else {
            const partyUids = new Set((partyDoc.members || []).map(m => {
              const acc = stmt.getAccount.get(m.username?.toLowerCase());
              return acc?.uid || null;
            }).filter(Boolean));
            // Only include UIDs that are actually in this party
            memberUidList = memberUids.filter(uid => partyUids.has(uid));
            if (!memberUidList.includes(clientUid)) memberUidList = [clientUid];
            if (memberUidList.length === 0) memberUidList = [clientUid];
          }
        } else {
          memberUidList = [clientUid];
        }
        try {
          const room = await CombatRoom.create(partyId, memberUidList, enemyList);
          rooms.set(partyId, room);
          room.start();
          _ejectCheatersFromRoom(room, partyId);
          console.log(`[ROOM] created partyId=${partyId} members=${room.members.length} enemies=${enemyList.length} zone=${zoneId}`);
        } catch (e) {
          console.error("[ROOM] create error:", e);
          send(ws, { type:"error", reason:"start_failed" });
        }
        break;
      }

      case "rejoin_combat": {
        if (!clientUid) return;
        const { roomId } = msg;
        // Queue if boot restore not yet complete
        if (!_bootRestoreComplete) {
          _rejoinQueue.push({ uid: clientUid, ws, roomId });
          console.log(`[REJOIN] queued uid=${clientUid} room=${roomId} (boot restore pending)`);
          break;
        }
        const room = rooms.get(roomId);
        if (room && !room.ended) {
          const member = room.members.find(m => m.uid === clientUid);
          if (member) {
            if (room._waitingForMembers) {
              room._reconnectedUids.add(clientUid);
              console.log(`[REJOIN] uid=${clientUid} reconnected to waiting room=${roomId} (${room._reconnectedUids.size}/${room.members.length})`);
              room.sendFullState(clientUid);
              // Check if all members are reconnected
              const allReconnected = room.members.every(m => room._reconnectedUids.has(m.uid));
              if (allReconnected) {
                console.log(`[REJOIN] all members reconnected, resuming room=${roomId}`);
                room._waitingForMembers = false;
                // Process queued actions
                for (const qa of room._queuedActions || []) {
                  if (!room.ended) room.handleAction(qa.uid, qa.msg);
                }
                room._queuedActions = [];
                room.ticker = setInterval(() => room._tick(), TICK_MS);
              }
            } else {
              room.sendFullState(clientUid);
              console.log(`[REJOIN] sent full_state to uid=${clientUid} room=${roomId}`);
            }
          }
        } else { send(ws, { type:"no_active_combat" }); }
        break;
      }

      case "action": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const room = findRoomForUid(clientUid);
        if (room) {
          room.handleAction(clientUid, msg);
        } else if (redisPub) {
          // Combat room lives on another machine — forward the action there
          _redisPublish({ t: "combat_action", uid: clientUid, msg, _src: MY_MACHINE_ID });
        } else {
          send(ws, { type:"error", reason:"not_in_combat" });
        }
        break;
      }

      case "flee": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const room = findRoomForUid(clientUid);
        if (room) {
          room.handleFlee(clientUid);
        } else if (redisPub) {
          _redisPublish({ t: "combat_flee", uid: clientUid, _src: MY_MACHINE_ID });
        }
        break;
      }

      case "buy_item": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        if (_inArenaQueue(clientUid)) { send(ws, { type:"buy_fail", reason:"in_arena_queue" }); return; }
        const { itemId, qty: buyQty } = msg;
        const itemDef = ITEM_DB[itemId];
        if (!itemDef) { send(ws, { type:"buy_fail", reason:"unknown_item" }); return; }
        // Zone validation: item must be available in the player's current zone
        const buyerZoneId = clients.get(clientUid)?.zone || null;
        if (buyerZoneId && itemDef.category === "action") {
          // Actions don't use zones[] — validate against the zone's market.action list instead
          const buyerZone = ZONE_DB[buyerZoneId];
          const zoneActions = (buyerZone?.market?.action) || [];
          if (!zoneActions.includes(itemId)) {
            flagAnomaly(clientUid, "buy_wrong_zone", { itemId, playerZone: buyerZoneId, type: "action" });
            send(ws, { type:"buy_fail", reason:"not_available_here" }); return;
          }
        } else if (buyerZoneId && itemDef.zones && itemDef.zones.length > 0) {
          if (!itemDef.zones.includes(buyerZoneId)) {
            flagAnomaly(clientUid, "buy_wrong_zone", { itemId, playerZone: buyerZoneId, itemZones: itemDef.zones });
            send(ws, { type:"buy_fail", reason:"not_available_here" }); return;
          }
        }
        const cappedQty = Math.min(Math.max(Math.floor(buyQty || 1), 1), 99);
        const isEconBuy = itemDef.type === "material" || itemDef.type === "provision";
        // Stock check for player-economy items — must have enough in market stock
        let newStock = 0;
        if (isEconBuy) {
          const stockAvail = _getStock(itemId);
          if (stockAvail < cappedQty) { send(ws, { type:"buy_fail", reason:"out_of_stock" }); return; }
          newStock = stockAvail - cappedQty;
        }
        const cost = itemDef.cost * cappedQty;
        try {
          const save = _getCachedSave(clientUid);
          if (!save) { send(ws, { type:"buy_fail", reason:"no_save" }); return; }
          const p = save.player;
          _checkGold(clientUid, p?.gold || 0);
          if (!p || p.gold < cost) {
            flagAnomaly(clientUid, "buy_insufficient_gold", { itemId, gold: p?.gold, cost });
            send(ws, { type:"buy_fail", reason:"not_enough_gold" }); return;
          }
          // Capture pre-mutation snapshot string here — before any changes to save/p
          const buySnapshotData = JSON.stringify(save);
          const buyClient = clients.get(clientUid);
          const buyIp = buyClient?.ip || buyClient?.ws?._ip || "offline";
          const buyDeviceId = buyClient?.deviceId || "unknown";
          const buyFingerprint = buyClient?.fingerprint || "unknown";
          if (itemDef.category === "action") {
            const learned = p.learnedActions || [];
            if (learned.includes(itemId)) { send(ws, { type:"buy_fail", reason:"already_learned" }); return; }
            p.gold -= cost;
            p.learnedActions = [...learned, itemId];
          } else {
            // Uniqueness check for equipment — cannot own more than one of the same piece
            const isEquipment = itemDef.type === "gear" || itemDef.type === "accessory";
            if (isEquipment) {
              const inv0 = p.inventory || {};
              const eq0 = p.equipment || {};
              const key0 = getInvKey(itemDef);
              const alreadyInBag = (inv0[key0] || []).some(i => i.id === itemId);
              const alreadyEquipped = (eq0.gear?.id === itemId) || ((eq0.accessories || []).some(a => a?.id === itemId));
              if (alreadyInBag || alreadyEquipped) {
                send(ws, { type:"buy_fail", reason:"already_owned" }); return;
              }
            }
            p.gold -= cost;
            const inv = p.inventory || {};
            const key = getInvKey(itemDef);
            const arr = inv[key] || [];
            if (itemDef.type === "provision" || itemDef.type === "material") {
              const idx = arr.findIndex(i => i.id === itemId);
              if (idx >= 0) arr[idx] = { ...arr[idx], qty: (arr[idx].qty || 1) + cappedQty };
              else arr.push({ id: itemDef.id, name: itemDef.name, rarity: itemDef.rarity || "common", type: itemDef.type, marketValue: itemDef.marketValue || 0, qty: cappedQty });
            } else {
              for (let i = 0; i < cappedQty; i++) arr.push({ id: itemDef.id, name: itemDef.name, rarity: itemDef.rarity || "common", type: itemDef.type, marketValue: itemDef.marketValue || 0 });
            }
            inv[key] = arr;
            p.inventory = inv;
          }
          // Wrap all DB writes in a single transaction — snapshot, save, and tx log
          // are either all committed or all rolled back together.
          const buyReason = itemDef.category === "action" ? "buy_action" : "buy_item";
          db.transaction(() => {
            stmt.insertSnapshot.run(clientUid, buyReason, buySnapshotData, buyIp, buyDeviceId, buyFingerprint, Date.now());
            stmt.trimSnapshotsForUser.run(clientUid, clientUid);
            stmt.upsertSave.run(clientUid, JSON.stringify(save));
            stmt.insertTxLog.run(clientUid, "buy", JSON.stringify({ itemId, qty: cappedQty, cost, goldAfter: p.gold }), Date.now());
          })();
          _saveCacheSet(clientUid, save);
          if (clients.has(clientUid)) _addItemOwner(clientUid, itemId); // targeted add — avoids full reseed on every buy
          // Update market stock in-memory and broadcast after DB commit
          if (isEconBuy) {
            _setStock(itemId, newStock);
            _broadcastStockUpdate(itemId, newStock);
            _logTx(clientUid, "stock_deduct", { itemId, qty: cappedQty, stockAfter: newStock });
          }
          _setExpectedGold(clientUid, p.gold);
          // Send only changed fields — action buys skip inventory, item buys send only changed invKey
          if (itemDef.category === "action") {
            send(ws, { type:"buy_ok", itemId, gold: p.gold, learnedActions: p.learnedActions });
          } else {
            const changedKey = getInvKey(itemDef);
            send(ws, { type:"buy_ok", itemId, gold: p.gold, invKey: changedKey, items: p.inventory[changedKey] });
          }
          console.log(`[BUY] uid=${clientUid} item=${itemId} cost=${cost} gold=${p.gold}`);
        } catch (e) {
          console.error("[BUY] error:", e.message);
          send(ws, { type:"buy_fail", reason:"server_error" });
        }
        break;
      }

      case "sell_item": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        if (_inArenaQueue(clientUid)) { send(ws, { type:"sell_fail", reason:"in_arena_queue" }); return; }
        const { itemId: sellItemId, invKey: sellKey, qty: sellQty } = msg;
        if (!sellItemId || !sellKey) { send(ws, { type:"sell_fail", reason:"bad_request" }); return; }
        // Equipment, actions/learned abilities, and crafting recipes cannot be sold or deleted
        const _noSellKeys = new Set(["gears","accessories"]);
        if (_noSellKeys.has(sellKey)) { send(ws, { type:"sell_fail", reason:"cannot_sell_equipment" }); return; }
        try {
          const save = _getCachedSave(clientUid);
          if (!save) { send(ws, { type:"sell_fail", reason:"no_save" }); return; }
          const p = save.player;
          _checkGold(clientUid, p?.gold || 0);
          const inv = p.inventory || {};
          const arr = inv[sellKey] || [];
          const idx = arr.findIndex(i => i.id === sellItemId);
          if (idx < 0) { flagAnomaly(clientUid, "sell_item_not_found", { sellItemId, sellKey }); send(ws, { type:"sell_fail", reason:"item_not_found" }); return; }
          const item = arr[idx];
          const isEconItem = item.type === 'material' || item.type === 'provision';
          const mv = item.marketValue != null ? item.marketValue : Math.floor((item.cost || 0) * 0.75);
          const stockNow = _getStock(sellItemId);
          const base = Math.floor(mv * 0.75);
          // Dynamic pricing tiers (materials/provisions only):
          //   stock < 1000  → +25% bonus  (93.75% of MV) — low supply
          //   stock < 10000 → standard    (75% of MV)
          //   stock >= 10000 → -25% penalty (56.25% of MV) — market flooded
          let sv = base;
          if (isEconItem) {
            if (stockNow < 1000)        sv = Math.floor(base * 1.25);
            else if (stockNow >= 10000) sv = Math.floor(base * 0.75);
          }
          const sq = Math.min(Math.max(Math.floor(sellQty || 1), 1), 99);
          if (item.qty != null && item.qty < sq) { send(ws, { type:"sell_fail", reason:"not_enough_qty" }); return; }
          const gold = sv * sq;
          const eq = p.equipment || {};
          if ((eq.gear && eq.gear.id === sellItemId) || ((eq.accessories || []).some(a => a && a.id === sellItemId))) {
            send(ws, { type:"sell_fail", reason:"equipped" }); return;
          }
          // Capture pre-mutation snapshot string before applying changes
          const sellSnapshotData = JSON.stringify(save);
          if (item.qty != null) { if (item.qty <= sq) arr.splice(idx, 1); else arr[idx] = { ...item, qty: item.qty - sq }; }
          else arr.splice(idx, 1);
          inv[sellKey] = arr; p.inventory = inv;
          p.gold = (p.gold || 0) + gold;
          p.stats = p.stats || {}; p.stats.goldFromSelling = (p.stats.goldFromSelling || 0) + gold;
          // Wrap all DB writes in a single transaction — snapshot, save, and tx log
          // are either all committed or all rolled back together.
          const sellClient = clients.get(clientUid);
          const sellIp = sellClient?.ip || sellClient?.ws?._ip || "offline";
          const sellDeviceId = sellClient?.deviceId || "unknown";
          const sellFingerprint = sellClient?.fingerprint || "unknown";
          db.transaction(() => {
            stmt.insertSnapshot.run(clientUid, "sell_item", sellSnapshotData, sellIp, sellDeviceId, sellFingerprint, Date.now());
            stmt.trimSnapshotsForUser.run(clientUid, clientUid);
            stmt.upsertSave.run(clientUid, JSON.stringify(save));
            stmt.insertTxLog.run(clientUid, "sell", JSON.stringify({ itemId: sellItemId, qty: sq, earned: gold, goldAfter: p.gold, marketValue: mv, bonusApplied: isEconItem && stockNow < 1000, penaltyApplied: isEconItem && stockNow >= 10000 }), Date.now());
          })();
          _saveCacheSet(clientUid, save);
          // Targeted removal: only evict from ownership index when the item is fully gone
          if (clients.has(clientUid)) {
            const stillOwned = (p.inventory[sellKey] || []).some(i => i.id === sellItemId);
            if (!stillOwned) _removeItemOwner(clientUid, sellItemId);
          }
          let newStock = stockNow;
          if (isEconItem) { newStock = stockNow + sq; _setStock(sellItemId, newStock); _broadcastStockUpdate(sellItemId, newStock); _logTx(clientUid, "stock_add", { itemId: sellItemId, qty: sq, stockAfter: newStock }); }
          _setExpectedGold(clientUid, p.gold);
          const stockUpdate = isEconItem ? { itemId: sellItemId, qty: newStock } : undefined;
          send(ws, { type:"sell_ok", itemId: sellItemId, gold: p.gold, earnedGold: gold, invKey: sellKey, items: p.inventory[sellKey], stockUpdate });
          console.log(`[SELL] uid=${clientUid} item=${sellItemId} qty=${sq} mv=${mv} earned=${gold} stock=${newStock} gold=${p.gold}`);
        } catch (e) { console.error("[SELL] error:", e.message); send(ws, { type:"sell_fail", reason:"server_error" }); }
        break;
      }

      case "delete_item": {
        // Discard Common/Uncommon/Rare equipment — no gold returned, item is gone
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        const { itemId: delItemId, invKey: delKey } = msg;
        if (!delItemId || !delKey) { send(ws, { type:"delete_fail", reason:"bad_request" }); return; }
        // Only equipment keys allowed for deletion
        const _deletableKeys = new Set(["gears","accessories"]);
        if (!_deletableKeys.has(delKey)) { send(ws, { type:"delete_fail", reason:"not_deletable" }); return; }
        try {
          const save = _getCachedSave(clientUid);
          if (!save) { send(ws, { type:"delete_fail", reason:"no_save" }); return; }
          const p = save.player;
          const eq = p.equipment || {};
          // Cannot delete while equipped
          if ((eq.gear?.id === delItemId) || ((eq.accessories||[]).some(a => a?.id === delItemId))) {
            send(ws, { type:"delete_fail", reason:"equipped" }); return;
          }
          const inv = p.inventory || {};
          const arr = inv[delKey] || [];
          const idx = arr.findIndex(i => i.id === delItemId);
          if (idx < 0) { send(ws, { type:"delete_fail", reason:"item_not_found" }); return; }
          const item = arr[idx];
          // Only Common/Uncommon/Rare may be deleted
          const deletableRarities = new Set(["common","uncommon","rare"]);
          if (!deletableRarities.has((item.rarity||"common").toLowerCase())) {
            send(ws, { type:"delete_fail", reason:"cannot_delete_rarity" }); return;
          }
          _snapshotSave(clientUid, "delete_item");
          arr.splice(idx, 1);
          inv[delKey] = arr; p.inventory = inv;
          _writeSave(clientUid, save);
          _logTx(clientUid, "delete", { itemId: delItemId, invKey: delKey });
          console.log(`[DELETE] uid=${clientUid} item=${delItemId}`);
          send(ws, { type:"delete_ok", itemId: delItemId, invKey: delKey, items: p.inventory[delKey] });
        } catch(e) { console.error("[DELETE] error:", e.message); send(ws, { type:"delete_fail", reason:"server_error" }); }
        break;
      }

      case "get_market_stock": {
        // Mark this client as having the market open — they'll receive targeted stock broadcasts
        if (clientUid) { const c = clients.get(clientUid); if (c) c.inMarket = true; }
        const stockObj = {};
        for (const [k, v] of MARKET_STOCK.entries()) stockObj[k] = v;
        send(ws, { type: "market_stock", stock: stockObj });
        break;
      }

      case "market_close": {
        // Client closed the market — stop sending them stock broadcasts
        if (clientUid) { const c = clients.get(clientUid); if (c) c.inMarket = false; }
        break;
      }

      case "use_provision": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        if (_inArenaQueue(clientUid)) { send(ws, { type:"provision_fail", reason:"in_arena_queue" }); return; }
        const { itemId: provItemId } = msg;
        try {
          const save = _getCachedSave(clientUid);
          if (!save) { send(ws, { type:"provision_fail", reason:"no_save" }); return; }
          const p = save.player;
          const provs = (p.inventory || {}).provisions || [];
          const idx = provs.findIndex(i => i.id === provItemId);
          if (idx < 0) { flagAnomaly(clientUid, "provision_not_found", { provItemId }); send(ws, { type:"provision_fail", reason:"not_found" }); return; }
          const item = provs[idx];
          const provLiveDef = ITEM_DB[provItemId];
          const provHealHp = provLiveDef ? (provLiveDef.healHp || 0) : (item.healHp || 0);
          // Check energy + cooldown via combat room if in combat
          const provRoom = findRoomForUid(clientUid);
          if (provRoom && !provRoom.ended) {
            const provMember = provRoom.members.find(m => m.uid === clientUid);
            if (provMember) {
              if (provMember.energy < ENERGY_TO_PLAYER) { send(ws, { type:"provision_fail", reason:"not_enough_energy" }); return; }
              const provCd = provMember.cooldowns["provisions"];
              if (provCd && Date.now() < provCd) { send(ws, { type:"provision_fail", reason:"on_cooldown" }); return; }
            }
          }
          if (item.qty > 1) provs[idx] = { ...item, qty: item.qty - 1 }; else provs.splice(idx, 1);
          p.inventory.provisions = provs;
          const hpBefore = p.hp || 0;
          // ── If in combat: resolve the effective heal now (before updating save)
          // so both the save and the room member use the same final value. ─────
          const provRoom2 = findRoomForUid(clientUid);
          const provMember2 = provRoom2 && !provRoom2.ended ? provRoom2.members.find(m => m.uid === clientUid) : null;
          let _provEffectiveHeal = provHealHp;
          let _provMaxHpMod = 0;
          if (provMember2) {
            _provMaxHpMod = _getStatMod(provRoom2, provMember2.uid, "maxHp");
            const _provHealMod = _getStatMod(provRoom2, provMember2.uid, "healReceived") + _getStatMod(provRoom2, provMember2.uid, "heal");
            _provEffectiveHeal = provHealHp + _provHealMod;
          }
          const _effectiveMaxHp = Math.max(1, (p.maxHp || 100) + _provMaxHpMod);
          if (provHealHp) p.hp = Math.max(0, Math.min(_effectiveMaxHp, hpBefore + _provEffectiveHeal));
          _writeSave(clientUid, save);
          send(ws, { type: "provision_ok", itemId: provItemId, hp: p.hp, provisions: p.inventory.provisions });
          // ── Update room member HP to match save exactly ───────────────────────
          if (provRoom2 && !provRoom2.ended && provMember2) {
            const healAmt = p.hp - hpBefore; // actual HP delta, after clamping to max
            provMember2.hp = p.hp; // sync directly from save — single source of truth
            provMember2.energy = Math.max(0, provMember2.energy - ENERGY_TO_PLAYER);
            provMember2.cooldowns["provisions"] = Date.now() + (ACTION_DB.provisions.cooldown || 0);
            provMember2.inventory = p.inventory;
            const newLogs = []; const newEvents = [];
            const _log = (tp, tx, au, vu) => { const e = provRoom2.seqLog(tp, tx, au, vu); if (e !== null) newLogs.push(e); };
            const _event = (ev) => { const e = provRoom2.seqEvent(ev); newEvents.push(e); };
            _fireAmbient(provRoom2, provMember2, "on_energy_spent", _log, _event);
            _fireAmbient(provRoom2, provMember2, "on_energy_threshold", _log, _event, null, { metricValue: provMember2.energy });
            _log("pm", `${provMember2.name} uses ${item.name}.${healAmt > 0 ? ` +${healAmt} HP.` : ""}`, provMember2.uid);
            if (healAmt > 0) _event({ k:"player_strike", au:provMember2.uid, vu:provMember2.uid, d:healAmt, h:1, an:item.name, ef:"heal" });
            const provDef = ITEM_DB[provItemId];
            if (provDef && Array.isArray(provDef._effects) && provDef._effects.length > 0) {
              _evalEffects(provRoom2, provMember2, provMember2, "on_use", { ability: provDef, actionId: "provisions", wasHit: true }, _log, _event);
            }
            // Equipment on_use effects (e.g. gear that procs when any item is consumed)
            _evalEquipEffects(provRoom2, provMember2, provMember2, "on_use", _log, _event);
            _fireAmbient(provRoom2, provMember2, "on_ability_use", _log, _event);
            const _dm = provRoom2.members.map(m => provRoom2._deltaMember(m)).filter(Boolean);
            const _pkt = { type:"tick" };
            if (_dm.length > 0) _pkt.members = _dm;
            if (newLogs.length > 0) _pkt.logs = newLogs;
            if (newEvents.length > 0) _pkt.events = newEvents;
            provRoom2.broadcastTick(_pkt);
          }
          if (save.partyId) _broadcastPartyHpPatch(save.partyId, clientUid, p.hp, p.maxHp);
          _logTx(clientUid, "use_provision", { itemId: provItemId, hpAfter: p.hp });
          console.log(`[PROVISION] uid=${clientUid} used=${provItemId} hp=${p.hp}`);
        } catch (e) { console.error("[PROVISION] error:", e.message); send(ws, { type:"provision_fail", reason:"server_error" }); }
        break;
      }

      case "equip_item": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        if (_inArenaQueue(clientUid)) { send(ws, { type:"equip_fail", reason:"in_arena_queue" }); return; }
        const { slot: eqSlot, itemId: eqItemId } = msg;
        try {
          const save = _getCachedSave(clientUid);
          if (!save) { send(ws, { type:"equip_fail", reason:"no_save" }); return; }
          const p = save.player;
          const eq = p.equipment || { gear: null, accessories: [null, null, null] };
          if (!eq.accessories) eq.accessories = [null, null, null];
          const inv = p.inventory || {};
          let prevItem = null;
          if (eqSlot === "gear") prevItem = eq.gear;
          else if (eqSlot.startsWith("acc_")) { const ai = parseInt(eqSlot.split("_")[1]); prevItem = eq.accessories[ai] || null; }
          // Capture pre-mutation snapshot string before any changes to save/p
          const eqSnapshotData = JSON.stringify(save);
          const eqClient = clients.get(clientUid);
          const eqIp = eqClient?.ip || eqClient?.ws?._ip || "offline";
          const eqDeviceId = eqClient?.deviceId || "unknown";
          const eqFingerprint = eqClient?.fingerprint || "unknown";
          const eqReason = eqItemId === null ? "unequip" : "equip";
          if (eqItemId === null) {
            if (eqSlot === "gear") eq.gear = null;
            else if (eqSlot.startsWith("acc_")) eq.accessories[parseInt(eqSlot.split("_")[1])] = null;
            else if (eqSlot.startsWith("acc_")) eq.accessories[parseInt(eqSlot.split("_")[1])] = null;
          } else {
            let invKey = eqSlot === "gear" ? "gears" : "accessories";
            const arr = inv[invKey] || [];
            const idx = arr.findIndex(i => i.id === eqItemId);
            if (idx < 0) { flagAnomaly(clientUid, "equip_item_not_found", { eqItemId, eqSlot }); send(ws, { type:"equip_fail", reason:"item_not_found" }); return; }
            const item = arr[idx];
            arr.splice(idx, 1); inv[invKey] = arr;
            if (prevItem) { const prevArr = inv[invKey] || []; if (!prevArr.some(i => i.id === prevItem.id)) prevArr.push(prevItem); inv[invKey] = prevArr; }
            if (eqSlot === "gear") eq.gear = item;
            else if (eqSlot.startsWith("acc_")) eq.accessories[parseInt(eqSlot.split("_")[1])] = item;
          }
          p.equipment = eq; p.inventory = inv;
          // ── Gear-type change: purge locked actions ──────────────────────────
          // Only fires when a gear slot changes and the gear TYPE differs.
          // Core actions (no requiresGearType) are never removed.
          if (eqSlot === "gear") {
            const oldType = prevItem?.gearType || null;
            const newType = eq.gear?.gearType || null;
            if (oldType && oldType !== newType) {
              // Remove learned actions locked to the OLD gear type
              const before = p.learnedActions || [];
              p.learnedActions = before.filter(aId => {
                const def = ITEM_DB[aId];
                return !def || def.requiresGearType !== oldType;
              });
              // Clear those same actions from slots (null them out, preserve slot positions)
              p.actionSlots = (p.actionSlots || []).map(aId => {
                if (!aId) return null;
                const def = ITEM_DB[aId];
                return (def && def.requiresGearType === oldType) ? null : aId;
              });
            }
          }
          const _liveMaxHp = (slot) => { if (!slot) return 0; const d = ITEM_DB[slot.id]; return d ? (d.maxHp || 0) : (slot.maxHp || 0); };
          const equipmentMhp = _liveMaxHp(eq.gear) + (eq.accessories || []).reduce((s, a) => s + _liveMaxHp(a), 0);
          p.maxHp = (p.baseMaxHp || 100) + equipmentMhp;
          p.hp = Math.min(p.hp != null ? p.hp : p.maxHp, p.maxHp);
          // Wrap all DB writes in a single transaction — snapshot, save, and tx log
          // are either all committed or all rolled back together.
          db.transaction(() => {
            stmt.insertSnapshot.run(clientUid, eqReason, eqSnapshotData, eqIp, eqDeviceId, eqFingerprint, Date.now());
            stmt.trimSnapshotsForUser.run(clientUid, clientUid);
            stmt.upsertSave.run(clientUid, JSON.stringify(save));
            stmt.insertTxLog.run(clientUid, eqItemId ? "equip" : "unequip", JSON.stringify({ slot: eqSlot, itemId: eqItemId, prevItem: prevItem?.id || null }), Date.now());
          })();
          _saveCacheSet(clientUid, save);
          // No ownership index update needed for equip/unequip — the item stays owned,
          // it just moves between the equipped slot and inventory.
          send(ws, { type: "equip_ok", slot: eqSlot, equipment: p.equipment, inventory: p.inventory, hp: p.hp, maxHp: p.maxHp, actionSlots: p.actionSlots, learnedActions: p.learnedActions });
          if (save.partyId) _broadcastPartyHpPatch(save.partyId, clientUid, p.hp, p.maxHp);
          console.log(`[EQUIP] uid=${clientUid} slot=${eqSlot} item=${eqItemId||"unequip"}`);
        } catch (e) { console.error("[EQUIP] error:", e.message); send(ws, { type:"equip_fail", reason:"server_error" }); }
        break;
      }

      case "craft_item": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        if (_inArenaQueue(clientUid)) { send(ws, { type:"craft_fail", reason:"in_arena_queue" }); return; }
        const { recipeId: craftRecipeId } = msg;
        try {
          const save = _getCachedSave(clientUid);
          if (!save) { send(ws, { type:"craft_fail", reason:"no_save" }); return; }
          const p = save.player;
          const recipes = p.learnedRecipes || [];
          const recipe = recipes.find(r => r.id === craftRecipeId);
          if (!recipe) { flagAnomaly(clientUid, "craft_unknown_recipe", { craftRecipeId }); send(ws, { type:"craft_fail", reason:"unknown_recipe" }); return; }
          const mats = p.inventory?.materials || [];
          for (const req of recipe.ingredients) {
            const have = mats.find(m => m.id === req.id);
            if (!have || have.qty < req.qty) { send(ws, { type:"craft_fail", reason:"missing_ingredients" }); return; }
          }
          let newMats = [...mats];
          // Capture pre-mutation snapshot string before ingredient consumption
          const craftSnapshotData = JSON.stringify(save);
          const craftClient = clients.get(clientUid);
          const craftIp = craftClient?.ip || craftClient?.ws?._ip || "offline";
          const craftDeviceId = craftClient?.deviceId || "unknown";
          const craftFingerprint = craftClient?.fingerprint || "unknown";
          for (const req of recipe.ingredients) {
            const idx = newMats.findIndex(m => m.id === req.id);
            if (idx >= 0) newMats[idx] = { ...newMats[idx], qty: newMats[idx].qty - req.qty };
          }
          newMats = newMats.filter(m => m.qty > 0);
          const result = recipe.result;
          const key = result.type === "provision" ? "provisions" : result.type === "gear" ? "gears" : "accessories";
          const inv = p.inventory || {};
          inv.materials = newMats;
          const existArr = [...(inv[key] || [])];
          const existIdx = existArr.findIndex(i => i.id === result.id);
          if (existIdx >= 0 && result.type === "provision") existArr[existIdx] = { ...existArr[existIdx], qty: (existArr[existIdx].qty || 1) + 1 };
          else existArr.push({ ...result, qty: 1 });
          inv[key] = existArr; p.inventory = inv;
          // Wrap all DB writes in a single transaction — snapshot, save, and tx log
          // are either all committed or all rolled back together.
          db.transaction(() => {
            stmt.insertSnapshot.run(clientUid, "craft", craftSnapshotData, craftIp, craftDeviceId, craftFingerprint, Date.now());
            stmt.trimSnapshotsForUser.run(clientUid, clientUid);
            stmt.upsertSave.run(clientUid, JSON.stringify(save));
            stmt.insertTxLog.run(clientUid, "craft", JSON.stringify({ recipeId: craftRecipeId, resultId: result.id }), Date.now());
          })();
          _saveCacheSet(clientUid, save);
          // Targeted ownership update: add the crafted item, remove any materials fully consumed
          if (clients.has(clientUid)) {
            _addItemOwner(clientUid, result.id);
            for (const req of recipe.ingredients) {
              const stillOwned = newMats.some(m => m.id === req.id);
              if (!stillOwned) _removeItemOwner(clientUid, req.id);
            }
          }
          // Send only changed inventory keys (materials consumed + result key) — much smaller than full inventory
          send(ws, { type: "craft_ok", recipeId: craftRecipeId,
            changedMaterials: newMats, resultKey: key, resultItems: inv[key] });
          console.log(`[CRAFT] uid=${clientUid} recipe=${craftRecipeId}`);
        } catch (e) { console.error("[CRAFT] error:", e.message); send(ws, { type:"craft_fail", reason:"server_error" }); }
        break;
      }

      // ── Fish start: server picks the fish and begins the authoritative session ──
      // The minigame itself (bite delay + reel) is now fully simulated server-side.
      // The client never reports a catch; it only reports hold/release state via
      // fish_input, and renders whatever the server's fish_state/fish_resolved tells it.
      case "fish_start": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }

        const now0 = Date.now();
        const lastCatch = _fishCooldowns.get(clientUid) || 0;
        if (now0 - lastCatch < FISH_COOLDOWN_MS) {
          send(ws, { type:"fish_fail", reason:"cooldown" }); return;
        }
        if (_fishReelSessions.has(clientUid)) {
          send(ws, { type:"fish_fail", reason:"already_fishing" }); return;
        }

        // Resolve which fishing subzone the player is in
        const fsZoneId  = msg.zoneId    || clients.get(clientUid)?.zone || null;
        const fsSubzoneId = msg.subzoneId || null; // subzone name or id sent by client
        const fsZone = fsZoneId ? ZONE_DB[fsZoneId] : null;

        let assignedFish = null;

        if (fsZone && Array.isArray(fsZone.subzones)) {
          // Find the fishing subzone — match by name/id if provided, else first Fishing subzone
          const fishingSZ = fsSubzoneId
            ? fsZone.subzones.find(sz => sz.type === 'Fishing' && (sz.name === fsSubzoneId || sz.id === fsSubzoneId))
            : fsZone.subzones.find(sz => sz.type === 'Fishing');

          if (fishingSZ && fishingSZ._fishing) {
            assignedFish = _rollFishFromSubzone(fishingSZ);
          }
        }

        // Fallback: legacy hardcoded roll (for goblin_forest and zones without subzone data)
        if (!assignedFish) assignedFish = _rollFishLegacy();

        // Validate the fish actually exists (live items or legacy DB)
        let fishDef = _getFishDef(assignedFish);
        if (!fishDef) { assignedFish = _rollFishLegacy(); fishDef = _getFishDef(assignedFish); }
        if (!fishDef) { send(ws, { type:"fish_fail", reason:"no_fish_available" }); return; }

        send(ws, { type:"fish_assigned", fishId: assignedFish });
        _fishStartSession(clientUid, ws, assignedFish, fishDef);
        break;
      }

      // ── Fish input: client reports hold/release state only — never a result.
      case "fish_input": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "fish_input")) return; // silently drop excess spam, no need to alarm the player
        const s = _fishReelSessions.get(clientUid);
        if (!s || s.state !== 'reeling') return; // no active reel session — ignore
        s.pressing = !!msg.pressing;
        break;
      }

      // ── Fish cancel: player left the fishing UI mid-session — tear down the
      // server-side simulation so its tick loop doesn't keep running forever.
      case "fish_cancel": {
        if (!clientUid) return;
        _fishClearSession(clientUid);
        break;
      }

      case "travel": {
        if (!clientUid) return;
        if (_inArenaQueue(clientUid)) { send(ws, { type:"travel_denied", reason:"in_arena_queue" }); return; }
        const cdResult = _cooldownOk(clientUid, "travel");
        if (!cdResult.ok) {
          flagAnomaly(clientUid, "travel_cooldown_bypass", { remaining: cdResult.remaining });
          send(ws, { type:"travel_denied", remaining: cdResult.remaining });
        } else {
          // Update server-side zone on successful travel
          const travelZone = msg.zoneId || msg.zone || null;
          if (travelZone && ZONE_DB[travelZone]) {
            _setClientZone(clientUid, travelZone);
          }
          send(ws, { type:"travel_ok" });
        }
        break;
      }

      case "arena_join_queue": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        if (findRoomForUid(clientUid)) { send(ws, { type:"error", reason:"in_combat" }); return; }
        const arenaMode = msg.mode || "1v1";
        if (arenaMode !== "1v1") { send(ws, { type:"error", reason:"invalid_mode" }); return; }
        const queue = arenaQueues[arenaMode];
        if (queue.some(q => q.uid === clientUid)) { send(ws, { type:"arena_queue_joined", mode: arenaMode }); return; }
        try {
          const save = _getCachedSave(clientUid);
          if (!save) { send(ws, { type:"error", reason:"no_save" }); return; }
          // Clear stale partyId if the party no longer exists
          if (save.partyId) {
            const partyStillExists = dbGetParty(save.partyId);
            if (!partyStillExists) {
              save.partyId = null;
              _writeSave(clientUid, save);
            }
          }
          if (save.partyId) { send(ws, { type:"error", reason:"in_party" }); return; }
          const p = save.player;
          stmt.deleteInboxCat.run(clientUid, "invites");
          p.stats = p.stats || {};
          p.stats.arena = p.stats.arena || {};
          const _soloRating = p.stats.arena[`rating${arenaMode}`] ?? 1000;
          queue.push({ uid: clientUid, ws, name: p.name, save, rating: _soloRating, queuedAt: Date.now() });
          _arenaQueueUids.add(clientUid);
          send(ws, { type:"arena_queue_joined", mode: arenaMode });
          console.log(`[ARENA] ${p.name} (${clientUid}) joined ${arenaMode} queue. Queue size: ${queue.length}`);
          _tryArenaMatch(arenaMode);
        } catch (e) { console.error("[ARENA] join error:", e.message); }
        break;
      }

      case "arena_leave_queue": {
        if (!clientUid) return;
        // Solo leave — only removes from 1v1 queue (team modes use arena_party_leave_queue)
        arenaQueues["1v1"] = arenaQueues["1v1"].filter(q => q.uid !== clientUid);
        _arenaQueueUids.delete(clientUid);
        send(ws, { type:"arena_queue_left" });
        console.log(`[ARENA] ${clientUid} left queue`);
        break;
      }

      // ── Party arena queue (2v2 / 4v4) — leader only, queues the whole team ──
      case "arena_party_queue_request": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        const apqMode = msg.mode;
        if (apqMode !== "2v2" && apqMode !== "4v4") { send(ws, { type:"error", reason:"invalid_mode" }); return; }
        const apqRequiredSize = apqMode === "2v2" ? 2 : 4;
        // Must be a party leader
        const apqPartyId = partySubscriptions.get(clientUid);
        if (!apqPartyId) { send(ws, { type:"arena_party_queue_fail", reason:"not_in_party" }); return; }
        const apqParty = dbGetParty(apqPartyId);
        if (!apqParty) { send(ws, { type:"arena_party_queue_fail", reason:"party_not_found" }); return; }
        const apqClient = clients.get(clientUid);
        if (!apqClient || apqParty.leader !== apqClient.username) { send(ws, { type:"arena_party_queue_fail", reason:"not_leader" }); return; }
        const apqMembers = (apqParty.members || []).filter(m => m.uid);
        if (apqMembers.length !== apqRequiredSize) {
          send(ws, { type:"arena_party_queue_fail", reason:"wrong_party_size", required: apqRequiredSize, actual: apqMembers.length });
          return;
        }
        // Check all members are online, not in combat, not in queue
        for (const m of apqMembers) {
          if (findRoomForUid(m.uid)) { send(ws, { type:"arena_party_queue_fail", reason:"member_in_combat", name: m.charName || m.username }); return; }
          if (_inArenaQueue(m.uid)) { send(ws, { type:"arena_party_queue_fail", reason:"member_in_queue", name: m.charName || m.username }); return; }
          const mc = clients.get(m.uid);
          if (!mc || mc.ws.readyState !== 1) { send(ws, { type:"arena_party_queue_fail", reason:"member_offline", name: m.charName || m.username }); return; }
        }
        // Build team entry — one slot per member uid, all share the same teamId
        const apqTeamId = `team_${apqPartyId}_${Date.now()}`;
        const apqTeamMembers = [];
        for (const m of apqMembers) {
          const mSave = _getCachedSave(m.uid);
          if (!mSave) { send(ws, { type:"arena_party_queue_fail", reason:"member_save_missing", name: m.charName || m.username }); return; }
          const mc = clients.get(m.uid);
          apqTeamMembers.push({ uid: m.uid, ws: mc.ws, name: mSave.player?.name || m.username, save: mSave });
        }
        const apqQueue = arenaQueues[apqMode];
        // Compute total party rating for matchmaking
        const apqTotalRating = apqTeamMembers.reduce((sum, m) => {
          const mStats = m.save?.player?.stats?.arena || {};
          return sum + (mStats[`rating${apqMode}`] ?? 1000);
        }, 0);
        // Build team slot — all members share a teamId
        const apqTeamEntry = { teamId: apqTeamId, partyId: apqPartyId, members: apqTeamMembers, mode: apqMode, rating: apqTotalRating, queuedAt: Date.now() };
        apqQueue.push(apqTeamEntry);
        for (const m of apqTeamMembers) _arenaQueueUids.add(m.uid);
        // Notify all team members
        for (const m of apqTeamMembers) {
          const mc = clients.get(m.uid);
          if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_queue_joined", mode: apqMode, team: true });
        }
        console.log(`[ARENA] Party ${apqPartyId} joined ${apqMode} queue as team ${apqTeamId}. Queue size: ${apqQueue.length} teams`);
        _tryArenaMatch(apqMode);
        break;
      }

      // ── Party arena leave queue — removes the whole team ──
      case "arena_party_leave_queue": {
        if (!clientUid) return;
        const aplqPartyId = partySubscriptions.get(clientUid);
        if (!aplqPartyId) break;
        const aplqParty = dbGetParty(aplqPartyId);
        if (!aplqParty) break;
        const aplqClient = clients.get(clientUid);
        if (!aplqClient || aplqParty.leader !== aplqClient.username) break; // only leader can remove team
        for (const mode of ["2v2","4v4"]) {
          const before = arenaQueues[mode];
          const after = [];
          for (const entry of before) {
            if (entry.partyId === aplqPartyId) {
              // Remove all member uids from the set and notify them
              for (const m of entry.members) {
                _arenaQueueUids.delete(m.uid);
                const mc = clients.get(m.uid);
                if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_queue_left", mode });
              }
            } else { after.push(entry); }
          }
          arenaQueues[mode] = after;
        }
        console.log(`[ARENA] Party ${aplqPartyId} left the team queue`);
        break;
      }

      // ── Arena match confirmation response ──────────────────────────────────
      case "arena_accept_match": {
        if (!clientUid) return;
        const pm = _pendingMatches.get(msg.matchId);
        if (!pm || !pm.allUids.includes(clientUid)) return;
        if (pm.accepted.has(clientUid) || pm.declined.has(clientUid)) return;
        pm.accepted.add(clientUid);
        console.log(`[ARENA] ${clientUid} accepted match ${msg.matchId} (${pm.accepted.size}/${pm.allUids.length})`);
        // Broadcast acceptance count to all participants so the UI can update
        for (const uid of pm.allUids) {
          const mc = clients.get(uid);
          if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_confirm_status", matchId: msg.matchId, accepted: pm.accepted.size, total: pm.allUids.length });
        }
        // If everyone accepted, start the match
        if (pm.accepted.size === pm.allUids.length) {
          clearTimeout(pm.timer);
          _pendingMatches.delete(msg.matchId);
          _startPendingMatch(pm).catch(e => console.error("[ARENA] _startPendingMatch error:", e.message));
        }
        return;
      }

      case "arena_decline_match": {
        if (!clientUid) return;
        const pm = _pendingMatches.get(msg.matchId);
        if (!pm || !pm.allUids.includes(clientUid)) return;
        if (pm.accepted.has(clientUid) || pm.declined.has(clientUid)) return;
        pm.declined.add(clientUid);
        clearTimeout(pm.timer);
        _pendingMatches.delete(msg.matchId);
        console.log(`[ARENA] ${clientUid} declined match ${msg.matchId} — cancelling`);
        const acceptedUids = [...pm.accepted];
        const declinedUids = [...pm.declined];
        _cancelPendingMatch(pm, acceptedUids, declinedUids, "declined");
        return;
      }

      case "safe_heal": {
        if (!clientUid) return;
        try {
          // Verify player is actually in a safe zone.
          // Prefer client.zone (set by zone_update); fall back to persisted lastZone
          // in case zone_update was deduped or hasn't arrived yet this session.
          const client = clients.get(clientUid);
          const save = _getCachedSave(clientUid);
          const savedZone = save && save.player && save.player.lastZone ? save.player.lastZone : null;
          const playerZone = (client && client.zone) || savedZone;
          const zoneData = playerZone ? ZONE_DB[playerZone] : null;
          if (!zoneData || !zoneData.safe) {
            flagAnomaly(clientUid, "safe_heal_in_hostile", { zone: playerZone });
            return;
          }
          if (!save || !save.player) return;
          const p = save.player;
          const maxHp = p.maxHp || 100;
          if ((p.hp || 0) < maxHp) {
            p.hp = maxHp;
            _writeSave(clientUid, save, { skipOwnerSeed: true }); // hp only
            // Broadcast HP change to party members via lightweight patch
            const partyId = save.partyId || null;
            if (partyId) _broadcastPartyHpPatch(partyId, clientUid, maxHp, maxHp);
          }
        } catch (e) { console.error("[SAFE_HEAL] error:", e.message); }
        break;
      }

      case "zone_update": {
        if (!clientUid) return;
        const client = clients.get(clientUid);
        const zuZone = msg.zone || null;
        const validZone = (zuZone && ZONE_DB[zuZone]) ? zuZone : null;
        const zoneChanged = client && client.zone !== validZone;
        if (client) _setClientZone(clientUid, validZone);
        if (validZone && client) {
          // Always persist to accounts.zone — this is the authoritative last-zone record
          // used by _sendOnlineFriends to show friend locations. Do not gate on zoneChanged
          // to avoid stale reads on reconnect when zone_update fires for the same zone.
          try { stmt.updateAccountZone.run(validZone, client.username.toLowerCase()); } catch(e) {}
          const zuFriends = _uidToFriends.get(clientUid);
          if (zuFriends) for (const fn of zuFriends) {
            const fuid = _usernameToUid.get(fn);
            const fc = fuid && clients.get(fuid);
            if (fc && fc.ws.readyState === 1) send(fc.ws, { type:"friend_zone", name:client.username, zone:validZone });
          }
          // Re-broadcast presence with zone on first zone_update after connect
          if (zoneChanged) _broadcastPresence(clientUid, client.username, true, validZone);

          // Update respawnZone when player enters a safe zone
          if (zoneChanged) {
            const zd = ZONE_DB[validZone];
            if (zd && zd.safe) {
              try {
                const rzSave = _getCachedSave(clientUid);
                if (rzSave && rzSave.player && rzSave.player.respawnZone !== validZone) {
                  rzSave.player.respawnZone = validZone;
                  _saveCacheSet(clientUid, rzSave);
                  stmt.upsertSave.run(clientUid, JSON.stringify(rzSave));
                }
              } catch(e) { console.error('[ZONE_UPDATE] respawnZone update error:', e.message); }
            }
          }

          // ── Zone-scoped catalog delivery on zone travel ───────────────────
          // Sends zone-specific items + hostiles for the new zone.
          // Also re-sends global items (zones:[]) so players online when a global
          // item is approved receive it on next zone travel without reconnecting.
          // Client merges into LIVE_ITEMS idempotently — safe to resend.
          if (zoneChanged) {
            try {
              const _zoneOnlyIds = _zoneItemIndex.get(validZone) || new Set();
              const _globalIds   = _zoneItemIndex.get('__global__') || new Set();
              const zoneItems    = [...new Set([..._globalIds, ..._zoneOnlyIds])]
                .map(id => ITEM_DB[id] ? _buildSlimItem(ITEM_DB[id]) : null)
                .filter(Boolean);
              const zoneHostiles = _buildZoneHostiles(validZone);
              if (zoneItems.length || zoneHostiles.length) {
                send(ws, { type:"live_catalog", items: zoneItems, hostiles: zoneHostiles,
                  itemsV: ITEM_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION, _slim: true, _zone: validZone });
              }
            } catch(ezi) { console.error('[ZONE_UPDATE] catalog push error:', ezi.message); }
          }
        }
        break;
      }

      case "friends_update": {
        if (!clientUid) return;
        const names = msg.names;
        if (!Array.isArray(names)) return;
        const friendNames = new Set(names.slice(0, 200).map(n => (n || "").toLowerCase()).filter(Boolean));
        _setFriends(clientUid, friendNames);
        _sendOnlineFriends(ws, clientUid);
        const fuClient = clients.get(clientUid);
        const fuUsername = fuClient ? fuClient.username : null;
        if (fuUsername) {
          const fuZone = fuClient && fuClient.zone ? fuClient.zone : null;
          for (const friendName of friendNames) {
            const friendUid = _usernameToUid.get(friendName);
            if (!friendUid) continue;
            const friendClient = clients.get(friendUid);
            if (!friendClient || friendClient.ws.readyState !== 1) continue;
            send(friendClient.ws, { type: "friend_online", name: fuUsername, zone: fuZone });
            _addFriend(friendUid, fuUsername.toLowerCase());
          }
        }
        break;
      }

      // zone_chat: primary zone chat path used by client
      // (send_zone_chat is the legacy alias path via _fbSet wrapper, rarely triggered)
      case "zone_chat": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const client = clients.get(clientUid);
        if (!client) return;
        // Use server-tracked zone; fall back to client-reported if not yet set
        const chatZone = client.zone || (msg.zone && ZONE_DB[msg.zone] ? msg.zone : null);
        if (client.zone !== chatZone && chatZone) _setClientZone(clientUid, chatZone); // update if was null
        const chatMsg = msg.msg;
        if (!chatZone || !chatMsg) return;
        if (typeof chatMsg !== "string" || chatMsg.length > 200) return;
        // Force name from server-side save - never trust client
        const chatSave = _getCachedSave(clientUid);
        const chatName = chatSave?.player?.name || client.username || "Unknown";
        const chatKey = `${clientUid}_${Date.now()}`;
        stmt.insertZoneChat.run(chatZone, chatKey, chatName, chatMsg, clientUid, Date.now());
        _broadcastZoneChat(chatZone, { name: chatName, msg: chatMsg });
        break;
      }

      case "guild_chat": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const gcMsg = msg.msg;
        if (!gcMsg || typeof gcMsg !== "string" || gcMsg.length > 200) return;
        // Look up the sender's guild from DB
        const gcMembership = stmt.getMemberGuild.get(clientUid);
        if (!gcMembership) return; // not in a guild
        const gcGuildId = gcMembership.guild_id;
        // Force name from server-side save
        const gcSave = _getCachedSave(clientUid);
        const gcName = gcSave?.player?.name || clients.get(clientUid)?.username || "Unknown";
        _broadcastGuildChat(gcGuildId, { name: gcName, msg: gcMsg }, clientUid);
        break;
      }

      case "party_chat": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const pcMsg = msg.msg;
        if (!pcMsg || typeof pcMsg !== "string" || pcMsg.length > 200) return;
        // Look up the sender's party from the subscription map
        const pcPartyId = partySubscriptions.get(clientUid);
        if (!pcPartyId) return; // not in a party
        const pcPartyData = dbGetParty(pcPartyId);
        if (!pcPartyData || !pcPartyData.members) return;
        // Only allow if the sender is actually a member
        const pcIsMember = pcPartyData.members.some(m => m.uid === clientUid);
        if (!pcIsMember) return;
        // Force name from server-side save
        const pcSave = _getCachedSave(clientUid);
        const pcName = pcSave?.player?.name || clients.get(clientUid)?.username || "Unknown";
        _broadcastPartyChat(pcPartyId, { name: pcName, msg: pcMsg }, clientUid);
        break;
      }

      case "set_title": {
        if (!clientUid) return;
        const newTitle = msg.title;
        if (!newTitle || typeof newTitle !== "string" || newTitle.length > 30) return;
        try {
          const save = _getCachedSave(clientUid);
          const p = save && save.player;
          const learned = p && p.learnedTitles ? (Array.isArray(p.learnedTitles) ? p.learnedTitles : Object.values(p.learnedTitles)) : ["New Player"];
          if (!learned.includes(newTitle)) { flagAnomaly(clientUid, "invalid_title", { title: newTitle }); return; }
          p.title = newTitle;
          _writeSave(clientUid, save, { skipOwnerSeed: true }); // title only
          send(ws, { type: "title_ok", title: newTitle });
        } catch (e) { console.error("[TITLE] error:", e.message); }
        break;
      }

      case "get_deferred_data": {
        if (!clientUid) return;
        const _dds = _getCachedSave(clientUid);
        const _ddp = _dds && _dds.player;
        send(ws, { type:"deferred_data",
          inventory: _ddp && _ddp.inventory || {},
          stats: _ddp && _ddp.stats || {},
          learnedRecipes: _ddp && _ddp.learnedRecipes || [],
          learnedTitles: _ddp && _ddp.learnedTitles || [],
          quests: _ddp && _ddp.quests || {},
        });
        break;
      }

      case "delete_save": {
        if (!clientUid) return;
        try {
          stmt.deleteSave.run(clientUid);
          _invalidateSaveCache(clientUid);
          _expectedGold.delete(clientUid);
          send(ws, { type: "save_deleted" });
          console.log(`[DELETE] save removed for uid=${clientUid}`);
        } catch (e) { console.error("[DELETE] error:", e.message); }
        break;
      }

      case "create_character": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        const charName = msg.name;
        if (!charName || typeof charName !== "string" || charName.length < 3 || charName.length > 10 || !/^[A-Za-z]+$/.test(charName)) {
          send(ws, { type:"create_fail", reason:"invalid_name" }); return;
        }
        try {
          const existingSave = _getCachedSave(clientUid);
          if (existingSave && existingSave.charCreated === true) { send(ws, { type:"create_fail", reason:"already_exists" }); return; }
          // Check character name uniqueness
          const existingName = stmt.getCharname.get(charName.toLowerCase());
          if (existingName && existingName.uid !== clientUid) { send(ws, { type:"create_fail", reason:"name_taken" }); return; }
          const player = {
            name: charName, title: "New Player", baseMaxHp: 100, maxHp: 100, hp: 100, energy: 0,
            learnedActions: ["basic_attack", "flee", "provisions"],
            learnedTitles: ["New Player"],
            actionSlots: ["basic_attack", null, null, null, "provisions", "flee"],
            equipment: { gear: null, accessories: [null, null, null] },
            inventory: { materials: [], provisions: [], gears: [], accessories: [] },
            gold: 0, respawnZone: _defaultRespawnZone(), cooldowns: {}, isAlive: true,
            quests: { active: [], completed: [] },
            stats: { kills: {}, exploreCount: 0, goldFromSelling: 0, fishCaught: 0, fishCounts: {}, arena: { wins1v1: 0, losses1v1: 0, wins2v2: 0, losses2v2: 0, wins4v4: 0, losses4v4: 0, rating1v1: 1000, rating2v2: 1000, rating4v4: 1000 } },
            learnedRecipes: [],
            isSubscribed: false,
          };
          const newSave = { charCreated: true, player };
          _writeSave(clientUid, newSave);
          _setExpectedGold(clientUid, 0);
          send(ws, { type: "create_ok", player });
          console.log(`[CREATE] uid=${clientUid} name=${charName}`);
        } catch (e) { console.error("[CREATE] error:", e.message); send(ws, { type:"create_fail", reason:"server_error" }); }
        break;
      }

      // ══════════════════════════════════════════════════════════════════════
      //  ADMIN COMMANDS
      // ══════════════════════════════════════════════════════════════════════

      case "admin_grant_gold": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const amount = msg.amount;
        if (typeof amount !== "number" || amount <= 0 || amount > 100000) return;
        try {
          const save = _getCachedSave(clientUid);
          if (!save?.player) { send(ws, { type:"error", reason:"no_save" }); return; }
          const currentGold = save.player.gold || 0;
          const newGold = currentGold + amount;
          save.player.gold = newGold;
          _writeSave(clientUid, save, { skipOwnerSeed: true }); // gold only
          _setExpectedGold(clientUid, newGold);
          send(ws, { type: "admin_gold_granted", amount, gold: newGold });
          _logTx(clientUid, "admin_grant_gold", { amount, goldAfter: newGold });
          console.log(`[ADMIN] granted ${amount} gold to uid=${clientUid}, new total=${newGold}`);
        } catch (e) { console.error("[ADMIN] grant_gold error:", e.message); }
        break;
      }

      case "admin_get_flags": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) { send(ws, { type:"admin_flags", data:{} }); break; }
        try {
          const rows = stmt.getAllAnomalies.all();
          const data = {};
          for (const row of rows) {
            if (!data[row.uid]) data[row.uid] = {};
            const key = "a_" + row.ts + "_" + Math.random().toString(36).slice(2,6);
            let details = null;
            try { details = row.details ? JSON.parse(row.details) : null; } catch {}
            data[row.uid][key] = { reason: row.reason, details, ts: row.ts };
          }
          send(ws, { type:"admin_flags", data });
        } catch (e) { console.error("[ADMIN] get_flags error:", e.message); send(ws, { type:"admin_flags", data: {} }); }
        break;
      }


      case "admin_lookup_name": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const { charName: lookupName } = msg;
        if (!lookupName) return;
        try {
          const cnRow = stmt.getCharname.get(lookupName.toLowerCase());
          if (!cnRow) { send(ws, { type:"admin_lookup_result", found:false, charName: lookupName }); return; }
          const uid = cnRow.uid;
          const username = cnRow.username;
          if (!uid) { send(ws, { type:"admin_lookup_result", found:false, charName: lookupName }); return; }
          const save = _getCachedSave(uid);
          const player = save?.player || null;
          const banned = save?.banned || false;
          const onlineClient = clients.get(uid);
          const currentIp = onlineClient?.ip || null;
          const deviceId = onlineClient?.deviceId || null;
          const fingerprint = onlineClient?.fingerprint || null;
          let lastIp = null, lastDeviceId = null, lastFingerprint = null;
          if (!currentIp) {
            const lastLogin = stmt.getTxLog.all(uid, 1);
            if (lastLogin.length) { try { const d = JSON.parse(lastLogin[0].details); lastIp = d.ip || null; lastDeviceId = d.deviceId || null; lastFingerprint = d.fingerprint || null; } catch(e){} }
          }
          send(ws, { type:"admin_player_data", targetUid:uid, username, player, banned, ip: currentIp || lastIp || "unknown", deviceId: deviceId || lastDeviceId || "unknown", fingerprint: fingerprint || lastFingerprint || "unknown", online: !!currentIp });
        } catch (e) { send(ws, { type:"admin_lookup_result", found:false, charName: lookupName }); }
        break;
      }

      case "admin_ban": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const { targetUid: banUid, banned } = msg;
        if (!banUid) return;
        // Prevent banning admins unless you're lead admin
        if (banned !== false) {
          const banTarget = clients.get(banUid);
          const banTargetUser = banTarget ? banTarget.username.toLowerCase() : "";
          if (LEAD_ADMINS.has(banTargetUser)) { send(ws, { type:"admin_action_fail", action:"ban", reason:"cannot_ban_lead_admin" }); return; }
          if (ADMIN_USERNAMES.has(banTargetUser) && !_isLeadAdmin(clientUid)) { send(ws, { type:"admin_action_fail", action:"ban", reason:"only_lead_admin_can_ban_admins" }); return; }
        }
        try {
          const save = _getCachedSave(banUid) || {};
          save.banned = banned !== false;
          if (banned !== false) save.bannedAt = Date.now(); else delete save.bannedAt;
          _writeSave(banUid, save, { skipOwnerSeed: true, sync: true }); // ban flag — target may be offline
          const bannedClient = clients.get(banUid);
          if (bannedClient && bannedClient.ws.readyState === 1) {
            send(bannedClient.ws, { type:"banned" });
            bannedClient.ws.close();
          }
          send(ws, { type:"admin_action_ok", action: banned !== false ? "ban" : "unban", targetUid: banUid });
          flagAnomaly(banUid, banned !== false ? "admin_ban" : "admin_unban", { by: clientUid });
        } catch (e) { send(ws, { type:"admin_action_fail", action:"ban", reason: e.message }); }
        break;
      }

      // ── Ban/Unban by character name (from chat commands) ──────────────
      case "admin_ban_name": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) { send(ws, { type:"admin_action_fail", action:"ban", reason:"not_authorized" }); break; }
        const { charName: banName } = msg;
        if (!banName) { send(ws, { type:"admin_action_fail", action:"ban", reason:"no_name" }); break; }
        try {
          const cnRow = stmt.getCharname.get(banName.toLowerCase());
          if (!cnRow) { send(ws, { type:"admin_action_fail", action:"ban", reason:"player_not_found" }); break; }
          const banUid = cnRow.uid;
          const banUsername = cnRow.username ? cnRow.username.toLowerCase() : "";
          if (banUid === clientUid) { send(ws, { type:"admin_action_fail", action:"ban", reason:"cannot_ban_self" }); break; }
          if (LEAD_ADMINS.has(banUsername)) { send(ws, { type:"admin_action_fail", action:"ban", reason:"cannot_ban_lead_admin" }); break; }
          if (ADMIN_USERNAMES.has(banUsername) && !_isLeadAdmin(clientUid)) { send(ws, { type:"admin_action_fail", action:"ban", reason:"only_lead_admin_can_ban_admins" }); break; }
          const save = _getCachedSave(banUid) || {};
          save.banned = true;
          save.bannedAt = Date.now();
          _writeSave(banUid, save, { skipOwnerSeed: true, sync: true }); // ban flag — target may be offline
          if (bannedClient && bannedClient.ws && bannedClient.ws.readyState === 1) {
            send(bannedClient.ws, { type:"banned" });
            bannedClient.ws.close();
          }
          send(ws, { type:"admin_action_ok", action:"ban", charName: banName });
          flagAnomaly(banUid, "admin_ban", { by: clientUid });
          _logTx(banUid, "admin_ban", { by: clientUid });
          console.log(`[ADMIN] ban ${banName} (${banUid}) by ${clientUid}`);
        } catch (e) { send(ws, { type:"admin_action_fail", action:"ban", reason: e.message }); }
        break;
      }

      case "admin_unban_name": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) { send(ws, { type:"admin_action_fail", action:"unban", reason:"not_authorized" }); break; }
        const { charName: unbanName } = msg;
        if (!unbanName) { send(ws, { type:"admin_action_fail", action:"unban", reason:"no_name" }); break; }
        try {
          const cnRow = stmt.getCharname.get(unbanName.toLowerCase());
          if (!cnRow) { send(ws, { type:"admin_action_fail", action:"unban", reason:"player_not_found" }); break; }
          const unbanUid = cnRow.uid;
          const save = _getCachedSave(unbanUid) || {};
          save.banned = false;
          delete save.bannedAt;
          _writeSave(unbanUid, save, { skipOwnerSeed: true, sync: true }); // ban flag — target may be offline
          send(ws, { type:"admin_action_ok", action:"unban", charName: unbanName });
          flagAnomaly(unbanUid, "admin_unban", { by: clientUid });
          _logTx(unbanUid, "admin_unban", { by: clientUid });
          console.log(`[ADMIN] unban ${unbanName} (${unbanUid}) by ${clientUid}`);
        } catch (e) { send(ws, { type:"admin_action_fail", action:"unban", reason: e.message }); }
        break;
      }

      case "admin_warn": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const { targetUid: warnUid, message: warnMsg } = msg;
        if (!warnUid || !warnMsg) return;
        try {
          const key = `admin_warn_${Date.now()}`;
          stmt.upsertInbox.run(warnUid, "system", key, JSON.stringify({ m: warnMsg, t: Date.now(), from:"SYSTEM" }), Date.now());
          _broadcastInbox(warnUid);
          flagAnomaly(warnUid, "admin_warn", { by: clientUid, message: warnMsg });
          send(ws, { type:"admin_action_ok", action:"warn", targetUid: warnUid });
        } catch (e) { send(ws, { type:"admin_action_fail", action:"warn", reason: e.message }); }
        break;
      }

      case "admin_clear_flags": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const { targetUid: cfUid } = msg;
        if (!cfUid) return;
        try {
          stmt.deleteAnomalies.run(cfUid);
          send(ws, { type:"admin_action_ok", action:"clear_flags", targetUid: cfUid });
        } catch (e) { send(ws, { type:"admin_action_fail", action:"clear_flags", reason: e.message }); }
        break;
      }

      case "admin_get_banned": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) { send(ws, { type:"admin_banned_list", players:[] }); break; }
        try {
          // Scan all saves for banned flag
          const allSaves = stmtGetAllSaves.all();
          const players = [];
          for (const row of allSaves) {
            try {
              const save = JSON.parse(row.data);
              if (save && save.banned === true) {
                players.push({ uid: row.uid, name: save.player?.name || null, bannedAt: save.bannedAt || null });
              }
            } catch {}
          }
          send(ws, { type:"admin_banned_list", players });
        } catch (e) { send(ws, { type:"admin_banned_list", players: [] }); }
        break;
      }

      // ── Make another player an admin (by character name) - LEAD ADMIN ONLY
      case "admin_make_admin": {
        if (!clientUid || !_isLeadAdmin(clientUid)) { send(ws, { type:"admin_action_fail", action:"make_admin", reason:"not_authorized" }); break; }
        const { charName: maName } = msg;
        if (!maName) { send(ws, { type:"admin_action_fail", action:"make_admin", reason:"no_name" }); break; }
        try {
          const cnRow = stmt.getCharname.get(maName.toLowerCase());
          if (!cnRow || !cnRow.username) {
            send(ws, { type:"admin_action_fail", action:"make_admin", reason:"player_not_found" }); break;
          }
          const targetUsername = cnRow.username.toLowerCase();
          if (ADMIN_USERNAMES.has(targetUsername)) {
            send(ws, { type:"admin_action_ok", action:"make_admin", targetUsername, already: true }); break;
          }
          ADMIN_USERNAMES.add(targetUsername);
          stmtAddAdmin.run(targetUsername);
          const targetUid = _usernameToUid.get(targetUsername);
          if (targetUid) {
            ADMIN_UIDS.add(targetUid);
            const targetClient = clients.get(targetUid);
            if (targetClient && targetClient.ws.readyState === 1) {
              send(targetClient.ws, { type:"admin_promoted" });
            }
          }
          send(ws, { type:"admin_action_ok", action:"make_admin", targetUsername });
          console.log(`[ADMIN] ${targetUsername} promoted to admin by uid=${clientUid}`);
        } catch (e) {
          send(ws, { type:"admin_action_fail", action:"make_admin", reason: e.message });
        }
        break;
      }

      // ── Remove admin from a player (by character name) - LEAD ADMIN ONLY
      case "admin_remove_admin": {
        if (!clientUid || !_isLeadAdmin(clientUid)) { send(ws, { type:"admin_action_fail", action:"remove_admin", reason:"not_authorized" }); break; }
        const { charName: raName } = msg;
        if (!raName) { send(ws, { type:"admin_action_fail", action:"remove_admin", reason:"no_name" }); break; }
        try {
          const cnRow = stmt.getCharname.get(raName.toLowerCase());
          if (!cnRow || !cnRow.username) {
            send(ws, { type:"admin_action_fail", action:"remove_admin", reason:"player_not_found" }); break;
          }
          const targetUsername = cnRow.username.toLowerCase();
          // Cannot remove lead admins
          if (LEAD_ADMINS.has(targetUsername)) {
            send(ws, { type:"admin_action_fail", action:"remove_admin", reason:"cannot_remove_lead_admin" }); break;
          }
          ADMIN_USERNAMES.delete(targetUsername);
          stmtRemoveAdmin.run(targetUsername);
          const targetUid = _usernameToUid.get(targetUsername);
          if (targetUid) ADMIN_UIDS.delete(targetUid);
          send(ws, { type:"admin_action_ok", action:"remove_admin", targetUsername });
          console.log(`[ADMIN] ${targetUsername} demoted from admin by uid=${clientUid}`);
        } catch (e) {
          send(ws, { type:"admin_action_fail", action:"remove_admin", reason: e.message });
        }
        break;
      }

      case "admin_make_dev": {
        // Lead Admin only: grant developer role to a player
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"admin_action_fail", action:"make_dev", reason:"not_authorized" }); break;
        }
        const mdTarget = (msg.charName || "").trim().toLowerCase();
        if (!mdTarget) { send(ws, { type:"admin_action_fail", action:"make_dev", reason:"no_name" }); break; }
        if (LEAD_ADMINS.has(mdTarget)) {
          send(ws, { type:"dev_result", ok:true, action:"make_dev", note:"Already lead admin." }); break;
        }
        // Look up account
        const mdAccRow = stmtGetAccountByCharName.get(mdTarget);
        if (!mdAccRow) {
          send(ws, { type:"dev_result", ok:false, action:"make_dev", error:"Player not found." }); break;
        }
        stmtAddDev.run(mdAccRow.username.toLowerCase());
        DEV_USERNAMES.add(mdAccRow.username.toLowerCase());
        // Notify target if online
        const mdClient = clients.get(mdAccRow.uid);
        if (mdClient && mdClient.ws.readyState === 1) {
          send(mdClient.ws, { type:"dev_promoted", message:"You have been granted Developer access." });
        }
        stmtDevAudit.run(clients.get(clientUid)?.username || clientUid, "make_dev", "account", mdTarget, 0, Date.now());
        send(ws, { type:"dev_result", ok:true, action:"make_dev", charName: mdTarget });
        console.log(`[ADMIN] ${clients.get(clientUid)?.username} granted dev to ${mdTarget}`);
        break;
      }

      case "admin_remove_dev": {
        // Lead Admin only: revoke developer role
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"admin_action_fail", action:"remove_dev", reason:"not_authorized" }); break;
        }
        const rdTarget = (msg.charName || "").trim().toLowerCase();
        if (!rdTarget) { send(ws, { type:"admin_action_fail", action:"remove_dev", reason:"no_name" }); break; }
        if (LEAD_ADMINS.has(rdTarget)) {
          send(ws, { type:"dev_result", ok:false, action:"remove_dev", error:"Cannot remove lead admin developer access." }); break;
        }
        const rdAccRow = stmtGetAccountByCharName.get(rdTarget);
        if (!rdAccRow) {
          send(ws, { type:"dev_result", ok:false, action:"remove_dev", error:"Player not found." }); break;
        }
        stmtRemoveDev.run(rdAccRow.username.toLowerCase());
        DEV_USERNAMES.delete(rdAccRow.username.toLowerCase());
        stmtDevAudit.run(clients.get(clientUid)?.username || clientUid, "remove_dev", "account", rdTarget, 0, Date.now());
        send(ws, { type:"dev_result", ok:true, action:"remove_dev", charName: rdTarget });
        console.log(`[ADMIN] ${clients.get(clientUid)?.username} revoked dev from ${rdTarget}`);
        break;
      }

      case "enter_subzone": {
        // Party leader (or solo) enters a dungeon/raid/trial subzone
        if (!clientUid) break;
        if (_inArenaQueue(clientUid)) { send(ws, { type:"subzone_denied", reason:"in_arena_queue" }); break; }
        if (rooms.size >= ROOMS_WARN) console.warn(`[ROOMS] high room count: ${rooms.size} (pve=${rooms.size - _pvpRoomCount} pvp=${_pvpRoomCount}) — possible leak`);
        const { zoneId, subzoneId, subzoneType, memberUids, partyId: szPartyId } = msg;
        if (!zoneId || !subzoneId) break;
        const zone = ZONE_DB[zoneId];
        if (!zone || zone.safe) { send(ws, { type:"subzone_denied", reason:"invalid_zone" }); break; }
        const subzones = zone.subzones || [];
        const sz = subzones.find(s => (s.id === subzoneId || s.name === subzoneId) && s.type === subzoneType);
        if (!sz) { send(ws, { type:"subzone_denied", reason:"subzone_not_found" }); break; }
        const levels = sz._levels || [];
        if (!levels.length) { send(ws, { type:"subzone_denied", reason:"no_levels_defined" }); break; }
        // Validate member list (same logic as start_combat)
        let szMemberUids;
        if (memberUids?.length > 1) {
          const partyDoc = szPartyId ? dbGetParty(szPartyId) : null;
          if (!partyDoc) { szMemberUids = [clientUid]; }
          else {
            const partyUids = new Set((partyDoc.members || []).map(m => {
              const acc = stmt.getAccount.get(m.username?.toLowerCase());
              return acc?.uid || null;
            }).filter(Boolean));
            szMemberUids = memberUids.filter(uid => partyUids.has(uid));
            if (!szMemberUids.includes(clientUid)) szMemberUids = [clientUid];
            if (!szMemberUids.length) szMemberUids = [clientUid];
          }
        } else { szMemberUids = [clientUid]; }
        // Build enemy list from Level 1 hostiles
        const _buildLevelEnemies = (lvl) => {
          return (lvl._hostiles || []).map((h, i) => {
            const hid = (h.id || '').trim();
            const def = ENEMY_DB[hid];
            if (!def) { console.warn(`[SUBZONE] unknown hostile id="${hid}" — skipping`); return null; }
            return { uid: `${hid}_${i}_${Date.now()}`, type: hid };
          }).filter(Boolean);
        };
        const lvl1Enemies = _buildLevelEnemies(levels[0]);
        if (!lvl1Enemies.length) { send(ws, { type:"subzone_denied", reason:"no_valid_hostiles_in_level_1" }); break; }
        const roomId = szPartyId || `solo_${clientUid}_${Date.now()}`;
        try {
          const room = await CombatRoom.create(roomId, szMemberUids, lvl1Enemies);
          // Tag the room with subzone progression data
          room._subzone = {
            zoneId, subzoneId, subzoneType,
            subzoneName: sz.name || subzoneType,
            levels,
            currentLevel: 1,
            totalLevels: levels.length,
            buildLevelEnemies: _buildLevelEnemies,
          };
          rooms.set(roomId, room);
          room.start();
          _ejectCheatersFromRoom(room, roomId);
          console.log(`[SUBZONE] uid=${clientUid} entered ${subzoneType}:"${sz.name}" zone=${zoneId} level=1/${levels.length} enemies=${lvl1Enemies.length}`);
        } catch(e) {
          console.error("[SUBZONE] create error:", e);
          send(ws, { type:"subzone_denied", reason:"start_failed" });
        }
        break;
      }

      case "get_live_zones": {
        // Any authenticated client can request the full zone catalog
        if (!clientUid) break;
        try { send(ws, { type:"live_zones", zones: _buildZoneCatalog(), _version: ZONE_DB_VERSION }); }
        catch(e) { send(ws, { type:"live_zones", zones:[], error: e.message }); }
        break;
      }

      case "get_live_items": {
        if (!clientUid) break;
        try { send(ws, { type:"live_items", items: Object.values(ITEM_DB).map(i=>_buildSlimItem(i)).filter(Boolean), _version: ITEM_DB_VERSION, _slim: true }); }
        catch(e) { send(ws, { type:"live_items", items:[], error: e.message }); }
        break;
      }

      case "get_item_detail": {
        // Client requests full detail for a single item (on popup open, equip attempt, etc.)
        // Returns all fields including description, _effects, full stats.
        // Also registers ownership so future catalog updates reach this player,
        // and pushes the current slim entry to guarantee the client has fresh data.
        if (!clientUid) break;
        try {
          const iid = msg.id;
          if (!iid) { send(ws, { type:"item_detail", id: iid, error:"missing id" }); break; }
          const item = ITEM_DB[iid];
          if (!item) { send(ws, { type:"item_detail", id: iid, error:"not_found" }); break; }
          // Register: this player has interacted with this item — ensure they get future updates
          _addItemOwner(clientUid, iid);
          const { _live, ...full } = item;
          // Push fresh slim entry alongside full detail so client catalog is always current
          const slim = _buildSlimItem(full);
          send(ws, { type:"item_detail", item: full, slim });
        } catch(e) { send(ws, { type:"item_detail", error: e.message }); }
        break;
      }

      case "get_item_details_batch": {
        // Batch version: client passes ids[] — used when opening inventory or market tab.
        // Registers ownership for all requested ids so future updates reach this player.
        if (!clientUid) break;
        try {
          const ids = Array.isArray(msg.ids) ? msg.ids.slice(0, 50) : []; // cap at 50
          const details = ids.map(id => {
            const item = ITEM_DB[id];
            if (!item) return null;
            _addItemOwner(clientUid, id); // register: player has viewed this item
            const { _live, ...full } = item;
            return full;
          }).filter(Boolean);
          send(ws, { type:"item_details_batch", items: details });
        } catch(e) { send(ws, { type:"item_details_batch", items:[], error: e.message }); }
        break;
      }

      case "get_live_hostiles": {
        if (!clientUid) break;
        try { send(ws, { type:"live_hostiles", hostiles: _buildHostilesCatalog(), _version: ENEMY_DB_VERSION }); }
        catch(e) { send(ws, { type:"live_hostiles", hostiles:[], error: e.message }); }
        break;
      }

      case "get_hostile_detail": {
        // Client requests full detail for a single hostile (on popup open, combat prep, etc.)
        // Registers the player as a viewer so they receive future updates to this hostile
        // even if they travel to a different zone before the update arrives.
        if (!clientUid) break;
        try {
          const hid = msg.id;
          if (!hid) { send(ws, { type:"hostile_detail", id: hid, error:"missing id" }); break; }
          const h = ENEMY_DB[hid];
          if (!h || !h._live) { send(ws, { type:"hostile_detail", id: hid, error:"not_found" }); break; }
          // Register: this player has viewed this hostile — ensure future updates reach them
          _addHostileViewer(clientUid, hid);
          const { _live, ...full } = h;
          send(ws, { type:"hostile_detail", hostile: { id: hid, ...full } });
        } catch(e) { send(ws, { type:"hostile_detail", error: e.message }); }
        break;
      }

      case "dev_delete_entity_hard": {
        // Lead admin only: permanently delete any entity regardless of status
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        try {
          const ent = stmtDevGetEntity.get(msg.id);
          if (!ent) { send(ws, { type:"dev_delete_entity_hard_result", ok:false, error:"Entity not found." }); break; }
          stmtDeleteDevEntity.run(ent.id);
          // Also delete any linked submissions
          stmtDeleteDevSubByEntity.run(ent.id);
          stmtDevAudit.run(clients.get(clientUid)?.username||clientUid, "delete_entity", ent.category, ent.name, ent.version, Date.now());
          // Rebuild catalogs if it was live
          if (ent.status === 'live') {
            if (ent.category === 'zone') {
              // Surgically remove zone's hostile index entries, then delete the zone
              const _dzEnemies = ZONE_DB[ent.id]?.enemies || [];
              delete ZONE_DB[ent.id];
              ZONE_DB_VERSION = Date.now();
              _zoneHostileIndex.delete(ent.id);
              for (const eid of _dzEnemies) {
                const zs = _hostileToZones.get(eid);
                if (zs) { zs.delete(ent.id); if (!zs.size) _hostileToZones.delete(eid); }
              }
              _logCatalogChange(ent.id, ent.category, 'delete');
              _broadcastAll({ type:"catalog_delta", itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
                upsertedZones: [], deletedZones: [ent.id], upsertedItems: [], deletedItems: [], upsertedHostiles: [], deletedHostiles: [] });
            } else if (ent.category === 'hostile' || ent.category === 'boss') {
              const _hd = _rebuildOneHostile(ent.id);
              _updateZoneHostileIndex(ent.id, true);
              _logCatalogChange(ent.id, ent.category, 'delete');
              _broadcastHostileDelta([], [ent.id]);
              _broadcastHostileToViewers([], [ent.id]);
            } else {
              // Delta: just remove this item and tell clients
              const _delId = ent.id;
              if (ITEM_DB[_delId]) delete ITEM_DB[_delId];
              ITEM_DB_VERSION = Date.now();
              _logCatalogChange(_delId, ent.category, 'delete');
              // Deletions: zone broadcast evicts from LIVE_ITEMS for market display;
              // owner broadcast ensures equipped/inventory players also evict immediately
              _broadcastItemDelta([], [_delId]);
              _broadcastItemToOwners([], [_delId]);
            }
          }
          send(ws, { type:"dev_delete_entity_hard_result", ok:true, id: ent.id, category: ent.category });
        } catch(e) { send(ws, { type:"dev_delete_entity_hard_result", ok:false, error: e.message }); }
        break;
      }

      case "dev_delete_entity": {
        // Dev or lead admin: delete a draft entity permanently
        if (!clientUid || (!DEV_USERNAMES.has(clients.get(clientUid)?.username?.toLowerCase()) && !_isLeadAdmin(clientUid))) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        try {
          const ent = stmtDevGetEntity.get(msg.id);
          if (!ent) { send(ws, { type:"dev_delete_entity_result", ok:false, error:"Entity not found." }); break; }
          if (ent.status !== "draft") { send(ws, { type:"dev_delete_entity_result", ok:false, error:"Only drafts can be deleted. Submitted or live entities cannot be removed this way." }); break; }
          stmtDeleteDevEntity.run(ent.id);
          stmtDevAudit.run(clients.get(clientUid)?.username||clientUid, "delete_draft", ent.category, ent.name, ent.version, Date.now());
          send(ws, { type:"dev_delete_entity_result", ok:true, id: ent.id });
        } catch(e) { send(ws, { type:"dev_delete_entity_result", ok:false, error: e.message }); }
        break;
      }

      case "dev_entities":        // alias sent by zone validate in client
      case "dev_get_entities": {
        // Fetch all entities for a given category
        if (!clientUid || (!DEV_USERNAMES.has(clients.get(clientUid)?.username?.toLowerCase()) && !_isLeadAdmin(clientUid))) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const catKey = (msg.category || "").toLowerCase().replace(/[^a-z-]/g,"");
        if (!catKey) { send(ws, { type:"dev_entities_result", category: catKey, entities:[] }); break; }
        try {
          const rows = stmtDevGetEntities.all(catKey);
          send(ws, { type:"dev_entities_result", category: catKey, entities: rows });
        } catch(e) { send(ws, { type:"dev_entities_result", category: catKey, entities:[], error: e.message }); }
        break;
      }

      case "dev_save_entity": {
        // Save or update a dev entity (creates a draft version, not live)
        if (!clientUid || (!DEV_USERNAMES.has(clients.get(clientUid)?.username?.toLowerCase()) && !_isLeadAdmin(clientUid))) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const actor = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        const eId = msg.id || `${(msg.category||"misc").toLowerCase()}_${now}_${Math.random().toString(36).slice(2,7)}`;
        const eVer = (msg.version || 1);
        try {
          // Always embed the row id inside the data blob so catalog rebuilds and the edit form have it
          const saveData = Object.assign({}, msg.data || {});
          if (!saveData.id) saveData.id = eId;
          stmtDevSaveEntity.run(
            eId, msg.category || "misc", msg.name || "Unnamed",
            eVer, msg.status || "draft", msg.flag || "Active",
            JSON.stringify(saveData),
            msg.createdBy || actor, msg.createdAt || now,
            actor, now
          );
          stmtDevAudit.run(actor, "save_draft", msg.category, msg.name, eVer, now);
          send(ws, { type:"dev_save_result", ok:true, id: eId, version: eVer });
        } catch(e) {
          send(ws, { type:"dev_save_result", ok:false, error: e.message });
        }
        break;
      }

      case "dev_submit": {
        // Submit an entity draft for admin review
        if (!clientUid || (!DEV_USERNAMES.has(clients.get(clientUid)?.username?.toLowerCase()) && !_isLeadAdmin(clientUid))) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const submitter = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        try {
          // Check for open conflicts
          const existingPending = db.prepare(
            `SELECT id FROM dev_submissions WHERE entity_name=? AND category=? AND status='pending'`
          ).get(msg.entityName || "", msg.category || "");
          if (existingPending) {
            send(ws, { type:"dev_submit_result", ok:false, conflict:true,
              error:`There is already a pending submission for "${msg.entityName}". Resolve it first.` }); break;
          }
          const subId = stmtDevInsertSub.run(
            submitter,
            msg.subType || "new",
            msg.category || "misc",
            msg.entityName || "Unnamed",
            msg.entityId || null,
            msg.version || 1,
            "pending",
            JSON.stringify(msg.data || {}),
            msg.aiAnalysis ? JSON.stringify(msg.aiAnalysis) : null,
            now
          ).lastInsertRowid;
          stmtDevAudit.run(submitter, "submit", msg.category, msg.entityName, msg.version || 1, now);
          // Notify lead admins online
          for (const [uid, c] of clients) {
            if (LEAD_ADMINS.has((c.username||"").toLowerCase()) && c.ws.readyState === 1) {
              send(c.ws, { type:"dev_new_submission", id: subId, creator: submitter,
                category: msg.category, entityName: msg.entityName });
            }
          }
          send(ws, { type:"dev_submit_result", ok:true, id: subId });
        } catch(e) {
          send(ws, { type:"dev_submit_result", ok:false, error: e.message });
        }
        break;
      }

      case "dev_get_submissions": {
        // Get submissions queue (lead admin or dev)
        if (!clientUid || (!DEV_USERNAMES.has(clients.get(clientUid)?.username?.toLowerCase()) && !_isLeadAdmin(clientUid))) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        try {
          const subs = stmtDevGetSubs.all();
          send(ws, { type:"dev_submissions_result", submissions: subs });
        } catch(e) { send(ws, { type:"dev_submissions_result", submissions:[], error: e.message }); }
        break;
      }

      case "dev_approve_submission": {
        // Lead admin only: approve a submission → promote entity to live
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const approver = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        try {
          const sub = stmtDevGetSub.get(msg.submissionId);
          if (!sub) { send(ws, { type:"dev_approve_result", ok:false, error:"Submission not found." }); break; }
          if (sub.status !== "pending") { send(ws, { type:"dev_approve_result", ok:false, error:"Submission is not pending." }); break; }
          // Promote entity to live — also update name and data from submission so edits take effect
          const subData = sub.data || '{}';
          let subName = sub.entity_name || '';
          let parsedSubData = {};
          try { parsedSubData = JSON.parse(subData); if (parsedSubData.name) subName = parsedSubData.name; } catch(ep) {}
          const approvedVersion = sub.version || 1;
          let newEid = null; // hoisted so post-approval rebuild blocks can reference it safely
          if (sub.entity_id) {
            // Existing entity (edit) — update to live and store new version
            // If the entity's id field in data changed (e.g. typo fix), we must replace the row
            const canonicalId = parsedSubData.id || sub.entity_id;
            if (canonicalId !== sub.entity_id) {
              // ID changed — delete old row and insert with correct ID
              stmtDeleteDevEntity.run(sub.entity_id);
              stmtDevSaveEntity.run(
                canonicalId, sub.category || "misc", subName,
                approvedVersion, "live", "Active",
                subData,
                sub.creator || approver, now,
                approver, now
              );
              // Also fix the submission row's entity_id so future edits reference the correct row
              stmtUpdateSubEntityId.run(canonicalId, sub.id);
            } else {
              db.prepare(`UPDATE dev_entities SET status=?, name=?, data=?, version=?, updated_by=?, updated_at=? WHERE id=?`)
                .run("live", subName, subData, approvedVersion, approver, now, sub.entity_id);
            }
          } else {
            // New entity — insert into dev_entities as live
            newEid = parsedSubData.id ||
              `${(sub.category||"misc").toLowerCase()}_${now}_${Math.random().toString(36).slice(2,7)}`;
            if (!parsedSubData.id) parsedSubData.id = newEid;
            const finalData = JSON.stringify(parsedSubData);
            stmtDevSaveEntity.run(
              newEid, sub.category || "misc", subName,
              approvedVersion, "live", "Active",
              finalData,
              sub.creator || approver, now,
              approver, now
            );
            // Write entity_id and corrected data back onto the submission row
            stmtUpdateSubEntityIdAndData.run(newEid, finalData, sub.id);
          }
          stmtDevUpdateSubStatus.run("approved", approver, now, null, sub.id);
          stmtDevAudit.run(approver, "approve", sub.category, sub.entity_name, sub.version, now);
          // Delta rebuild: zone handled once below (with hostile index update)
          if (sub.category === 'hostile' || sub.category === 'boss') {
            try {
              const _hd = _rebuildOneHostile(sub.entity_id || newEid);
              if (_hd) {
                _logCatalogChange(_hd.hostile?.id || _hd.deleted, sub.category, _hd.deleted ? 'delete' : 'upsert');
                _updateZoneHostileIndex(_hd.hostile?.id || _hd.deleted, !!_hd.deleted);
                const _upserted = _hd.hostile ? [_hd.hostile] : [];
                const _deleted  = [...(_hd.deleted ? [_hd.deleted] : []), ...(_hd.oldDeleted ? [_hd.oldDeleted] : [])];
                _broadcastHostileDelta(_upserted, _deleted);
                _broadcastHostileToViewers(_upserted, _deleted);
              }
            } catch(ezb) { console.error('[ENEMIES] delta error:', ezb.message); }
          }
          if (sub.category === 'zone') {
            try {
              const _zid = sub.entity_id || newEid;
              const _oldEnemies = new Set(ZONE_DB[_zid]?.enemies || []);
              const _delta = _rebuildOneZone(_zid);
              if (_delta) {
                const _newEnemies = new Set(_delta.zone?.enemies || []);
                for (const eid of _oldEnemies) { if (!_newEnemies.has(eid)) _updateZoneHostileIndex(eid, false); }
                for (const eid of _newEnemies) { if (!_oldEnemies.has(eid)) _updateZoneHostileIndex(eid, false); }
                if (_delta.deleted) for (const eid of _oldEnemies) _updateZoneHostileIndex(eid, false);
                _logCatalogChange(_delta.zone?.id || _delta.deleted, sub.category, _delta.deleted ? 'delete' : 'upsert');
                const _upsertedZones = _delta.zone ? [_delta.zone] : [];
                const _deletedZones  = [...(_delta.deleted ? [_delta.deleted] : []), ...(_delta.oldDeleted ? [_delta.oldDeleted] : [])];
                _broadcastAll({ type:"catalog_delta",
                  itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
                  upsertedZones: _upsertedZones, deletedZones: _deletedZones,
                  upsertedItems: [], deletedItems: [], upsertedHostiles: [], deletedHostiles: [] });
              }
            } catch(ezb) { console.error('[ZONE_DB] delta error:', ezb.message); }
          }
          if (['equipment','action','material','provision','gears','accessories'].includes(sub.category)) {
            try {
              const _delta = _rebuildOneItem(sub.entity_id || newEid);
              if (_delta) {
                _logCatalogChange(_delta.item?.id || _delta.deleted, sub.category, _delta.deleted ? 'delete' : 'upsert');
                const _slimArr = _delta.item ? [_buildSlimItem(_delta.item)].filter(Boolean) : [];
                const _delArr  = [...(_delta.deleted ? [_delta.deleted] : []), ...(_delta.oldDeleted ? [_delta.oldDeleted] : [])];
                _broadcastItemDelta(_slimArr, _delArr);
                _broadcastItemToOwners(_slimArr, _delArr);
              }
            } catch(ezb) { console.error('[ITEMS] delta error:', ezb.message); }
          }
          send(ws, { type:"dev_approve_result", ok:true, id: sub.id });
        } catch(e) { send(ws, { type:"dev_approve_result", ok:false, error: e.message }); }
        break;
      }

      case "dev_reject_submission": {
        // Lead admin only: reject a submission
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const rejector = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        try {
          const sub = stmtDevGetSub.get(msg.submissionId);
          if (!sub) { send(ws, { type:"dev_reject_result", ok:false, error:"Submission not found." }); break; }
          stmtDevUpdateSubStatus.run("rejected", rejector, now, msg.reason || "No reason provided.", sub.id);
          stmtDevAudit.run(rejector, "reject", sub.category, sub.entity_name, sub.version, now);
          // Notify creator if online
          const creatorUid = _usernameToUid.get((sub.creator||"").toLowerCase());
          if (creatorUid) {
            const cc = clients.get(creatorUid);
            if (cc && cc.ws.readyState === 1) {
              send(cc.ws, { type:"dev_submission_rejected", entityName: sub.entity_name,
                reason: msg.reason || "No reason provided." });
            }
          }
          send(ws, { type:"dev_reject_result", ok:true, id: sub.id });
        } catch(e) { send(ws, { type:"dev_reject_result", ok:false, error: e.message }); }
        break;
      }

      case "dev_delete_submission": {
        // Lead admin only: permanently delete a rejected submission from the queue
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        try {
          const sub = stmtDevGetSub.get(msg.submissionId);
          if (!sub) { send(ws, { type:"dev_delete_result", ok:false, error:"Submission not found." }); break; }
          if (sub.status === "pending") { send(ws, { type:"dev_delete_result", ok:false, error:"Cannot delete a pending submission. Reject it first." }); break; }
          stmtDeleteDevSubById.run(sub.id);
          stmtDevAudit.run(clients.get(clientUid)?.username||clientUid, "delete_submission", sub.category, sub.entity_name, sub.version, Date.now());
          send(ws, { type:"dev_delete_result", ok:true, id: sub.id });
        } catch(e) { send(ws, { type:"dev_delete_result", ok:false, error: e.message }); }
        break;
      }

      case "dev_create_package": {
        // Create or update a package (draft stage)
        if (!clientUid || (!DEV_USERNAMES.has(clients.get(clientUid)?.username?.toLowerCase()) && !_isLeadAdmin(clientUid))) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const pkgCreator = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        try {
          if (msg.packageId) {
            // Update existing package
            const existing = stmtPkgGet.get(msg.packageId);
            if (!existing || existing.creator !== pkgCreator) {
              send(ws, { type:"dev_pkg_result", ok:false, error:"Package not found or not yours." }); break;
            }
            stmtPkgUpdate.run(msg.name||existing.name, msg.description||existing.description, now, msg.packageId);
            send(ws, { type:"dev_pkg_result", ok:true, packageId:msg.packageId, action:"updated" });
          } else {
            const result = stmtPkgInsert.run(msg.name||"Unnamed Package", msg.description||"", pkgCreator, "draft", now, now);
            send(ws, { type:"dev_pkg_result", ok:true, packageId:result.lastInsertRowid, action:"created" });
          }
        } catch(e) { send(ws, { type:"dev_pkg_result", ok:false, error:e.message }); }
        break;
      }

      case "dev_submit_package": {
        // Submit a package — wraps multiple submissions under one package
        if (!clientUid || (!DEV_USERNAMES.has(clients.get(clientUid)?.username?.toLowerCase()) && !_isLeadAdmin(clientUid))) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const pkgSubmitter = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        try {
          const pkg = stmtPkgGet.get(msg.packageId);
          if (!pkg || pkg.creator !== pkgSubmitter) {
            send(ws, { type:"dev_pkg_submit_result", ok:false, error:"Package not found or not yours." }); break;
          }
          if (pkg.status !== "draft") {
            send(ws, { type:"dev_pkg_submit_result", ok:false, error:"Package is already submitted." }); break;
          }
          // Submit each item in the package as an individual submission
          const items = msg.items || []; // [{category, entityName, entityId, subType, version, data, aiAnalysis}]
          const submittedIds = [];
          const failedItems = [];
          for (const item of items) {
            try {
              const subId = stmtDevInsertSub.run(
                pkgSubmitter, item.subType||"new", item.category||"misc",
                item.entityName||"Unnamed", item.entityId||null,
                item.version||1, "pending",
                JSON.stringify(item.data||{}),
                item.aiAnalysis ? JSON.stringify(item.aiAnalysis) : null,
                now
              ).lastInsertRowid;
              stmtPkgItemInsert.run(msg.packageId, subId, item.entityName, item.category, "pending");
              submittedIds.push(subId);
              stmtDevAudit.run(pkgSubmitter, "pkg_submit_item", item.category, item.entityName, item.version||1, now);
            } catch(itemErr) {
              failedItems.push({ name: item.entityName, error: itemErr.message });
            }
          }
          // Mark package as pending
          db.prepare(`UPDATE dev_packages SET status=?,submitted_at=?,updated_at=? WHERE id=?`)
            .run("pending", now, now, msg.packageId);
          stmtDevAudit.run(pkgSubmitter, "submit_package", "package", pkg.name, 1, now);
          // Notify lead admins
          for (const [uid, c] of clients) {
            if (LEAD_ADMINS.has((c.username||"").toLowerCase()) && c.ws.readyState === 1) {
              send(c.ws, { type:"dev_new_package", packageId:msg.packageId, name:pkg.name, creator:pkgSubmitter, itemCount:submittedIds.length });
            }
          }
          send(ws, { type:"dev_pkg_submit_result", ok:true, packageId:msg.packageId, submittedCount:submittedIds.length, failedItems });
        } catch(e) { send(ws, { type:"dev_pkg_submit_result", ok:false, error:e.message }); }
        break;
      }

      case "dev_get_packages": {
        if (!clientUid || (!DEV_USERNAMES.has(clients.get(clientUid)?.username?.toLowerCase()) && !_isLeadAdmin(clientUid))) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        try {
          const pkgs = stmtPkgGetAll.all();
          // Attach items to each package
          const result = pkgs.map(function(p) {
            const items = stmtPkgItemsGet.all(p.id);
            // Enrich items with submission data
            const enriched = items.map(function(item) {
              if (item.submission_id) {
                try {
                  const sub = stmtDevGetSub.get(item.submission_id);
                  if (sub) return Object.assign({}, item, { sub_status: sub.status, reject_reason: item.reject_reason || sub.reject_reason });
                } catch(e) {}
              }
              return item;
            });
            return Object.assign({}, p, { items: enriched });
          });
          send(ws, { type:"dev_packages_result", packages: result });
        } catch(e) { send(ws, { type:"dev_packages_result", packages:[], error:e.message }); }
        break;
      }

      case "dev_approve_package": {
        // Lead admin: approve entire package at once
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const pkgApprover = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        try {
          const pkg = stmtPkgGet.get(msg.packageId);
          if (!pkg) { send(ws, { type:"dev_pkg_approve_result", ok:false, error:"Package not found." }); break; }
          const items = stmtPkgItemsGet.all(msg.packageId);
          const results = [];
          // Collect all changes across the package — broadcast ONE delta at the end
          // instead of catalog_versions (which would cause every client to re-request
          // the full catalog, creating a thundering herd at scale).
          const _pkgDelta = {
            upsertedItems: [], deletedItems: [],
            upsertedZones: [], deletedZones: [],
            upsertedHostiles: [], deletedHostiles: [],
          };
          for (const item of items) {
            if (item.status !== "pending") { results.push({id:item.id, skipped:true}); continue; }
            try {
              const sub = stmtDevGetSub.get(item.submission_id);
              if (!sub || sub.status !== "pending") { results.push({id:item.id, skipped:true}); continue; }
              // Approve the underlying submission (reuse approval logic inline)
              const subData = sub.data || '{}';
              let subName = sub.entity_name || '';
              let parsedSubData = {};
              try { parsedSubData = JSON.parse(subData); if (parsedSubData.name) subName = parsedSubData.name; } catch(ep) {}
              const approvedVersion = sub.version || 1;
              let newEid = null; // hoisted so post-approval rebuild blocks can reference it safely
              if (sub.entity_id) {
                const canonicalId = parsedSubData.id || sub.entity_id;
                if (canonicalId !== sub.entity_id) {
                  stmtDeleteDevEntity.run(sub.entity_id);
                  stmtDevSaveEntity.run(canonicalId, sub.category||"misc", subName, approvedVersion, "live", "Active", subData, sub.creator||pkgApprover, now, pkgApprover, now);
                  stmtUpdateSubEntityId.run(canonicalId, sub.id);
                } else {
                  db.prepare(`UPDATE dev_entities SET status=?,name=?,data=?,version=?,updated_by=?,updated_at=? WHERE id=?`)
                    .run("live", subName, subData, approvedVersion, pkgApprover, now, sub.entity_id);
                }
              } else {
                newEid = parsedSubData.id || `${(sub.category||"misc").toLowerCase()}_${now}_${Math.random().toString(36).slice(2,7)}`;
                if (!parsedSubData.id) parsedSubData.id = newEid;
                const finalData = JSON.stringify(parsedSubData);
                stmtDevSaveEntity.run(newEid, sub.category||"misc", subName, approvedVersion, "live", "Active", finalData, sub.creator||pkgApprover, now, pkgApprover, now);
                stmtUpdateSubEntityIdAndData.run(newEid, finalData, sub.id);
              }
              stmtDevUpdateSubStatus.run("approved", pkgApprover, now, null, sub.id);
              stmtPkgItemUpdateStatus.run("approved", null, pkgApprover, now, item.id);
              stmtDevAudit.run(pkgApprover, "pkg_approve_item", sub.category, sub.entity_name, sub.version, now);
              // Surgical rebuild — collect into delta arrays.
              // Zone rebuilt once below (with hostile index reconciliation) — first block removed to prevent double-push.
              const _eid = sub.entity_id || newEid;
              if (sub.category === 'hostile' || sub.category === 'boss') {
                try { const _d = _rebuildOneHostile(_eid); if (_d) { _logCatalogChange(_d.hostile?.id||_d.deleted, sub.category, _d.deleted?'delete':'upsert'); _updateZoneHostileIndex(_d.hostile?.id||_d.deleted, !!_d.deleted); if (_d.hostile) _pkgDelta.upsertedHostiles.push(_d.hostile); if (_d.deleted) _pkgDelta.deletedHostiles.push(_d.deleted); if (_d.oldDeleted) _pkgDelta.deletedHostiles.push(_d.oldDeleted); } } catch(e) {}
              }
              if (sub.category === 'zone') {
                try {
                  const _zid2 = _eid; const _oldEn2 = new Set(ZONE_DB[_zid2]?.enemies || []);
                  const _d = _rebuildOneZone(_zid2);
                  if (_d) {
                    const _newEn2 = new Set(_d.zone?.enemies || []);
                    for (const eid of _oldEn2) { if (!_newEn2.has(eid)) _updateZoneHostileIndex(eid, false); }
                    for (const eid of _newEn2) { if (!_oldEn2.has(eid)) _updateZoneHostileIndex(eid, false); }
                    if (_d.deleted) for (const eid of _oldEn2) _updateZoneHostileIndex(eid, false);
                    _logCatalogChange(_d.zone?.id||_d.deleted, sub.category, _d.deleted?'delete':'upsert');
                    if (_d.zone) _pkgDelta.upsertedZones.push(_d.zone); if (_d.deleted) _pkgDelta.deletedZones.push(_d.deleted); if (_d.oldDeleted) _pkgDelta.deletedZones.push(_d.oldDeleted);
                  }
                } catch(e) {}
              }
              if (['equipment','action','material','provision','gears','accessories'].includes(sub.category)) {
                try { const _d = _rebuildOneItem(_eid); if (_d) { _logCatalogChange(_d.item?.id||_d.deleted, sub.category, _d.deleted?'delete':'upsert'); if (_d.item) _pkgDelta.upsertedItems.push(_buildSlimItem(_d.item)); if (_d.deleted) _pkgDelta.deletedItems.push(_d.deleted); if (_d.oldDeleted) _pkgDelta.deletedItems.push(_d.oldDeleted); } } catch(e) {}
              }
              results.push({id:item.id, ok:true});
            } catch(itemErr) { results.push({id:item.id, ok:false, error:itemErr.message}); }
          }
          // Broadcast ONE delta for the entire package — no thundering herd.
          // Zones → all clients. Hostiles + items → zone-scoped.
          try {
            if (_pkgDelta.upsertedZones.length || _pkgDelta.deletedZones.length) {
              _broadcastAll({ type:"catalog_delta",
                itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
                upsertedItems: [], deletedItems: [],
                upsertedZones: _pkgDelta.upsertedZones, deletedZones: _pkgDelta.deletedZones,
                upsertedHostiles: [], deletedHostiles: [],
              });
            }
            if (_pkgDelta.upsertedHostiles.length || _pkgDelta.deletedHostiles.length) {
              _broadcastHostileDelta(_pkgDelta.upsertedHostiles, _pkgDelta.deletedHostiles);
              _broadcastHostileToViewers(_pkgDelta.upsertedHostiles, _pkgDelta.deletedHostiles);
            }
            if (_pkgDelta.upsertedItems.length || _pkgDelta.deletedItems.length) {
              _broadcastItemDelta(_pkgDelta.upsertedItems, _pkgDelta.deletedItems);
              _broadcastItemToOwners(_pkgDelta.upsertedItems, _pkgDelta.deletedItems);
            }
          } catch(e) {}
          // Mark package approved
          stmtPkgUpdateStatus.run("approved", pkgApprover, now, null, now, msg.packageId);
          stmtDevAudit.run(pkgApprover, "approve_package", "package", pkg.name, 1, now);
          send(ws, { type:"dev_pkg_approve_result", ok:true, packageId:msg.packageId, results });
        } catch(e) { send(ws, { type:"dev_pkg_approve_result", ok:false, error:e.message }); }
        break;
      }

      case "dev_approve_package_item": {
        // Lead admin: approve a single item within a package
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const itemApprover = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        try {
          const item = stmtPkgItemGet.get(msg.itemId);
          if (!item) { send(ws, { type:"dev_pkg_item_result", ok:false, error:"Item not found." }); break; }
          const sub = stmtDevGetSub.get(item.submission_id);
          if (!sub) { send(ws, { type:"dev_pkg_item_result", ok:false, error:"Submission not found." }); break; }
          // Reuse full approval logic
          const subData = sub.data || '{}';
          let subName = sub.entity_name || '';
          let parsedSubData = {};
          try { parsedSubData = JSON.parse(subData); if (parsedSubData.name) subName = parsedSubData.name; } catch(ep) {}
          const approvedVersion = sub.version || 1;
          let newEid = null; // hoisted so post-approval rebuild blocks can reference it safely
          if (sub.entity_id) {
            const canonicalId = parsedSubData.id || sub.entity_id;
            if (canonicalId !== sub.entity_id) {
              stmtDeleteDevEntity.run(sub.entity_id);
              stmtDevSaveEntity.run(canonicalId, sub.category||"misc", subName, approvedVersion, "live", "Active", subData, sub.creator||itemApprover, now, itemApprover, now);
              stmtUpdateSubEntityId.run(canonicalId, sub.id);
            } else {
              db.prepare(`UPDATE dev_entities SET status=?,name=?,data=?,version=?,updated_by=?,updated_at=? WHERE id=?`)
                .run("live", subName, subData, approvedVersion, itemApprover, now, sub.entity_id);
            }
          } else {
            newEid = parsedSubData.id || `${(sub.category||"misc").toLowerCase()}_${now}_${Math.random().toString(36).slice(2,7)}`;
            if (!parsedSubData.id) parsedSubData.id = newEid;
            const finalData = JSON.stringify(parsedSubData);
            stmtDevSaveEntity.run(newEid, sub.category||"misc", subName, approvedVersion, "live", "Active", finalData, sub.creator||itemApprover, now, itemApprover, now);
            stmtUpdateSubEntityIdAndData.run(newEid, finalData, sub.id);
          }
          stmtDevUpdateSubStatus.run("approved", itemApprover, now, null, sub.id);
          stmtPkgItemUpdateStatus.run("approved", null, itemApprover, now, item.id);
          stmtDevAudit.run(itemApprover, "approve_pkg_item", sub.category, sub.entity_name, sub.version, now);
          if (sub.category === 'zone') {
            try {
              const _zid3 = sub.entity_id || newEid; const _oldEn3 = new Set(ZONE_DB[_zid3]?.enemies || []);
              const _d = _rebuildOneZone(_zid3);
              if (_d) {
                const _newEn3 = new Set(_d.zone?.enemies || []);
                for (const eid of _oldEn3) { if (!_newEn3.has(eid)) _updateZoneHostileIndex(eid, false); }
                for (const eid of _newEn3) { if (!_oldEn3.has(eid)) _updateZoneHostileIndex(eid, false); }
                if (_d.deleted) for (const eid of _oldEn3) _updateZoneHostileIndex(eid, false);
                _logCatalogChange(_d.zone?.id||_d.deleted, sub.category, _d.deleted?'delete':'upsert');
                const _upsertedZones3 = _d.zone ? [_d.zone] : [];
                const _deletedZones3  = [...(_d.deleted ? [_d.deleted] : []), ...(_d.oldDeleted ? [_d.oldDeleted] : [])];
                _broadcastAll({ type:"catalog_delta", itemsV: ITEM_DB_VERSION, zonesV: ZONE_DB_VERSION, hostilesV: ENEMY_DB_VERSION, actionsV: ACTION_DB_VERSION,
                  upsertedZones: _upsertedZones3, deletedZones: _deletedZones3,
                  upsertedItems: [], deletedItems: [], upsertedHostiles: [], deletedHostiles: [] });
              }
            } catch(e) {}
          }
          if (sub.category === 'hostile' || sub.category === 'boss') {
            try {
              const _hd = _rebuildOneHostile(sub.entity_id || newEid);
              if (_hd) {
                _logCatalogChange(_hd.hostile?.id||_hd.deleted, sub.category, _hd.deleted?'delete':'upsert');
                _updateZoneHostileIndex(_hd.hostile?.id || _hd.deleted, !!_hd.deleted);
                const _upserted3 = _hd.hostile ? [_hd.hostile] : [];
                const _deleted3  = [...(_hd.deleted ? [_hd.deleted] : []), ...(_hd.oldDeleted ? [_hd.oldDeleted] : [])];
                _broadcastHostileDelta(_upserted3, _deleted3);
                _broadcastHostileToViewers(_upserted3, _deleted3);
              }
            } catch(e) {}
          }
          if (['equipment','action','material','provision','gears','accessories'].includes(sub.category)) {
            try {
              const _d = _rebuildOneItem(sub.entity_id || newEid);
              if (_d) {
                _logCatalogChange(_d.item?.id||_d.deleted, sub.category, _d.deleted?'delete':'upsert');
                const _slimD = _d.item ? [_buildSlimItem(_d.item)].filter(Boolean) : [];
                const _delD  = [...(_d.deleted ? [_d.deleted] : []), ...(_d.oldDeleted ? [_d.oldDeleted] : [])];
                _broadcastItemDelta(_slimD, _delD);
                _broadcastItemToOwners(_slimD, _delD);
              }
            } catch(e) {}
          }
          // Check if all items in package are resolved → auto-mark package
          const allItems = stmtPkgItemsGet.all(item.package_id);
          const anyPending = allItems.some(i => i.status === 'pending');
          const anyRejected = allItems.some(i => i.status === 'rejected');
          if (!anyPending) {
            const pkgFinalStatus = anyRejected ? 'approved_partial' : 'approved';
            stmtPkgUpdateStatus.run(pkgFinalStatus, itemApprover, now, null, now, item.package_id);
          }
          send(ws, { type:"dev_pkg_item_result", ok:true, itemId:msg.itemId, packageId:item.package_id });
        } catch(e) { send(ws, { type:"dev_pkg_item_result", ok:false, error:e.message }); }
        break;
      }

      case "dev_reject_package_item": {
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const itemRejector = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        try {
          const item = stmtPkgItemGet.get(msg.itemId);
          if (!item) { send(ws, { type:"dev_pkg_item_result", ok:false, error:"Item not found." }); break; }
          const sub = stmtDevGetSub.get(item.submission_id);
          if (sub) {
            stmtDevUpdateSubStatus.run("rejected", itemRejector, now, msg.reason||"Rejected in package review.", sub.id);
            stmtDevAudit.run(itemRejector, "reject_pkg_item", sub.category, sub.entity_name, sub.version, now);
          }
          stmtPkgItemUpdateStatus.run("rejected", msg.reason||"No reason given.", itemRejector, now, item.id);
          // Auto-mark package if all resolved
          const allItems = stmtPkgItemsGet.all(item.package_id);
          const anyPending = allItems.some(i => i.status === 'pending' && i.id !== item.id);
          if (!anyPending) {
            stmtPkgUpdateStatus.run("approved_partial", itemRejector, now, null, now, item.package_id);
          }
          send(ws, { type:"dev_pkg_item_result", ok:true, itemId:msg.itemId, packageId:item.package_id });
        } catch(e) { send(ws, { type:"dev_pkg_item_result", ok:false, error:e.message }); }
        break;
      }

      case "dev_reject_package": {
        // Lead admin: reject entire package
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        const pkgRejector = clients.get(clientUid)?.username || clientUid;
        const now = Date.now();
        try {
          const pkg = stmtPkgGet.get(msg.packageId);
          if (!pkg) { send(ws, { type:"dev_pkg_reject_result", ok:false, error:"Package not found." }); break; }
          const items = stmtPkgItemsGet.all(msg.packageId);
          for (const item of items) {
            if (item.status !== "pending") continue;
            stmtPkgItemUpdateStatus.run("rejected", msg.reason||"Package rejected.", pkgRejector, now, item.id);
            if (item.submission_id) {
              try { stmtDevUpdateSubStatus.run("rejected", pkgRejector, now, msg.reason||"Package rejected.", item.submission_id); } catch(e) {}
            }
          }
          stmtPkgUpdateStatus.run("rejected", pkgRejector, now, msg.reason||null, now, msg.packageId);
          stmtDevAudit.run(pkgRejector, "reject_package", "package", pkg.name, 1, now);
          // Notify creator if online
          const creatorUid = _usernameToUid.get((pkg.creator||"").toLowerCase());
          if (creatorUid) {
            const cc = clients.get(creatorUid);
            if (cc && cc.ws.readyState === 1) {
              send(cc.ws, { type:"dev_package_rejected", packageName:pkg.name, reason:msg.reason||"No reason provided." });
            }
          }
          send(ws, { type:"dev_pkg_reject_result", ok:true, packageId:msg.packageId });
        } catch(e) { send(ws, { type:"dev_pkg_reject_result", ok:false, error:e.message }); }
        break;
      }

      case "dev_delete_package": {
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        try {
          const pkg = stmtPkgGet.get(msg.packageId);
          if (!pkg || pkg.status === "pending") { send(ws, { type:"dev_pkg_delete_result", ok:false, error:"Cannot delete a pending package." }); break; }
          stmtDeletePkgItems.run(msg.packageId);
          stmtDeletePkg.run(msg.packageId);
          send(ws, { type:"dev_pkg_delete_result", ok:true, packageId:msg.packageId });
        } catch(e) { send(ws, { type:"dev_pkg_delete_result", ok:false, error:e.message }); }
        break;
      }

      case "dev_dashboard": {
        // Dashboard stats (lead admin or dev)
        if (!clientUid || (!DEV_USERNAMES.has(clients.get(clientUid)?.username?.toLowerCase()) && !_isLeadAdmin(clientUid))) {
          send(ws, { type:"dev_action_fail", reason:"not_authorized" }); break;
        }
        try {
          const pending = stmtDevCountPending.get()?.c || 0;
          const cats = ["material","provision","gears","accessories","action","hostile","boss","zone","quest"];
          const totals = {};
          for (const cat of cats) {
            totals[cat] = (stmtGetDevEntityCount.get(cat)?.c || 0);
          }
          const recentAudit = stmtGetRecentAudit.all();
          const liveCounts  = {};
          for (const cat of cats) {
            liveCounts[cat] = (stmtGetDevEntityLiveCount.get(cat)?.c || 0);
          }
          send(ws, { type:"dev_dashboard_result", pending, totals, liveCounts, recentAudit });
        } catch(e) { send(ws, { type:"dev_dashboard_result", pending:0, totals:{}, liveCounts:{}, recentAudit:[], error:e.message }); }
        break;
      }


      // ── Admin: list snapshots for a player ──────────────────────────────
      case "admin_snapshots": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) break;
        const { charName: snapName } = msg;
        if (!snapName) { send(ws, { type:"admin_snapshots_result", rows:[], error:"no_name" }); break; }
        try {
          const cnRow = stmt.getCharname.get(snapName.toLowerCase());
          if (!cnRow) { send(ws, { type:"admin_snapshots_result", rows:[], error:"player_not_found" }); break; }
          const rows = stmt.getSnapshots.all(cnRow.uid);
          send(ws, { type:"admin_snapshots_result", charName: snapName, rows: rows.map(r => ({ id: r.id, reason: r.reason, ip: r.ip || "unknown", deviceId: r.device_id || "unknown", fingerprint: r.fingerprint || "unknown", ts: r.ts })) });
        } catch (e) { send(ws, { type:"admin_snapshots_result", rows:[], error: e.message }); }
        break;
      }

      // ── Admin: view transaction log for a player ───────────────────────
      case "admin_txlog": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) break;
        const { charName: txName, limit: txLimit } = msg;
        if (!txName) { send(ws, { type:"admin_txlog_result", rows:[], error:"no_name" }); break; }
        try {
          const cnRow = stmt.getCharname.get(txName.toLowerCase());
          if (!cnRow) { send(ws, { type:"admin_txlog_result", rows:[], error:"player_not_found" }); break; }
          const rows = stmt.getTxLog.all(cnRow.uid, Math.min(txLimit || 30, 100));
          send(ws, { type:"admin_txlog_result", charName: txName, rows });
        } catch (e) { send(ws, { type:"admin_txlog_result", rows:[], error: e.message }); }
        break;
      }

      // ── Lead Admin: restore a player's save from a snapshot ────────────
      case "admin_restore": {
        if (!clientUid || !_isLeadAdmin(clientUid)) { send(ws, { type:"admin_action_fail", action:"restore", reason:"not_authorized" }); break; }
        const { charName: restoreName, snapshotId } = msg;
        if (!restoreName) { send(ws, { type:"admin_action_fail", action:"restore", reason:"no_name" }); break; }
        try {
          const cnRow = stmt.getCharname.get(restoreName.toLowerCase());
          if (!cnRow) { send(ws, { type:"admin_action_fail", action:"restore", reason:"player_not_found" }); break; }
          const targetUid = cnRow.uid;
          let snapRow;
          if (snapshotId) {
            snapRow = stmt.getSnapshotById.get(snapshotId, targetUid);
            if (!snapRow) { send(ws, { type:"admin_action_fail", action:"restore", reason:"snapshot_not_found" }); break; }
          } else {
            const latest = stmt.getLatestSnapshot.get(targetUid);
            if (!latest) { send(ws, { type:"admin_action_fail", action:"restore", reason:"no_snapshots" }); break; }
            snapRow = latest;
          }
          _snapshotSave(targetUid, "pre_restore");
          const restoredSave = JSON.parse(snapRow.data);
          _writeSave(targetUid, restoredSave, { sync: true }); // restore — target may be offline
          // Invalidate password — force new password on next login
          const randomHash = _hashPassword(crypto.randomBytes(32).toString("hex"));
          stmt.setMustReset.run(randomHash, targetUid);
          _logTx(targetUid, "admin_restore", { by: clientUid, snapshotId: snapshotId || "latest", snapshotTs: snapRow.ts || 0, passwordReset: true });
          const targetClient = clients.get(targetUid);
          if (targetClient && targetClient.ws && targetClient.ws.readyState === 1) {
            send(targetClient.ws, { type:"save_restored", save: restoredSave, passwordReset: true });
          }
          send(ws, { type:"admin_action_ok", action:"restore", charName: restoreName, snapshotTs: snapRow.ts || 0 });
          console.log(`[ADMIN] Restored ${restoreName} (uid=${targetUid}) from snapshot ${snapshotId || "latest"} by uid=${clientUid}`);
        } catch (e) {
          send(ws, { type:"admin_action_fail", action:"restore", reason: e.message });
        }
        break;
      }

      // ── Lead Admin: set a player's password (for offline restore recovery) ──
      case "admin_set_password": {
        if (!clientUid || !_isLeadAdmin(clientUid)) { send(ws, { type:"admin_action_fail", action:"set_password", reason:"not_authorized" }); break; }
        const { charName: spName, password: spPw } = msg;
        if (!spName || !spPw) { send(ws, { type:"admin_action_fail", action:"set_password", reason:"missing_fields" }); break; }
        if (typeof spPw !== "string" || spPw.length < 6) { send(ws, { type:"admin_action_fail", action:"set_password", reason:"Password must be at least 6 characters." }); break; }
        try {
          const cnRow = stmt.getCharname.get(spName.toLowerCase());
          if (!cnRow) { send(ws, { type:"admin_action_fail", action:"set_password", reason:"player_not_found" }); break; }
          const hashedPw = _hashPassword(spPw);
          stmt.updateAuthPasswordKeepReset.run(hashedPw, cnRow.uid);
          _logTx(cnRow.uid, "admin_set_password", { by: clientUid });
          send(ws, { type:"admin_action_ok", action:"set_password", charName: spName });
          console.log(`[ADMIN] Password set for ${spName} by uid=${clientUid}`);
        } catch (e) {
          send(ws, { type:"admin_action_fail", action:"set_password", reason: e.message });
        }
        break;
      }

      // ── Lead Admin: permanently delete a character and their account ──
      case "admin_delete_account": {
        if (!clientUid || !_isLeadAdmin(clientUid)) { send(ws, { type:"admin_action_fail", action:"delete_account", reason:"not_authorized" }); break; }
        const { charName: delName } = msg;
        if (!delName) { send(ws, { type:"admin_action_fail", action:"delete_account", reason:"no_name" }); break; }
        try {
          const cnRow = stmt.getCharname.get(delName.toLowerCase());
          if (!cnRow) { send(ws, { type:"admin_action_fail", action:"delete_account", reason:"player_not_found" }); break; }
          const delUid = cnRow.uid;
          const delUsername = cnRow.username.toLowerCase();
          // Prevent deleting yourself
          if (delUid === clientUid) { send(ws, { type:"admin_action_fail", action:"delete_account", reason:"cannot_delete_self" }); break; }
          // Prevent deleting other lead admins
          if (LEAD_ADMINS.has(delUsername)) { send(ws, { type:"admin_action_fail", action:"delete_account", reason:"cannot_delete_lead_admin" }); break; }
          // Kick if online
          const delClient = clients.get(delUid);
          if (delClient && delClient.ws && delClient.ws.readyState === 1) {
            send(delClient.ws, { type:"kicked", reason:"account_deleted" });
            delClient.ws.close();
          }
          clients.delete(delUid);
          // Remove from admin sets if applicable
          ADMIN_UIDS.delete(delUid);
          ADMIN_USERNAMES.delete(delUsername);
          stmtRemoveAdmin.run(delUsername);
          // Remove from all caches
          _saveCache.delete(delUid);
          _usernameToUid.delete(delUsername);
          _expectedGold.delete(delUid);
          _clearItemOwners(delUid);
          _clearHostileViewers(delUid);
          // Guild cleanup
          const delGuildMem = stmt.getMemberGuild.get(delUid);
          if (delGuildMem) {
            const delGuildId = delGuildMem.guild_id;
            if (delGuildMem.role === "leader") {
              const delGuildAll = stmt.getGuildMembers.all(delGuildId);
              delGuildAll.forEach(m => {
                if (m.uid !== delUid) _sendGuildLeft(m.uid, "dissolved");
                stmt.deleteGuildMember.run(delGuildId, m.uid);
              });
              stmt.deleteGuild.run(delGuildId);
            } else {
              stmt.deleteGuildMember.run(delGuildId, delUid);
              _broadcastGuildUpdate(delGuildId);
            }
          }
          // Purge from every table
          stmt.deleteSave.run(delUid);
          stmt.deleteAccount.run(delUsername);
          stmt.deleteCharname.run(delUid);
          stmt.deleteSocial.run(delUid);
          stmt.deleteProfile.run(delUid);
          stmt.deleteInboxAll.run(delUid);
          stmt.deleteDmsAll.run(delUid);
          stmt.deletePresence.run(delUsername);
          stmt.deleteZonePresenceAll.run(delUsername);
          stmt.deleteAnomalies.run(delUid);
          stmt.deleteAuth.run(delUid);
          stmt.deleteSnapshots.run(delUid);
          stmt.deleteTxLog.run(delUid);
          stmt.deleteZoneChat.run(delUid);
          send(ws, { type:"admin_action_ok", action:"delete_account", charName: delName, username: delUsername });
          console.log(`[ADMIN] DELETED account ${delName} (${delUsername}, uid=${delUid}) by uid=${clientUid}`);
        } catch (e) {
          send(ws, { type:"admin_action_fail", action:"delete_account", reason: e.message });
        }
        break;
      }

      case "admin_wipe_db": {
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"admin_wipe_db_fail", reason:"not_authorized" });
          break;
        }
        try {
          console.log(`[WIPE] Full database wipe initiated by uid=${clientUid}`);

          // ── Step 1: Wipe the database first ──────────────────────────────
          const _wTables = ["saves","accounts","charnames","social","profiles","inbox","dms",
                            "parties","party_votes","zonechat","presence","zone_presence",
                            "anomalies","save_snapshots","transaction_log",
                            "combat_rooms","auth_credentials","admins"];
          db.transaction(() => {
            for (const t of _wTables) {
              try { db.prepare(`DELETE FROM ${t}`).run(); console.log(`[WIPE] cleared ${t}`); }
              catch(e) { console.warn(`[WIPE] skip ${t}: ${e.message}`); }
            }
          })();
          db.prepare("VACUUM").run();
          console.log("[WIPE] ✓ Database cleared.");

          // ── Step 2: Clear ALL in-memory caches ───────────────────────────
          _saveCache.clear();
          _usernameToUid.clear();
          _uidToFriends.clear(); _friendsOfMe.clear();
          _expectedGold.clear();

          // ── Step 3: Notify all clients then close connections ────────────
          const _wipeClients = [...clients.values()];
          clients.clear(); // clear map now so no new messages are processed
          for (const c of _wipeClients) {
            try { send(c.ws, { type:"kicked", reason:"server_wipe" }); } catch(e){}
          }
          // Close connections after 400ms so kicked message is received
          setTimeout(() => {
            for (const c of _wipeClients) {
              try { c.ws.close(); } catch(e){}
            }
            console.log("[WIPE] ✓ All clients disconnected. Re-register as viddle to restore lead admin.");
          }, 400);

        } catch(e) {
          console.error("[WIPE] ERROR:", e.message);
          try { send(ws, { type:"admin_wipe_db_fail", reason: e.message }); } catch(_){}
        }
        break;
      }

      case "save_audit": {
        if (!clientUid) return;
        const { gold, hp, maxHp } = msg;
        try {
          const save = _getCachedSave(clientUid);
          if (!save) return;
          const serverP = save.player || {};
          if (gold != null && serverP.gold != null) {
            const expectedG = _expectedGold.has(clientUid) ? _expectedGold.get(clientUid) : serverP.gold;
            if (gold > expectedG) flagAnomaly(clientUid, "gold_discrepancy", { clientGold: gold, serverGold: serverP.gold, expected: expectedG });
          }
          _setExpectedGold(clientUid, serverP.gold || 0);
          const result = _validateSave(clientUid, save);
          if (!result.valid) {
            result.anomalies.forEach(a => flagAnomaly(clientUid, a.reason, a));
            if (Object.keys(result.fixes).length > 0) {
              _applyFixes(save, result.fixes);
              _writeSave(clientUid, save);
              console.log(`[ANTI-CHEAT] reverted ${Object.keys(result.fixes).length} fields for uid=${clientUid}`);
              send(ws, { type:"save_corrected", fields: Object.keys(result.fixes) });
            }
          }
        } catch (e) { console.error("[AUDIT] error:", e.message); }
        break;
      }

      case "debug_db_info": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        try {
          const accounts = db.prepare("SELECT COUNT(*) as c FROM auth_credentials").get().c;
          const saves = db.prepare("SELECT COUNT(*) as c FROM saves").get().c;
          let size = "unknown";
          try { const s = fs.statSync(DB_PATH); size = (s.size / 1024).toFixed(1) + " KB"; } catch {}
          send(ws, { type:"debug_db_info", dbPath: DB_PATH, envSet: !!process.env.DB_PATH, size, accounts, saves });
        } catch (e) {
          send(ws, { type:"debug_db_info", dbPath: DB_PATH, envSet: !!process.env.DB_PATH, size:"error", accounts:0, saves:0 });
        }
        break;
      }
      // ── Admin SQL query - LEAD ADMIN ONLY, read-only ─────────────────
      case "admin_sql": {
        if (!clientUid || !_isLeadAdmin(clientUid)) { send(ws, { type:"admin_sql_result", error:"not_authorized" }); break; }
        const { query: sqlQuery } = msg;
        if (!sqlQuery || typeof sqlQuery !== "string") { send(ws, { type:"admin_sql_result", error:"no_query" }); break; }
        // Block write operations and auth table access
        const upper = sqlQuery.trim().toUpperCase();
        if (!upper.startsWith("SELECT") && !upper.startsWith("PRAGMA")) {
          send(ws, { type:"admin_sql_result", error:"read_only", message:"Only SELECT and PRAGMA queries allowed." }); break;
        }
        if (upper.includes("AUTH")) {
          send(ws, { type:"admin_sql_result", error:"blocked", message:"Auth table access is blocked." }); break;
        }
        try {
          const rows = db.prepare(sqlQuery).all();
          const limited = rows.slice(0, 50);
          send(ws, { type:"admin_sql_result", rows: limited, total: rows.length, truncated: rows.length > 50 });
        } catch (e) {
          send(ws, { type:"admin_sql_result", error: e.message });
        }
        break;
      }

      case "admin_terminal": {
        if (!clientUid || !_isLeadAdmin(clientUid)) {
          send(ws, { type:"admin_terminal_result", id: msg.id, error:"not_authorized" }); break;
        }
        const cmd = (msg.cmd || "").trim();
        const cmdId = msg.id || null;
        const _termReply = (output, error) => send(ws, { type:"admin_terminal_result", id:cmdId, output, error:error||null });

        // ── Parse command + args ──────────────────────────────────────────────
        const parts = cmd.match(/(?:[^\s"]+|"[^"]*")+/g) || [];
        const base = (parts[0]||"").toLowerCase();
        const args = parts.slice(1).map(a=>a.replace(/^"|"$/g,""));

        try {
          switch(base) {

            // ── systeminfo

            // ── systeminfo ────────────────────────────────────────────────────
            case "systeminfo": {
              const mem = process.memoryUsage();
              const upSecs = Math.floor(process.uptime());
              const h = Math.floor(upSecs/3600), m = Math.floor((upSecs%3600)/60), s = upSecs%60;
              const toMB = b => (b/1024/1024).toFixed(1)+"MB";
              _termReply([
                `Platform   : ${os.platform()} ${os.arch()}`,
                `Hostname   : ${os.hostname()}`,
                `Node.js    : ${process.version}`,
                `Uptime     : ${h}h ${m}m ${s}s`,
                `CPU cores  : ${os.cpus().length} x ${os.cpus()[0]?.model||"unknown"}`,
                `Load avg   : ${os.loadavg().map(l=>l.toFixed(2)).join(" ")}`,
                `Free mem   : ${toMB(os.freemem())} / ${toMB(os.totalmem())}`,
                `Heap used  : ${toMB(mem.heapUsed)} / ${toMB(mem.heapTotal)}`,
                `RSS        : ${toMB(mem.rss)}`,
                `External   : ${toMB(mem.external)}`,
                `PID        : ${process.pid}`,
              ]);
              break;
            }

            // ── uptime ────────────────────────────────────────────────────────
            case "uptime": {
              const upSecs = Math.floor(process.uptime());
              const h = Math.floor(upSecs/3600), m = Math.floor((upSecs%3600)/60), s = upSecs%60;
              _termReply([`Server uptime: ${h}h ${m}m ${s}s (${upSecs}s)`]);
              break;
            }

            // ── memory ────────────────────────────────────────────────────────
            case "memory": {
              const mem = process.memoryUsage();
              const toMB = b => (b/1024/1024).toFixed(2)+"MB";
              _termReply([
                `Heap Used  : ${toMB(mem.heapUsed)}`,
                `Heap Total : ${toMB(mem.heapTotal)}`,
                `RSS        : ${toMB(mem.rss)}`,
                `External   : ${toMB(mem.external)}`,
                `Free OS    : ${toMB(os.freemem())}`,
                `Total OS   : ${toMB(os.totalmem())}`,
              ]);
              break;
            }

            // ── version ───────────────────────────────────────────────────────
            case "version": {
              _termReply([
                `Node.js    : ${process.version}`,
                `Platform   : ${os.platform()} ${os.arch()}`,
                `PID        : ${process.pid}`,
              ]);
              break;
            }

            // ── environment ───────────────────────────────────────────────────
            case "environment": {
              const safe = ["NODE_ENV","PORT","DB_PATH","FLY_REGION","FLY_APP_NAME","FLY_ALLOC_ID"];
              _termReply(safe.map(k=>`${k.padEnd(16)}: ${process.env[k]||"(not set)"}`));
              break;
            }

            // ── sql ───────────────────────────────────────────────────────────
            case "sql": {
              const q = args.join(" ");
              if(!q) { _termReply(null,"Usage: sql <query>"); break; }
              const upper = q.trim().toUpperCase();
              if(!upper.startsWith("SELECT")&&!upper.startsWith("PRAGMA")&&!upper.startsWith("EXPLAIN")) {
                _termReply(null,"Only SELECT, PRAGMA and EXPLAIN queries allowed."); break;
              }
              if(upper.includes("AUTH")) { _termReply(null,"Auth table access is blocked."); break; }
              const rows = db.prepare(q).all();
              if(!rows.length) { _termReply(["(no rows)"]); break; }
              const cols = Object.keys(rows[0]);
              const widths = cols.map(c=>Math.max(c.length, ...rows.slice(0,100).map(r=>String(r[c]??"").length)));
              const header = cols.map((c,i)=>c.padEnd(widths[i])).join(" | ");
              const divider = widths.map(w=>"-".repeat(w)).join("-+-");
              const lines = [header, divider, ...rows.slice(0,200).map(r=>cols.map((c,i)=>String(r[c]??"").padEnd(widths[i])).join(" | "))];
              if(rows.length>200) lines.push(`... (${rows.length} total, showing 200)`);
              _termReply(lines);
              break;
            }

            // ── tables ────────────────────────────────────────────────────────
            case "tables": {
              const rows = db.prepare("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name").all();
              _termReply(rows.map(r=>`${r.type.padEnd(6)} ${r.name}`));
              break;
            }

            // ── schema ────────────────────────────────────────────────────────
            case "schema": {
              const tbl = args[0];
              if(!tbl) { _termReply(null,"Usage: schema <table>"); break; }
              const rows = db.prepare(`PRAGMA table_info(${tbl})`).all();
              if(!rows.length) { _termReply(null,`Table "${tbl}" not found.`); break; }
              _termReply(rows.map(r=>`${String(r.cid).padStart(2)}  ${r.name.padEnd(24)} ${r.type.padEnd(16)} ${r.notnull?"NOT NULL":"        "} ${r.dflt_value!=null?"DEFAULT "+r.dflt_value:""}`));
              break;
            }

            // ── count ─────────────────────────────────────────────────────────
            case "count": {
              const tbl = args[0];
              if(!tbl) { _termReply(null,"Usage: count <table>"); break; }
              try {
                const r = db.prepare(`SELECT COUNT(*) as n FROM ${tbl}`).get();
                _termReply([`${tbl}: ${r.n} rows`]);
              } catch(e) { _termReply(null,e.message); }
              break;
            }

            // ── dbsize ────────────────────────────────────────────────────────
            case "dbsize": {
              try {
                const stat = fs.statSync(DB_PATH);
                const kb = (stat.size/1024).toFixed(1);
                const mb = (stat.size/1024/1024).toFixed(3);
                _termReply([`Database: ${DB_PATH}`,`Size    : ${kb} KB (${mb} MB)`]);
              } catch(e) { _termReply(null,e.message); }
              break;
            }

            // ── players ───────────────────────────────────────────────────────
            case "players": {
              // Build online uid set for presence detection
              const onlineUids = new Set([...clients.values()].filter(c=>c.uid).map(c=>c.uid));
              // Pull every account that has ever been created, joined with save data
              const allAccRows = db.prepare(
                "SELECT a.uid, a.charName, a.username, s.data FROM accounts a LEFT JOIN saves s ON s.uid=a.uid ORDER BY a.charName COLLATE NOCASE"
              ).all();
              if(!allAccRows.length) { _termReply(["No players found in database."]); break; }
              const entries = allAccRows.map(row => {
                let playtime = 0, isSub = false;
                try {
                  const save = JSON.parse(row.data||"{}");
                  playtime = save.player?.totalPlaytime || 0;
                  isSub    = !!(save.player?.isSubscribed);
                } catch(e){}
                // Add current session time for players still connected
                if (onlineUids.has(row.uid)) {
                  const liveClient = clients.get(row.uid);
                  if (liveClient && liveClient.loginAt) {
                    playtime += Math.floor((Date.now() - liveClient.loginAt) / 1000);
                  }
                }
                return {
                  charName: row.charName || "?",
                  username: row.username || "?",
                  playtime,
                  isSub,
                  online: onlineUids.has(row.uid),
                };
              });
              // Sort: subscribed first, then within each group by playtime descending
              entries.sort((a,b) => {
                if(a.isSub !== b.isSub) return a.isSub ? -1 : 1;
                return b.playtime - a.playtime;
              });
              const subCount    = entries.filter(e=>e.isSub).length;
              const onlineCount = entries.filter(e=>e.online).length;
              const lines = [
                `${entries.length} total player(s)  —  ${onlineCount} online  |  ${subCount} subscribed`,
              ];
              // Pre-compute d/h/m for each entry so we can size columns from real data
              const parsed = entries.map(e => {
                const d = Math.floor(e.playtime/86400);
                const h = Math.floor((e.playtime%86400)/3600);
                const m = Math.floor((e.playtime%3600)/60);
                return { ...e, d, h, m };
              });
              const maxName = Math.max(8, ...parsed.map(e => (e.charName||'').length));
              const maxD    = Math.max(2, ...parsed.map(e => String(e.d).length));
              const maxH    = Math.max(2, ...parsed.map(e => String(e.h).length));
              const maxM    = Math.max(2, ...parsed.map(e => String(e.m).length));
              let lastSub = null;
              for(const e of parsed) {
                if(lastSub !== e.isSub) {
                  lines.push("");
                  lines.push(e.isSub ? "[ SUBSCRIBED ]" : "[ NON-SUBSCRIBED ]");
                  lastSub = e.isSub;
                }
                const status = e.online ? "● ONLINE " : "○ OFFLINE";
                const dCol = String(e.d).padStart(maxD) + 'd';
                const hCol = String(e.h).padStart(maxH) + 'h';
                const mCol = String(e.m).padStart(maxM) + 'm';
                lines.push(`  ${(e.charName||'?').padEnd(maxName)}  ${status}  ${dCol} ${hCol} ${mCol}`);
              }
              _termReply(lines);
              break;
            }

            // ── player ────────────────────────────────────────────────────────
            case "player": {
              const pname = args[0];
              if(!pname) { _termReply(null,"Usage: player <charname>"); break; }
              const accRow = db.prepare("SELECT uid, username, zone FROM accounts WHERE LOWER(charName)=?").get(pname.toLowerCase());
              if(!accRow) { _termReply(null,`Player "${pname}" not found.`); break; }
              const row = db.prepare("SELECT uid, data FROM saves WHERE uid=?").get(accRow.uid);
              if(!row) { _termReply(null,`No save data for "${pname}".`); break; }
              const save = JSON.parse(row.data||"{}");
              const p = save.player||{};
              const eq = p.equipment||{};
              // Zone: check all storage locations in priority order
              const zone = save["p/lz"] || p.lastZone || accRow.zone || "?";
              const isOnline = [...clients.values()].some(c=>(c.username||"").toLowerCase()===accRow.username.toLowerCase());
              const ptH = Math.floor((p.totalPlaytime||0)/3600);
              const ptM = Math.floor(((p.totalPlaytime||0)%3600)/60);
              _termReply([
                `Status     : ${isOnline ? "● ONLINE" : "○ OFFLINE"}`,
                `UID        : ${row.uid}`,
                `Account    : ${accRow.username||"?"}`,
                `Name       : ${p.name||"?"}`,
                `Title      : ${p.title||"?"}`,
                `Subscribed : ${p.isSubscribed ? "YES" : "no"}`,
                `HP         : ${p.hp}/${p.maxHp}`,
                `Gold       : ${p.gold||0}`,
                `Zone       : ${zone}`,
                `Respawn    : ${p.respawnZone||"?"}`,
                `Gear     : ${eq.gear?.name||"none"}`,
                `Accessories: ${(eq.accessories||[]).filter(Boolean).map(a=>a.name).join(", ")||"none"}`,
                `Actions    : ${(p.actionSlots||[]).filter(Boolean).join(", ")||"none"}`,
                `Playtime   : ${ptH}h ${ptM}m`,
              ]);
              break;
            }

            // ── flags ─────────────────────────────────────────────────────────
            case "flags": {
              const fname = args[0];
              let rows;
              if(fname) {
                const frow = db.prepare("SELECT uid FROM saves WHERE username=? COLLATE NOCASE").get(fname);
                if(!frow) { _termReply(null,`Player "${fname}" not found.`); break; }
                rows = db.prepare("SELECT uid, reason, details, ts FROM anomalies WHERE uid=? ORDER BY ts DESC LIMIT 50").all(frow.uid);
              } else {
                rows = db.prepare("SELECT uid, reason, details, ts FROM anomalies ORDER BY ts DESC LIMIT 50").all();
              }
              if(!rows.length) { _termReply(["No flags found."]); break; }
              _termReply(rows.map(r=>{
                const d = r.details ? JSON.parse(r.details) : {};
                const detail = d.path ? ` path=${d.path}` : d.reason ? ` ${d.reason}` : "";
                return `${new Date(r.ts).toLocaleString().padEnd(22)} ${r.reason.padEnd(28)}${detail}`;
              }));
              break;
            }

            // ── clearflags ────────────────────────────────────────────────────
            case "clearflags": {
              const cfname = args[0];
              if(!cfname) { _termReply(null,"Usage: clearflags <name>"); break; }
              const cfrow = db.prepare("SELECT uid FROM saves WHERE username=? COLLATE NOCASE").get(cfname);
              if(!cfrow) { _termReply(null,`Player "${cfname}" not found.`); break; }
              const r = db.prepare("DELETE FROM anomalies WHERE uid=?").run(cfrow.uid);
              _termReply([`Cleared ${r.changes} flag(s) for ${cfname}.`]);
              break;
            }

            // ── kick ──────────────────────────────────────────────────────────
            case "kick": {
              const kname = args[0];
              if(!kname) { _termReply(null,"Usage: kick <charname>"); break; }
              // Resolve charname → username
              const kickAcc = db.prepare("SELECT username FROM accounts WHERE LOWER(charName)=?").get(kname.toLowerCase());
              const kickUser = kickAcc ? kickAcc.username.toLowerCase() : kname.toLowerCase();
              let kicked = false;
              for(const [uid, c] of clients.entries()) {
                if((c.username||"").toLowerCase()===kickUser) {
                  if(c.ws&&c.ws.readyState===1) {
                    send(c.ws, { type:"system_message", message:"You have been disconnected by an administrator." });
                    c.ws.close();
                  }
                  kicked = true;
                }
              }
              _termReply([kicked ? `${kname} has been kicked.` : `${kname} is not online.`]);
              break;
            }

            // ── ban ───────────────────────────────────────────────────────────
            case "ban": {
              const banname = args[0];
              if(!banname) { _termReply(null,"Usage: ban <charname>"); break; }
              // Resolve charname → username
              const banAcc = db.prepare("SELECT username FROM accounts WHERE LOWER(charName)=?").get(banname.toLowerCase());
              const banUsername = (banAcc ? banAcc.username : banname).toLowerCase();
              if(LEAD_ADMINS.has(banUsername)) { _termReply(null,"Cannot ban the lead admin."); break; }
              const banrow = db.prepare("SELECT uid FROM accounts WHERE LOWER(username)=?").get(banUsername);
              if(!banrow) { _termReply(null,`Player "${banname}" not found.`); break; }
              db.prepare("UPDATE auth_credentials SET banned=1 WHERE LOWER(username)=?").run(banUsername);
              for(const [uid, c] of clients.entries()) {
                if((c.username||"").toLowerCase()===banUsername&&c.ws&&c.ws.readyState===1) {
                  send(c.ws,{type:"auth_fail",reason:"banned"});c.ws.close();
                }
              }
              _termReply([`${banname} has been banned.`]);
              break;
            }

            // ── unban ─────────────────────────────────────────────────────────
            case "unban": {
              const ubanname = args[0];
              if(!ubanname) { _termReply(null,"Usage: unban <name>"); break; }
              const r = db.prepare("UPDATE auth SET banned=0 WHERE username=? COLLATE NOCASE").run(ubanname);
              _termReply([r.changes ? `${ubanname} has been unbanned.` : `${ubanname} not found in auth.`]);
              break;
            }

            // ── warn ──────────────────────────────────────────────────────────
            case "warn": {
              const wname = args[0]; const wmsg = args.slice(1).join(" ");
              if(!wname||!wmsg) { _termReply(null,"Usage: warn <name> <message>"); break; }
              let warned = false;
              for(const [uid, c] of clients.entries()) {
                if((c.username||"").toLowerCase()===wname.toLowerCase()&&c.ws&&c.ws.readyState===1) {
                  send(c.ws,{type:"admin_warning",message:wmsg});
                  warned = true;
                }
              }
              _termReply([warned ? `Warning sent to ${wname}: "${wmsg}"` : `${wname} is not online. Warning not delivered.`]);
              break;
            }

            // ── broadcast ─────────────────────────────────────────────────────
            case "broadcast": {
              const bmsg = args.join(" ");
              if(!bmsg) { _termReply(null,"Usage: broadcast <message>"); break; }
              let count = 0;
              for(const [uid, c] of clients.entries()) {
                if(c.ws&&c.ws.readyState===1) {
                  send(c.ws,{type:"system_message",message:`[BROADCAST] ${bmsg}`});
                  count++;
                }
              }
              _termReply([`Broadcast sent to ${count} client(s): "${bmsg}"`]);
              break;
            }

            // ── clients ───────────────────────────────────────────────────────
            case "clients": {
              const cl = [...clients.entries()];
              if(!cl.length) { _termReply(["No clients connected."]); break; }
              _termReply([
                `${cl.length} client(s):`,
                "UID                      USERNAME",
                ...cl.map(([uid,c])=>`${uid.slice(0,24).padEnd(26)} ${c.username||"(auth pending)"}`)
              ]);
              break;
            }

            // ── rooms ─────────────────────────────────────────────────────────
            case "rooms": {
              const roomList = [...rooms.entries()];
              if(!roomList.length) { _termReply(["No active combat rooms."]); break; }
              _termReply([
                `${roomList.length} active room(s):`,
                `${"TYPE".padEnd(12)}${"MEMBERS".padEnd(9)}${"ENEMIES".padEnd(9)}NAME`,
                "─".repeat(48),
                ...roomList.map(([id,r])=>{
                  const rtype = r.isPvP ? "PvP" : (r._subzone ? (r._subzone.subzoneType||"combat").toUpperCase() : "COMBAT");
                  const name  = r._subzone ? (r._subzone.subzoneName||id) : id.slice(0,24);
                  return `${rtype.padEnd(12)}${String((r.members||[]).length).padEnd(9)}${String((r.enemies||[]).length).padEnd(9)}${name}`;
                })
              ]);
              break;
            }

            // ── stats ─────────────────────────────────────────────────────────
            case "stats": {
              const mem = process.memoryUsage();
              const toMB = b=>(b/1024/1024).toFixed(1)+"MB";
              const upSecs = Math.floor(process.uptime());
              const h=Math.floor(upSecs/3600),m2=Math.floor((upSecs%3600)/60),s=upSecs%60;
              let dbSize = "?";
              try { dbSize = (fs.statSync(DB_PATH).size/1024).toFixed(0)+" KB"; } catch{}
              _termReply([
                `Uptime     : ${h}h ${m2}m ${s}s`,
                `Clients    : ${clients.size}`,
                `Heap       : ${toMB(mem.heapUsed)} / ${toMB(mem.heapTotal)}`,
                `RSS        : ${toMB(mem.rss)}`,
                `DB Size    : ${dbSize}`,
                `Load avg   : ${os.loadavg().map(l=>l.toFixed(2)).join(" / ")}`,
              ]);
              break;
            }

            // ── setpassword ───────────────────────────────────────────────────
            case "setpassword": {
              // setpassword <charname> <newpassword>
              const spName = args[0]; const spPw = args.slice(1).join(" ");
              if(!spName||!spPw) { _termReply(null,"Usage: setpassword <charname> <newpassword>"); break; }
              if(spPw.length < 6) { _termReply(null,"Password must be at least 6 characters."); break; }
              try {
                const cnRow = stmt.getCharname.get(spName.toLowerCase());
                if(!cnRow) { _termReply(null,`Character "${spName}" not found.`); break; }
                const hashed = _hashPassword(spPw);
                stmt.updateAuthPasswordKeepReset.run(hashed, cnRow.uid);
                _logTx(cnRow.uid, "admin_set_password", { by: clientUid, terminal: true });
                _termReply([`Password updated for ${spName}.`]);
              } catch(e) { _termReply(null, e.message); }
              break;
            }

            // ── forcepasswordreset ────────────────────────────────────────────
            case "forcereset": {
              // forcereset <charname> — marks account must_reset so next login forces change
              const frName = args[0];
              if(!frName) { _termReply(null,"Usage: forcereset <charname>"); break; }
              try {
                const cnRow = stmt.getCharname.get(frName.toLowerCase());
                if(!cnRow) { _termReply(null,`Character "${frName}" not found.`); break; }
                const tempPw = _hashPassword(crypto.randomBytes(8).toString("hex"));
                stmt.setMustReset.run(tempPw, cnRow.uid);
                _termReply([`${frName} will be forced to reset their password on next login.`]);
              } catch(e) { _termReply(null, e.message); }
              break;
            }

            // ── makeadmin ─────────────────────────────────────────────────────
            case "makeadmin": {
              const maName = args[0];
              if(!maName) { _termReply(null,"Usage: makeadmin <username>"); break; }
              try {
                const maRow = stmt.getAccount.get(maName.toLowerCase());
                if(!maRow) { _termReply(null,`Account "${maName}" not found.`); break; }
                if(ADMIN_USERNAMES.has(maName.toLowerCase())) { _termReply([`${maName} is already an admin.`]); break; }
                stmtAddAdmin.run(maName.toLowerCase());
                ADMIN_USERNAMES.add(maName.toLowerCase());
                // If online, upgrade their session
                for(const [uid,c] of clients.entries()) {
                  if((c.username||"").toLowerCase()===maName.toLowerCase()) { ADMIN_UIDS.add(uid); break; }
                }
                _termReply([`${maName} promoted to admin.`]);
              } catch(e) { _termReply(null, e.message); }
              break;
            }

            // ── removeadmin ───────────────────────────────────────────────────
            case "removeadmin": {
              const raName = args[0];
              if(!raName) { _termReply(null,"Usage: removeadmin <username>"); break; }
              if(LEAD_ADMINS.has(raName.toLowerCase())) { _termReply(null,"Cannot remove the lead admin."); break; }
              try {
                stmtRemoveAdmin.run(raName.toLowerCase());
                ADMIN_USERNAMES.delete(raName.toLowerCase());
                for(const [uid,c] of clients.entries()) {
                  if((c.username||"").toLowerCase()===raName.toLowerCase()) { ADMIN_UIDS.delete(uid); break; }
                }
                _termReply([`${raName} admin access removed.`]);
              } catch(e) { _termReply(null, e.message); }
              break;
            }

            // ── makedev ───────────────────────────────────────────────────────
            case "makedev": {
              const mdName = args[0];
              if(!mdName) { _termReply(null,"Usage: makedev <username>"); break; }
              try {
                const mdRow = stmt.getAccount.get(mdName.toLowerCase());
                if(!mdRow) { _termReply(null,`Account "${mdName}" not found.`); break; }
                stmtAddDev.run(mdName.toLowerCase());
                DEV_USERNAMES.add(mdName.toLowerCase());
                for(const [uid,c] of clients.entries()) {
                  if((c.username||"").toLowerCase()===mdName.toLowerCase()&&c.ws.readyState===1) {
                    send(c.ws,{type:"dev_promoted",message:"You have been granted Developer access."});
                  }
                }
                _termReply([`${mdName} is now a Developer.`]);
              } catch(e) { _termReply(null, e.message); }
              break;
            }

            // ── removedev ─────────────────────────────────────────────────────
            case "removedev": {
              const rdName = args[0];
              if(!rdName) { _termReply(null,"Usage: removedev <username>"); break; }
              try {
                stmtRemoveDev.run(rdName.toLowerCase());
                DEV_USERNAMES.delete(rdName.toLowerCase());
                _termReply([`${rdName} developer access removed.`]);
              } catch(e) { _termReply(null, e.message); }
              break;
            }

            // ── admins ────────────────────────────────────────────────────────
            case "admins": {
              const adminList = [...ADMIN_USERNAMES];
              const devList = [...DEV_USERNAMES];
              const lines = [
                `Lead admins : ${[...LEAD_ADMINS].join(", ")||"none"}`,
                `Admins (${adminList.length}): ${adminList.join(", ")||"none"}`,
                `Devs   (${devList.length}): ${devList.join(", ")||"none"}`,
              ];
              _termReply(lines);
              break;
            }

            // ── devs ──────────────────────────────────────────────────────────
            case "devs": {
              const devList = [...DEV_USERNAMES];
              if(!devList.length) { _termReply(["No developers found."]); break; }
              _termReply([
                `Developers (${devList.length}):`,
                ...devList.map(u => {
                  const online = [...clients.values()].some(c=>(c.username||"").toLowerCase()===u.toLowerCase());
                  return `  ${u.padEnd(24)} ${online ? "● online" : "○ offline"}`;
                })
              ]);
              break;
            }

            // ── banned ────────────────────────────────────────────────────────
            case "banned": {
              try {
                const bannedRows = db.prepare("SELECT username, uid FROM auth_credentials WHERE banned=1 ORDER BY username").all().concat(
                  // legacy: check saves.data for banned flag
                );
                // check auth table for banned column
                let rows2 = [];
                try { rows2 = db.prepare("SELECT username FROM accounts WHERE username IN (SELECT username FROM auth_credentials WHERE banned=1)").all(); } catch(e){}
                // Use anomalies-based ban approach
                const banRows = db.prepare("SELECT DISTINCT uid FROM anomalies WHERE reason='banned' ORDER BY uid").all();
                if(!banRows.length) { _termReply(["No banned players found."]); break; }
                const lines = banRows.map(r=>{
                  const accRow = db.prepare("SELECT username, charName FROM accounts WHERE uid=?").get(r.uid);
                  return accRow ? `${(accRow.charName||"?").padEnd(20)} (${accRow.username})` : `uid:${r.uid.slice(0,20)}`;
                });
                _termReply([`${banRows.length} ban record(s):`, ...lines]);
              } catch(e) { _termReply(null, e.message); }
              break;
            }

            // ── setgold (alias for gold) ───────────────────────────────────
            case "setgold": {
              const sgname = args[0]; const sgamt = parseInt(args[1]);
              if(!sgname||isNaN(sgamt)) { _termReply(null,"Usage: setgold <charname> <amount>"); break; }
              const sgrow = db.prepare("SELECT uid, data FROM saves WHERE username=? COLLATE NOCASE").get(sgname);
              if(!sgrow) { _termReply(null,`Player "${sgname}" not found.`); break; }
              const sgsave = JSON.parse(sgrow.data||"{}");
              const sgold = sgsave.player?.gold||0;
              if(sgsave.player) sgsave.player.gold = Math.max(0, sgamt);
              db.prepare("UPDATE saves SET data=? WHERE uid=?").run(JSON.stringify(sgsave), sgrow.uid);
              _setExpectedGold(sgrow.uid, Math.max(0, sgamt));
              _logTx(sgrow.uid, "admin_set_gold", { by: clientUid, oldGold: sgold, newGold: sgamt, terminal: true });
              _termReply([`${sgname}: gold ${sgold} → ${Math.max(0, sgamt)}`]);
              break;
            }

            // ── addgold ────────────────────────────────────────────────────────
            case "addgold": {
              const agname = args[0]; const agamt = parseInt(args[1]);
              if(!agname||isNaN(agamt)) { _termReply(null,"Usage: addgold <charname> <amount>"); break; }
              const agrow = db.prepare("SELECT uid, data FROM saves WHERE username=? COLLATE NOCASE").get(agname);
              if(!agrow) { _termReply(null,`Player "${agname}" not found.`); break; }
              const agsave = JSON.parse(agrow.data||"{}");
              const agold = agsave.player?.gold||0;
              const agnew = Math.max(0, agold + agamt);
              if(agsave.player) agsave.player.gold = agnew;
              db.prepare("UPDATE saves SET data=? WHERE uid=?").run(JSON.stringify(agsave), agrow.uid);
              _setExpectedGold(agrow.uid, agnew);
              _logTx(agrow.uid, "admin_add_gold", { by: clientUid, delta: agamt, newGold: agnew, terminal: true });
              _termReply([`${agname}: gold ${agold} + ${agamt} = ${agnew}`]);
              break;
            }

            // ── sethp ──────────────────────────────────────────────────────────
            case "sethp": {
              const hpname = args[0]; const hpamt = parseInt(args[1]);
              if(!hpname||isNaN(hpamt)) { _termReply(null,"Usage: sethp <charname> <hp>"); break; }
              const hprow = db.prepare("SELECT uid, data FROM saves WHERE username=? COLLATE NOCASE").get(hpname);
              if(!hprow) { _termReply(null,`Player "${hpname}" not found.`); break; }
              const hpsave = JSON.parse(hprow.data||"{}");
              const hpold = hpsave.player?.hp||0;
              const hpmax = hpsave.player?.maxHp||100;
              if(hpsave.player) hpsave.player.hp = Math.max(1, Math.min(hpamt, hpmax));
              db.prepare("UPDATE saves SET data=? WHERE uid=?").run(JSON.stringify(hpsave), hprow.uid);
              _termReply([`${hpname}: hp ${hpold} → ${hpsave.player.hp} (max: ${hpmax})`]);
              break;
            }

            // ── healplayer ─────────────────────────────────────────────────────
            case "heal": {
              const healname = args[0];
              if(!healname) { _termReply(null,"Usage: heal <charname>"); break; }
              const healrow = db.prepare("SELECT uid, data FROM saves WHERE username=? COLLATE NOCASE").get(healname);
              if(!healrow) { _termReply(null,`Player "${healname}" not found.`); break; }
              const healsave = JSON.parse(healrow.data||"{}");
              const healmax = healsave.player?.maxHp||100;
              if(healsave.player) healsave.player.hp = healmax;
              db.prepare("UPDATE saves SET data=? WHERE uid=?").run(JSON.stringify(healsave), healrow.uid);
              _termReply([`${healname} healed to full HP (${healmax}).`]);
              break;
            }

            // ── setzone ────────────────────────────────────────────────────────
            case "setzone": {
              // setzone <charname> <zoneId>  — teleport a player to a zone
              const szpname = args[0]; const szzone = args[1];
              if(!szpname||!szzone) { _termReply(null,"Usage: setzone <charname> <zoneId>"); break; }
              const szrow = db.prepare("SELECT uid, data FROM saves WHERE username=? COLLATE NOCASE").get(szpname);
              if(!szrow) { _termReply(null,`Player "${szpname}" not found.`); break; }
              const szsave = JSON.parse(szrow.data||"{}");
              const szold = szsave.lastZone||"?";
              if(szsave.player) szsave.player.lastZone = szzone;
              szsave.lastZone = szzone;
              db.prepare("UPDATE saves SET data=? WHERE uid=?").run(JSON.stringify(szsave), szrow.uid);
              // Notify if online
              for(const [uid,c] of clients.entries()) {
                if((c.username||"").toLowerCase()===szpname.toLowerCase()&&c.ws.readyState===1) {
                  send(c.ws,{type:"system_message",message:`An administrator has moved you to ${szzone}.`});
                }
              }
              _termReply([`${szpname}: zone ${szold} → ${szzone}`]);
              break;
            }

            // ── setrespawn ─────────────────────────────────────────────────────
            case "setrespawn": {
              const srpname = args[0]; const srzone = args[1];
              if(!srpname||!srzone) { _termReply(null,"Usage: setrespawn <charname> <zoneId>"); break; }
              const srrow = db.prepare("SELECT uid, data FROM saves WHERE username=? COLLATE NOCASE").get(srpname);
              if(!srrow) { _termReply(null,`Player "${srpname}" not found.`); break; }
              const srsave = JSON.parse(srrow.data||"{}");
              const srold = srsave.player?.respawnZone||"?";
              if(srsave.player) srsave.player.respawnZone = srzone;
              db.prepare("UPDATE saves SET data=? WHERE uid=?").run(JSON.stringify(srsave), srrow.uid);
              _termReply([`${srpname}: respawnZone ${srold} → ${srzone}`]);
              break;
            }

            // ── inventory ─────────────────────────────────────────────────────
            case "inventory": {
              const invname = args[0];
              if(!invname) { _termReply(null,"Usage: inventory <charname>"); break; }
              const invrow = db.prepare("SELECT uid, data FROM saves WHERE username=? COLLATE NOCASE").get(invname);
              if(!invrow) { _termReply(null,`Player "${invname}" not found.`); break; }
              const invsave = JSON.parse(invrow.data||"{}");
              const inv = invsave.player?.inventory||{};
              const lines = [`${invname}'s inventory:`];
              const mats = Object.entries(inv.materials||{});
              const provs = Object.entries(inv.provisions||{});
              const eqInv = Object.entries(inv.equipment||{});
              if(!mats.length&&!provs.length&&!eqInv.length) { lines.push("  (empty)"); }
              if(mats.length) { lines.push("  Materials:"); mats.forEach(([k,v])=>lines.push(`    ${k.padEnd(30)} x${v}`)); }
              if(provs.length) { lines.push("  Provisions:"); provs.forEach(([k,v])=>lines.push(`    ${k.padEnd(30)} x${v}`)); }
              if(eqInv.length) { lines.push("  Equipment:"); eqInv.forEach(([k,v])=>lines.push(`    ${String(v?.name||k).padEnd(30)}`)); }
              _termReply(lines);
              break;
            }

            // ── clearinventory ────────────────────────────────────────────────
            case "clearinventory": {
              const ciname = args[0]; const citype = (args[1]||"all").toLowerCase();
              if(!ciname) { _termReply(null,"Usage: clearinventory <charname> [materials|provisions|equipment|all]"); break; }
              const cirow = db.prepare("SELECT uid, data FROM saves WHERE username=? COLLATE NOCASE").get(ciname);
              if(!cirow) { _termReply(null,`Player "${ciname}" not found.`); break; }
              const cisave = JSON.parse(cirow.data||"{}");
              if(!cisave.player?.inventory) { _termReply([`${ciname} has no inventory.`]); break; }
              if(citype==="materials"||citype==="all") cisave.player.inventory.materials={};
              if(citype==="provisions"||citype==="all") cisave.player.inventory.provisions={};
              if(citype==="equipment"||citype==="all") cisave.player.inventory.equipment={};
              _snapshotSave(cirow.uid, "admin_clearinv");
              db.prepare("UPDATE saves SET data=? WHERE uid=?").run(JSON.stringify(cisave), cirow.uid);
              _termReply([`${ciname} inventory (${citype}) cleared.`]);
              break;
            }

            // ── giveitem ──────────────────────────────────────────────────────
            case "giveitem": {
              // giveitem <charname> <itemId> [qty]
              const giname = args[0]; const giitem = args[1]; const giamt = Math.max(1, parseInt(args[2]||"1")||1);
              if(!giname||!giitem) { _termReply(null,"Usage: giveitem <charname> <itemId> [qty]"); break; }
              const girow = db.prepare("SELECT uid, data FROM saves WHERE username=? COLLATE NOCASE").get(giname);
              if(!girow) { _termReply(null,`Player "${giname}" not found.`); break; }
              const gisave = JSON.parse(girow.data||"{}");
              if(!gisave.player) gisave.player = {};
              if(!gisave.player.inventory) gisave.player.inventory = {};
              if(!gisave.player.inventory.materials) gisave.player.inventory.materials = {};
              const giInvKey = "materials";
              const prev = gisave.player.inventory[giInvKey][giitem]||0;
              gisave.player.inventory[giInvKey][giitem] = prev + giamt;
              db.prepare("UPDATE saves SET data=? WHERE uid=?").run(JSON.stringify(gisave), girow.uid);
              _logTx(girow.uid, "admin_give_item", { by: clientUid, itemId: giitem, qty: giamt, terminal: true });
              _termReply([`Gave ${giamt}x ${giitem} to ${giname}. New qty: ${prev + giamt}`]);
              break;
            }

            // ── snapshot ──────────────────────────────────────────────────────
            case "snapshot": {
              // snapshot <charname>  — take a manual backup snapshot of a player's save
              const snname = args[0];
              if(!snname) { _termReply(null,"Usage: snapshot <charname>"); break; }
              const snrow = db.prepare("SELECT uid FROM saves WHERE username=? COLLATE NOCASE").get(snname);
              if(!snrow) { _termReply(null,`Player "${snname}" not found.`); break; }
              _snapshotSave(snrow.uid, "admin_manual_terminal");
              _termReply([`Snapshot taken for ${snname}.`]);
              break;
            }

            // ── snapshots ─────────────────────────────────────────────────────
            case "snapshots": {
              const ssname = args[0];
              if(!ssname) { _termReply(null,"Usage: snapshots <charname>"); break; }
              const ssrow = db.prepare("SELECT uid FROM saves WHERE username=? COLLATE NOCASE").get(ssname);
              if(!ssrow) { _termReply(null,`Player "${ssname}" not found.`); break; }
              const snaps = stmt.getSnapshots.all(ssrow.uid);
              if(!snaps.length) { _termReply([`No snapshots for ${ssname}.`]); break; }
              _termReply([
                `${ssname} — ${snaps.length} snapshot(s):`,
                "ID    REASON                    DATE",
                ...snaps.map(s=>`${String(s.id).padEnd(6)}${(s.reason||"?").padEnd(26)}${new Date(s.ts).toLocaleString()}`)
              ]);
              break;
            }

            // ── restore ───────────────────────────────────────────────────────
            case "restore": {
              // restore <charname> <snapshot_id>
              const rname = args[0]; const rid = parseInt(args[1]);
              if(!rname||isNaN(rid)) { _termReply(null,"Usage: restore <charname> <snapshot_id>  (get IDs from: snapshots <charname>)"); break; }
              const rrow = db.prepare("SELECT uid FROM saves WHERE username=? COLLATE NOCASE").get(rname);
              if(!rrow) { _termReply(null,`Player "${rname}" not found.`); break; }
              const snap = stmt.getSnapshotById.get(rid, rrow.uid);
              if(!snap) { _termReply(null,`Snapshot ${rid} not found for ${rname}.`); break; }
              // Take a snapshot of the current state before restoring
              _snapshotSave(rrow.uid, `pre_restore_${rid}`);
              db.prepare("UPDATE saves SET data=? WHERE uid=?").run(snap.data, rrow.uid);
              _invalidateSaveCache(rrow.uid);
              // Kick if online so they reload with restored data
              for(const [uid,c] of clients.entries()) {
                if((c.username||"").toLowerCase()===rname.toLowerCase()&&c.ws.readyState===1) {
                  send(c.ws,{type:"system_message",message:"Your save has been restored by an administrator. Reconnecting..."});
                  setTimeout(()=>{ try{c.ws.close();}catch(e){} }, 1500);
                }
              }
              _termReply([`${rname} restored from snapshot ${rid}. Player will be kicked to reload.`]);
              break;
            }

            // ── txlog ─────────────────────────────────────────────────────────
            case "txlog": {
              const txname = args[0]; const txlimit = parseInt(args[1]||"20");
              if(!txname) { _termReply(null,"Usage: txlog <charname> [limit]"); break; }
              const txrow = db.prepare("SELECT uid FROM saves WHERE username=? COLLATE NOCASE").get(txname);
              if(!txrow) { _termReply(null,`Player "${txname}" not found.`); break; }
              const logs = stmt.getTxLog.all(txrow.uid, Math.min(txlimit,100));
              if(!logs.length) { _termReply([`No transaction log for ${txname}.`]); break; }
              const _fmtTx = ts => new Date(ts).toISOString().slice(0,19).replace("T"," ");
              _termReply([
                `${txname} — last ${logs.length} transactions:`,
                ...logs.map(l=>`${_fmtTx(l.ts)}  ${(l.action||"?").padEnd(26)}${l.details||""}`)
              ]);
              break;
            }

            // ── deleteaccount ─────────────────────────────────────────────────
            case "deleteaccount": {
              const dcname = args[0];
              if(!dcname) { _termReply(null,"Usage: deleteaccount <charname>"); break; }
              const dcn = stmt.getCharname.get(dcname.toLowerCase());
              if(!dcn) { _termReply(null,`Character "${dcname}" not found.`); break; }
              const delUid = dcn.uid; const delUsername = dcn.username.toLowerCase();
              if(LEAD_ADMINS.has(delUsername)) { _termReply(null,"Cannot delete the lead admin."); break; }
              if(delUid === clientUid) { _termReply(null,"Cannot delete yourself."); break; }
              // Kick if online
              for(const [uid,c] of clients.entries()) {
                if((c.username||"").toLowerCase()===delUsername&&c.ws&&c.ws.readyState===1) {
                  try{ send(c.ws,{type:"kicked",reason:"account_deleted"}); c.ws.close(); }catch(e){}
                }
              }
              clients.delete(delUid);
              ADMIN_UIDS.delete(delUid); ADMIN_USERNAMES.delete(delUsername); stmtRemoveAdmin.run(delUsername);
              _saveCache.delete(delUid); _usernameToUid.delete(delUsername); _expectedGold.delete(delUid);
              // Guild cleanup
              const dcGuildMem = stmt.getMemberGuild.get(delUid);
              if (dcGuildMem) {
                const dcGuildId = dcGuildMem.guild_id;
                if (dcGuildMem.role === "leader") {
                  const dcGuildAll = stmt.getGuildMembers.all(dcGuildId);
                  dcGuildAll.forEach(m => {
                    if (m.uid !== delUid) _sendGuildLeft(m.uid, "dissolved");
                    stmt.deleteGuildMember.run(dcGuildId, m.uid);
                  });
                  stmt.deleteGuild.run(dcGuildId);
                } else {
                  stmt.deleteGuildMember.run(dcGuildId, delUid);
                  _broadcastGuildUpdate(dcGuildId);
                }
              }
              stmt.deleteSave.run(delUid); stmt.deleteAccount.run(delUsername); stmt.deleteCharname.run(delUid);
              stmt.deleteSocial.run(delUid); stmt.deleteProfile.run(delUid); stmt.deleteInboxAll.run(delUid);
              stmt.deleteDmsAll.run(delUid); stmt.deletePresence.run(delUsername); stmt.deleteZonePresenceAll.run(delUsername);
              stmt.deleteAnomalies.run(delUid); stmt.deleteAuth.run(delUid); stmt.deleteSnapshots.run(delUid);
              stmt.deleteTxLog.run(delUid); stmt.deleteZoneChat.run(delUid);
              _termReply([`Account "${dcname}" (${delUsername}) permanently deleted.`]);
              console.log(`[ADMIN] DELETED account ${dcname} (${delUsername}, uid=${delUid}) via terminal by uid=${clientUid}`);
              break;
            }

            // ── zones ─────────────────────────────────────────────────────────
            case "zones": {
              const zoneFilter = (args[0]||"").toLowerCase();
              const zoneList = Object.entries(ZONE_DB)
                .filter(([id]) => !zoneFilter || id.includes(zoneFilter))
                .sort(([a],[b])=>a.localeCompare(b));
              if(!zoneList.length) { _termReply(["No zones found."]); break; }
              _termReply([
                `${zoneList.length} zone(s):`,
                "ID                          TYPE    RARITY",
                ...zoneList.map(([id,z])=>`${id.padEnd(28)} ${(z.safe?"safe":"danger").padEnd(8)} ${z.rarity||"?"}`)
              ]);
              break;
            }

            // ── hostiles ──────────────────────────────────────────────────────
            case "hostiles": {
              const enemyFilter = (args[0]||"").toLowerCase();
              const enemyList = Object.entries(ENEMY_DB)
                .filter(([id])=>!enemyFilter||id.includes(enemyFilter))
                .sort(([a],[b])=>a.localeCompare(b));
              if(!enemyList.length) { _termReply(["No enemies found."]); break; }
              _termReply([
                `${enemyList.length} hostile(s):`,
                "ID                          NAME                     HP      RARITY",
                ...enemyList.map(([id,e])=>`${id.padEnd(28)} ${(e.name||"?").padEnd(25)} ${String(e.maxHp||"?").padEnd(8)} ${e.rarity||"?"}`)
              ]);
              break;
            }

            // ── items ─────────────────────────────────────────────────────────
// ── gears ───────────────────────────────────────────────────────
            case "gears": {
              const wf = (args[0]||"").toLowerCase();
              const wList = Object.entries(ITEM_DB)
                .filter(([id,it])=>it.type==="gear"&&(!wf||id.includes(wf)||(it.name||"").toLowerCase().includes(wf)))
                .sort(([a],[b])=>a.localeCompare(b));
              if(!wList.length) { _termReply(["No gears found."]); break; }
              _termReply([
                `${wList.length} gear(s):`,
                "ID                          NAME                     DMG   ACC   COST",
                ...wList.map(([id,it])=>`${id.padEnd(28)} ${(it.name||"?").padEnd(25)} ${String(it.dmg||0).padEnd(6)}${String(it.acc||0).padEnd(6)}${it.cost||0}`)
              ]);
              break;
            }

            // ── accessories ───────────────────────────────────────────────────
            case "accessories": {
              const acf = (args[0]||"").toLowerCase();
              const acList = Object.entries(ITEM_DB)
                .filter(([id,it])=>it.type==="accessory"&&(!acf||id.includes(acf)||(it.name||"").toLowerCase().includes(acf)))
                .sort(([a],[b])=>a.localeCompare(b));
              if(!acList.length) { _termReply(["No accessories found."]); break; }
              _termReply([
                `${acList.length} accessor(ies):`,
                "ID                          NAME                     RARITY       COST",
                ...acList.map(([id,it])=>`${id.padEnd(28)} ${(it.name||"?").padEnd(25)} ${(it.rarity||"?").padEnd(13)}${it.cost||0}`)
              ]);
              break;
            }

            // ── materials ─────────────────────────────────────────────────────
            case "materials": {
              const mf = (args[0]||"").toLowerCase();
              const mList = Object.entries(ITEM_DB)
                .filter(([id,it])=>it.type==="material"&&(!mf||id.includes(mf)||(it.name||"").toLowerCase().includes(mf)))
                .sort(([a],[b])=>a.localeCompare(b));
              if(!mList.length) { _termReply(["No materials found."]); break; }
              _termReply([
                `${mList.length} material(s):`,
                "ID                          NAME                     RARITY       COST",
                ...mList.map(([id,it])=>`${id.padEnd(28)} ${(it.name||"?").padEnd(25)} ${(it.rarity||"?").padEnd(13)}${it.cost||0}`)
              ]);
              break;
            }

            // ── provisions ────────────────────────────────────────────────────
            case "provisions": {
              const pf = (args[0]||"").toLowerCase();
              const pList = Object.entries(ITEM_DB)
                .filter(([id,it])=>it.type==="provision"&&(!pf||id.includes(pf)||(it.name||"").toLowerCase().includes(pf)))
                .sort(([a],[b])=>a.localeCompare(b));
              if(!pList.length) { _termReply(["No provisions found."]); break; }
              _termReply([
                `${pList.length} provision(s):`,
                "ID                          NAME                     HEAL  COST",
                ...pList.map(([id,it])=>`${id.padEnd(28)} ${(it.name||"?").padEnd(25)} ${String(it.healHp||0).padEnd(6)}${it.cost||0}`)
              ]);
              break;
            }

            // ── actions ───────────────────────────────────────────────────────
            case "actions": {
              const actnf = (args[0]||"").toLowerCase();
              const actnList = Object.entries(ITEM_DB)
                .filter(([id,it])=>it.category==="action"&&(!actnf||id.includes(actnf)||(it.name||"").toLowerCase().includes(actnf)))
                .sort(([a],[b])=>a.localeCompare(b));
              if(!actnList.length) { _termReply(["No actions found."]); break; }
              _termReply([
                `${actnList.length} action(s):`,
                "ID                          NAME                     REQ. GEAR",
                ...actnList.map(([id,it])=>`${id.padEnd(28)} ${(it.name||"?").padEnd(25)} ${it.requiresGearType||"any"}`)
              ]);
              break;
            }

            // ── rooms (detailed) ───────────────────────────────────────────────
            case "room": {
              const roomId = args[0];
              if(!roomId) { _termReply(null,"Usage: room <roomId>  (get IDs from: rooms)"); break; }
              const room = [...rooms.entries()].find(([id])=>id.includes(roomId));
              if(!room) { _termReply(null,`Room "${roomId}" not found.`); break; }
              const [rid,r] = room;
              const lines = [
                `Room ID   : ${rid}`,
                `Type      : ${r.isPvP?"PvP":"PvE"}`,
                `Ended     : ${r.ended?"yes":"no"}`,
                `Started   : ${new Date(r.combatAt).toLocaleString()}`,
                `Members   : ${(r.members||[]).map(m=>`${m.name} HP:${m.hp}/${m.maxHp}`).join(", ")||"none"}`,
                `Enemies   : ${(r.enemies||[]).map(e=>`${e.name} HP:${e.hp}/${e.maxHp}`).join(", ")||"none"}`,
                `DoTs      : ${(r.dots||[]).length}`,
              ];
              _termReply(lines);
              break;
            }

            // ── endroom ────────────────────────────────────────────────────────
            case "endroom": {
              // Force-end a combat room (useful for stuck rooms)
              const erName = args[0];
              if(!erName) { _termReply(null,"Usage: endroom <partial-roomId>  (get IDs from: rooms)"); break; }
              const erMatch = [...rooms.entries()].find(([id])=>id.includes(erName));
              if(!erMatch) { _termReply(null,`No room matching "${erName}" found.`); break; }
              const [erid,er] = erMatch;
              if(er.ticker) clearInterval(er.ticker);
              er.ended = true;
              for(const m of (er.members||[])) {
                const c = clients.get(m.uid);
                if(c&&c.ws.readyState===1) send(c.ws,{type:"system_message",message:"Combat room ended by administrator."});
              }
              rooms.delete(erid);
              if (er.isPvP) _pvpRoomCount = Math.max(0, _pvpRoomCount - 1);
              _termReply([`Room ${erid} force-ended and deleted.`]);
              break;
            }

            // ── announce ───────────────────────────────────────────────────────
            case "announce": {
              // announce <message> — sends as a system update (yellow warning style)
              const amsg = args.join(" ");
              if(!amsg) { _termReply(null,"Usage: announce <message>"); break; }
              let acount = 0;
              for(const [uid,c] of clients.entries()) {
                if(c.ws&&c.ws.readyState===1) {
                  send(c.ws,{type:"zone_chat_msg",msg:amsg,zone:"__system__",isSystemUpdate:true});
                  acount++;
                }
              }
              _termReply([`Announcement sent to ${acount} client(s): "${amsg}"`]);
              break;
            }

            // ── dm ─────────────────────────────────────────────────────────────
            case "dm": {
              // dm <charname> <message> — send a direct message as [ADMIN]
              const dmTarget = args[0]; const dmMsg = args.slice(1).join(" ");
              if(!dmTarget||!dmMsg) { _termReply(null,"Usage: dm <charname> <message>"); break; }
              // Resolve charname → username via DB
              const dmAccRow = db.prepare("SELECT username FROM accounts WHERE LOWER(charName)=?").get(dmTarget.toLowerCase());
              if(!dmAccRow) { _termReply([`No account found with charname "${dmTarget}".`]); break; }
              const dmUsername = dmAccRow.username.toLowerCase();
              let dmSent = false;
              for(const [uid,c] of clients.entries()) {
                if((c.username||"").toLowerCase()===dmUsername&&c.ws.readyState===1) {
                  send(c.ws,{type:"zone_chat_msg",name:"[ADMIN]",msg:dmMsg,zone:"__dm__"});
                  dmSent = true;
                }
              }
              _termReply([dmSent ? `DM sent to ${dmTarget}: "${dmMsg}"` : `${dmTarget} is not online.`]);
              break;
            }

            // ── search ─────────────────────────────────────────────────────────
            case "search": {
              // search <partial_name> — find players by character name or username
              const sterm = (args[0]||"").toLowerCase();
              if(!sterm) { _termReply(null,"Usage: search <name_fragment>"); break; }
              const srows = db.prepare(
                "SELECT a.username, a.charName, a.uid FROM accounts a WHERE LOWER(a.username) LIKE ? OR LOWER(a.charName) LIKE ? LIMIT 30"
              ).all(`%${sterm}%`, `%${sterm}%`);
              if(!srows.length) { _termReply([`No players matching "${sterm}".`]); break; }
              const sEnriched = srows.map(r => {
                let playtime = 0, isSub = false;
                try {
                  const sd = db.prepare("SELECT data FROM saves WHERE uid=?").get(r.uid);
                  if(sd){ const sp=JSON.parse(sd.data||"{}"); playtime=sp.player?.totalPlaytime||0; isSub=!!(sp.player?.isSubscribed); }
                } catch(e){}
                const online = [...clients.values()].some(c=>(c.username||"").toLowerCase()===r.username.toLowerCase());
                const ptH=Math.floor(playtime/3600), ptM=Math.floor((playtime%3600)/60);
                return { ...r, online, ptStr: ptH>0?`${ptH}h ${ptM}m`:`${ptM}m`, isSub };
              });
              _termReply([
                `${sEnriched.length} result(s) for "${sterm}":`,
                `${"CHARNAME".padEnd(18)}${"STATUS".padEnd(10)}${"PLAYTIME".padEnd(12)}SUB`,
                "─".repeat(44),
                ...sEnriched.map(r=>
                  `${(r.charName||"?").padEnd(18)}${(r.online?"● ONLINE":"○ offline").padEnd(10)}${r.ptStr.padEnd(12)}${r.isSub?"YES":"no"}`
                )
              ]);
              break;
            }

            // ── leaderboard ────────────────────────────────────────────────────
            case "leaderboard": {
              const lbField = (args[0]||"gold").toLowerCase();
              const lbLimit = Math.min(parseInt(args[1]||"10"),50);
              let lbRows, lbTitle;
              if(lbField==="gold") {
                lbRows = db.prepare("SELECT s.uid, a.charName, a.username, s.data FROM saves s LEFT JOIN accounts a ON a.uid=s.uid").all();
                lbRows = lbRows.map(r=>{ try{ const d=JSON.parse(r.data||"{}"); return {...r, val: d.player?.gold||0}; }catch{return {...r,val:0};} });
                lbRows.sort((a,b)=>b.val-a.val);
                lbTitle = "Top players by gold";
              } else if(lbField==="kills") {
                lbRows = db.prepare("SELECT s.uid, a.charName, a.username, s.data FROM saves s LEFT JOIN accounts a ON a.uid=s.uid").all();
                lbRows = lbRows.map(r=>{ try{ const d=JSON.parse(r.data||"{}"); const kills=d.player?.stats?.kills||{}; return {...r, val: Object.values(kills).reduce((s,n)=>s+n,0)}; }catch{return {...r,val:0};} });
                lbRows.sort((a,b)=>b.val-a.val);
                lbTitle = "Top players by kills";
              } else if(lbField==="playtime") {
                lbRows = db.prepare("SELECT s.uid, a.charName, a.username, s.data FROM saves s LEFT JOIN accounts a ON a.uid=s.uid").all();
                lbRows = lbRows.map(r=>{ try{ const d=JSON.parse(r.data||"{}"); return {...r, val: d.player?.totalPlaytime||0}; }catch{return {...r,val:0};} });
                lbRows.sort((a,b)=>b.val-a.val);
                lbTitle = "Top players by playtime";
              } else {
                _termReply(null,"Usage: leaderboard [gold|kills|playtime] [limit]"); break;
              }
              const top = lbRows.slice(0, lbLimit);
              _termReply([
                lbTitle+` (top ${top.length}):`,
                "RANK  CHARNAME         USERNAME         VALUE",
                ...top.map((r,i)=>`${String(i+1).padStart(4)}  ${(r.charName||"?").padEnd(16)} ${(r.username||"?").padEnd(16)} ${lbField==="playtime" ? `${Math.floor(r.val/3600)}h ${Math.floor((r.val%3600)/60)}m` : r.val}`)
              ]);
              break;
            }

            // ── serverlog ──────────────────────────────────────────────────────
            case "serverlog": {
              const slCmd = (args[0]||'').toLowerCase();

              // serverlog on / off — toggle verbose streaming of console.log lines
              if (slCmd === 'on') {
                _logVerbose = true;
                if(global._logVerboseTimer){ clearTimeout(global._logVerboseTimer); global._logVerboseTimer = null; }
                _termReply([
                  'Verbose streaming ON — all console.log lines now stream live.',
                  'Type  serverlog off  to stop.',
                ]);
                break;
              }
              if (slCmd === 'off') {
                _logVerbose = false;
                if(global._logVerboseTimer){ clearTimeout(global._logVerboseTimer); global._logVerboseTimer = null; }
                _termReply(['Verbose streaming OFF — only errors and warnings stream live.']);
                break;
              }

              // serverlog clear — wipe ring buffer
              if (slCmd === 'clear') {
                _logRing.length = 0;
                _termReply(['Log ring buffer cleared.']);
                break;
              }

              // serverlog [tail [n]] — dump last N lines from ring buffer (default 50)
              const n = Math.min(parseInt(args[slCmd==='tail'?1:0]||'50')||50, _logRingMax);
              const slice = _logRing.slice(-n);
              if (!slice.length) { _termReply(['No log entries buffered yet.']); break; }
              const fmt = ts => new Date(ts).toISOString().slice(11,19);
              _termReply([
                `Last ${slice.length} of ${_logRing.length} buffered entries  |  verbose: ${_logVerbose?'ON':'OFF'}`,
                '─'.repeat(52),
                ...slice.map(e => `${fmt(e.ts)} ${e.prefix}${e.text}`),
              ]);
              break;
            }

            // ── vacuum ─────────────────────────────────────────────────────────
            case "vacuum": {
              try {
                db.prepare("VACUUM").run();
                const stat = fs.statSync(DB_PATH);
                _termReply([`VACUUM complete. DB size: ${(stat.size/1024).toFixed(1)} KB`]);
              } catch(e) { _termReply(null, e.message); }
              break;
            }

            // ── pruneold ───────────────────────────────────────────────────────
            case "pruneold": {
              try {
                const snapCutoff = Date.now() - SNAPSHOT_MAX_AGE_MS;
                const txCutoff   = Date.now() - TXLOG_MAX_AGE_MS;
                const chatCutoff = Date.now() - 3600000;
                const snaps = stmt.pruneSnapshots.run(snapCutoff).changes;
                const txs   = stmt.pruneTxLog.run(txCutoff).changes;
                const chats = stmt.pruneZoneChat.run(chatCutoff).changes;
                _termReply([`Pruned: ${snaps} snapshots, ${txs} txlog entries, ${chats} zone chat messages.`]);
              } catch(e) { _termReply(null, e.message); }
              break;
            }

            // ── unknown ───────────────────────────────────────────────────────
            default:
              if(!base) { _termReply([]); break; }
              _termReply(null, `Unknown command: "${base}". Type "help" for a list of commands.`);
          }
        } catch(e) {
          _termReply(null, `Error: ${e.message}`);
        }
        break;
      }

      case "admin_debug_social": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) { send(ws, { type:"admin_debug_social_result", error:"not_authorized" }); break; }
        const { username: dbgUsername } = msg;
        if (!dbgUsername) { send(ws, { type:"admin_debug_social_result", error:"no_username" }); break; }
        const dbgAccRow = stmt.getAccount.get(dbgUsername.toLowerCase());
        const dbgUid = dbgAccRow ? dbgAccRow.uid : null;
        if (!dbgUid) { send(ws, { type:"admin_debug_social_result", error:"user_not_found", uid: null }); break; }
        const dbgSocial = dbGetSocial(dbgUid);
        const dbgMemFriends = _uidToFriends.get(dbgUid);
        // Also check inbox and accounts uid column
        const dbgInbox = dbGetInboxObject(dbgUid);
        const dbgInvites = Object.keys(dbgInbox.invites || {});
        const dbgAccByUid = stmt.getAccountByUid.get(dbgUid);
        send(ws, {
          type: "admin_debug_social_result",
          uid: dbgUid,
          dbFriends: ((dbgSocial && dbgSocial.friends) || []).map(f => f.name),
          dbPendingOut: ((dbgSocial && dbgSocial.pendingOut) || []).map(f => f.name),
          memFriends: dbgMemFriends ? [...dbgMemFriends] : null,
          inboxInvites: dbgInvites,
          uidLookupOk: !!(dbgAccByUid && dbgAccByUid.username),
        });
        break;
      }

      // ══════════════════════════════════════════════════════════════════════
      //  GUILD HANDLERS
      // ══════════════════════════════════════════════════════════════════════

      // Create a guild
      case "guild_create": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "invite")) return;
        const { name: gcName } = msg;
        if (!gcName || typeof gcName !== "string") break;
        const trimName = _toTitleCase(gcName.trim());
        if (!_guildNameValid(trimName)) {
          send(ws, { type: "guild_error", msg: "Invalid guild name." });
          break;
        }
        const gcExisting = _getUidGuild(clientUid);
        if (gcExisting) {
          send(ws, { type: "guild_error", msg: "You are already in a guild." });
          break;
        }
        const gcTaken = stmt.getGuildByName.get(trimName.toLowerCase());
        if (gcTaken) {
          send(ws, { type: "guild_error", msg: "That guild name is already taken." });
          break;
        }
        const gcClient = clients.get(clientUid);
        const gcUsername = gcClient ? gcClient.username : "";
        const gcSave = _getCachedSave(clientUid);
        const gcCharName = gcSave?.player?.name || gcUsername;
        const guildId = _genGuildId();
        const now = Date.now();
        stmt.insertGuild.run(guildId, trimName, trimName.toLowerCase(), clientUid, "{}", now);
        stmt.insertGuildMember.run(guildId, clientUid, gcUsername, gcCharName, "leader", now);
        _broadcastGuildUpdate(guildId);
        send(ws, { type: "guild_created", guildId, name: trimName });
        break;
      }

      // Send a guild invite




      // Leave a guild
      case "guild_leave": {
        if (!clientUid) return;
        const glMyGuild = _getUidGuild(clientUid);
        if (!glMyGuild) break;
        const guildId = glMyGuild.guild_id;
        if (glMyGuild.role === "leader") {
          const glMembers = stmt.getGuildMembers.all(guildId);
          glMembers.forEach(m => {
            if (m.uid !== clientUid) _sendGuildLeft(m.uid, "dissolved");
            stmt.deleteGuildMember.run(guildId, m.uid);
          });
          stmt.deleteGuild.run(guildId);
          send(ws, { type: "guild_left", reason: "dissolved" });
        } else {
          stmt.deleteGuildMember.run(guildId, clientUid);
          send(ws, { type: "guild_left", reason: "left" });
          _broadcastGuildUpdate(guildId);
        }
        break;
      }

      // Kick a member
      case "guild_kick": {
        if (!clientUid) return;
        const { targetUid: gkTargetUid } = msg;
        if (!gkTargetUid || gkTargetUid === clientUid) break;
        const gkMyGuild = _getUidGuild(clientUid);
        if (!gkMyGuild) break;
        const guildId = gkMyGuild.guild_id;
        if (gkMyGuild.role !== "leader" && gkMyGuild.role !== "officer") break;
        const gkTarget = stmt.getGuildMember.get(guildId, gkTargetUid);
        if (!gkTarget) break;
        if (gkMyGuild.role === "officer" && gkTarget.role !== "member") {
          send(ws, { type: "guild_error", msg: "Officers can only kick members." });
          break;
        }
        if (gkTarget.role === "leader") break;
        stmt.deleteGuildMember.run(guildId, gkTargetUid);
        _sendGuildLeft(gkTargetUid, "kicked");
        _broadcastGuildUpdate(guildId);
        break;
      }

      // Set a member's role (leader only)
      case "guild_set_role": {
        if (!clientUid) return;
        const { targetUid: gsrUid, role: gsrRole } = msg;
        if (!gsrUid || !gsrRole) break;
        if (!["officer","member"].includes(gsrRole)) break;
        const gsrMyGuild = _getUidGuild(clientUid);
        if (!gsrMyGuild || gsrMyGuild.role !== "leader") break;
        const guildId = gsrMyGuild.guild_id;
        const gsrTarget = stmt.getGuildMember.get(guildId, gsrUid);
        if (!gsrTarget || gsrTarget.role === "leader") break;
        if (gsrRole === "officer") {
          const gsrOfficerCnt = stmt.countGuildOfficers.get(guildId);
          if (gsrOfficerCnt && gsrOfficerCnt.cnt >= GUILD_MAX_OFFICERS) {
            send(ws, { type: "guild_error", msg: `Max ${GUILD_MAX_OFFICERS} officers allowed.` });
            break;
          }
        }
        stmt.updateGuildRole.run(gsrRole, guildId, gsrUid);
        _broadcastGuildUpdate(guildId);
        break;
      }

      // Transfer guild leadership to another member (leader only)
      case "guild_transfer_leadership": {
        if (!clientUid) return;
        const { targetUid: gtlTargetUid } = msg;
        if (!gtlTargetUid || gtlTargetUid === clientUid) break;
        const gtlMyGuild = _getUidGuild(clientUid);
        if (!gtlMyGuild || gtlMyGuild.role !== "leader") break;
        const guildId = gtlMyGuild.guild_id;
        const gtlTarget = stmt.getGuildMember.get(guildId, gtlTargetUid);
        if (!gtlTarget) { send(ws, { type: "guild_error", msg: "That player is not in your guild." }); break; }
        // Promote target to leader, demote current leader to officer
        stmt.updateGuildRole.run("leader", guildId, gtlTargetUid);
        stmt.updateGuildRole.run("officer", guildId, clientUid);
        stmt.updateGuildLeader.run(gtlTargetUid, guildId);
        _broadcastGuildUpdate(guildId);
        break;
      }

      // Fetch guild data (for login / reconnect)
      case "guild_fetch": {
        if (!clientUid) return;
        const gfMyGuild = _getUidGuild(clientUid);
        if (!gfMyGuild) {
          send(ws, { type: "guild_update", guild: null });
          break;
        }
        const gfRow = stmt.getGuild.get(gfMyGuild.guild_id);
        if (!gfRow) {
          send(ws, { type: "guild_update", guild: null });
          break;
        }
        const gfMembers = stmt.getGuildMembers.all(gfMyGuild.guild_id);
        send(ws, {
          type: "guild_update",
          guild: {
            id: gfRow.id,
            name: gfRow.name,
            leaderUid: gfRow.leader_uid,
            members: gfMembers.map(m => ({
              uid: m.uid,
              username: m.username,
              charName: m.char_name,
              role: m.role,
              joinedAt: m.joined_at
            }))
          }
        });
        break;
      }

      // Update char name in guild (called on login)
      case "guild_sync_charname": {
        if (!clientUid) return;
        const gscMyGuild = _getUidGuild(clientUid);
        if (!gscMyGuild) break;
        const gscSave = _getCachedSave(clientUid);
        const gscName = gscSave?.player?.name;
        if (!gscName) break;
        stmt.updateGuildCharName.run(gscName, gscMyGuild.guild_id, clientUid);
        break;
      }

      // Check if guild name is available
      // ── Send a guild invite ─────────────────────────────────────────────
      // Any current guild member (not just leader/officer) may invite, same
      // permission model as send_party_invite. Invites are stored one-per-
      // guild-per-recipient (keyed by guildId) in the recipient's inbox under
      // category "ginvites" — re-inviting the same person to the same guild
      // just refreshes the existing entry rather than piling up duplicates.
      case "guild_invite": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "invite")) return;
        const { targetUid: giTargetUid } = msg;
        if (!giTargetUid || giTargetUid === clientUid) break;
        const giMyGuild = _getUidGuild(clientUid);
        if (!giMyGuild) { send(ws, { type:"guild_error", msg:"You are not in a guild." }); break; }
        const guildId = giMyGuild.guild_id;
        const giGuildRow = stmt.getGuild.get(guildId);
        if (!giGuildRow) break; // guild vanished underneath us — nothing to invite to
        const giTargetGuild = _getUidGuild(giTargetUid);
        if (giTargetGuild) { send(ws, { type:"guild_error", msg:"That player is already in a guild." }); break; }
        const giMemberCount = stmt.getGuildMembers.all(guildId).length;
        if (giMemberCount >= GUILD_MAX_MEMBERS) { send(ws, { type:"guild_error", msg:`Guild is full (max ${GUILD_MAX_MEMBERS}).` }); break; }
        const giSenderClient = clients.get(clientUid);
        const giSenderUsername = giSenderClient ? giSenderClient.username : "";
        const giSenderSave = _getCachedSave(clientUid);
        const giSenderCharName = giSenderSave?.player?.name || giSenderUsername;
        stmt.upsertInbox.run(giTargetUid, "ginvites", guildId, JSON.stringify({
          guildId, guildName: giGuildRow.name,
          fromUid: clientUid, fromUsername: giSenderUsername, fromCharName: giSenderCharName,
          t: Date.now(),
        }), Date.now());
        _broadcastInbox(giTargetUid);
        send(ws, { type:"guild_invite_sent", targetUid: giTargetUid });
        break;
      }

      // ── Accept a guild invite ────────────────────────────────────────────
      // Re-validates everything server-side at accept time (not just at
      // invite time) since time may have passed: the invite could be stale,
      // the invitee may have joined another guild since, or the target guild
      // may have filled up or been dissolved.
      case "guild_invite_accept": {
        if (!clientUid) return;
        const { guildId: giaGuildId } = msg;
        if (!giaGuildId) break;
        const giaInvite = (dbGetInboxObject(clientUid).ginvites || {})[giaGuildId];
        if (!giaInvite) { send(ws, { type:"guild_error", msg:"That invite is no longer valid." }); break; }
        if (_getUidGuild(clientUid)) { send(ws, { type:"guild_error", msg:"You are already in a guild." }); stmt.deleteInbox.run(clientUid, "ginvites", giaGuildId); break; }
        const giaGuildRow = stmt.getGuild.get(giaGuildId);
        if (!giaGuildRow) { send(ws, { type:"guild_error", msg:"That guild no longer exists." }); stmt.deleteInbox.run(clientUid, "ginvites", giaGuildId); break; }
        const giaMemberCount = stmt.getGuildMembers.all(giaGuildId).length;
        if (giaMemberCount >= GUILD_MAX_MEMBERS) { send(ws, { type:"guild_error", msg:`Guild is full (max ${GUILD_MAX_MEMBERS}).` }); stmt.deleteInbox.run(clientUid, "ginvites", giaGuildId); break; }
        const giaClient = clients.get(clientUid);
        const giaUsername = giaClient ? giaClient.username : "";
        const giaSave = _getCachedSave(clientUid);
        const giaCharName = giaSave?.player?.name || giaUsername;
        stmt.insertGuildMember.run(giaGuildId, clientUid, giaUsername, giaCharName, "member", Date.now());
        stmt.deleteInbox.run(clientUid, "ginvites", giaGuildId);
        // Clean up any OTHER pending guild invites this player had — they can
        // only be in one guild, so leftover invites to other guilds are now moot.
        try {
          const giaOthers = db.prepare("SELECT entry_key FROM inbox WHERE target_uid=? AND category='ginvites'").all(clientUid);
          for (const row of giaOthers) stmt.deleteInbox.run(clientUid, "ginvites", row.entry_key);
        } catch(e) {}
        _broadcastGuildUpdate(giaGuildId);
        send(ws, { type:"guild_joined", guildId: giaGuildId, name: giaGuildRow.name });
        break;
      }

      // ── Decline / delete a guild invite (mirrors delete_party_invite) ───
      case "delete_guild_invite": {
        if (!clientUid) return;
        const { guildId: dgiGuildId } = msg;
        if (!dgiGuildId) break;
        stmt.deleteInbox.run(clientUid, "ginvites", dgiGuildId);
        break;
      }

      case "guild_check_name": {
        if (!clientUid) return;
        const { name: gcnName } = msg;
        if (!gcnName) break;
        const gcnNormalized = _toTitleCase(gcnName.trim());
        const valid = _guildNameValid(gcnNormalized);
        const taken = valid ? !!stmt.getGuildByName.get(gcnNormalized.toLowerCase()) : false;
        send(ws, { type: "guild_name_check", valid, taken, name: gcnNormalized });
        break;
      }

      case "ping": { send(ws, { type:"pong" }); break; } // lightweight latency probe

      default:
        if (clientUid) console.log(`[WS] unknown msg type="${msg.type}" from uid=${clientUid}`);
        break;
    }
    } catch (topErr) {
      console.error(`[WS] UNHANDLED ERROR in msg type="${msg.type}" uid=${clientUid}:`, topErr.message, topErr.stack);
      try { send(ws, { type:"zone_chat_msg", name:"[SRV-ERR]", msg:`${msg.type}: ${topErr.message}`, zone:"system" }); } catch(e) {}
    }
  });

  ws.on("close", (code, reason) => {
    if (clientUid) {
      console.log(`[DISCONNECT] uid=${clientUid} code=${code} reason=${reason||''}`);
      const client = clients.get(clientUid);
      const _uname = client ? client.username.toLowerCase() : null;
      // Flush session playtime to save before clearing cache
      if (client && client.loginAt) {
        try {
          const sessionSecs = Math.floor((Date.now() - client.loginAt) / 1000);
          if (sessionSecs > 0) {
            const ptSave = _getCachedSave(clientUid);
            if (ptSave && ptSave.player) {
              ptSave.player.totalPlaytime = (ptSave.player.totalPlaytime || 0) + sessionSecs;
              _writeSave(clientUid, ptSave, { skipOwnerSeed: true }); // playtime only
              console.log(`[PLAYTIME] uid=${clientUid} +${sessionSecs}s total=${ptSave.player.totalPlaytime}s`);
            }
          }
        } catch (e) { console.error("[PLAYTIME] flush error:", e.message); }
      }
      if (_uname) {
        _broadcastPresence(clientUid, _uname, false, null);
        _usernameToUid.delete(_uname);
      }
      _deleteFriends(clientUid);
      _setClientZone(clientUid, null); // evict from _zoneClients before clients.delete
      clients.delete(clientUid);
      _expectedGold.delete(clientUid);
      _invalidateSaveCache(clientUid);
      _clearItemOwners(clientUid);
      _clearHostileViewers(clientUid);
      _fishClearSession(clientUid);
      _fishRareLog.delete(clientUid);
      _rateBuckets.delete(clientUid);
      // 1v1 solo queue cleanup
      arenaQueues["1v1"] = arenaQueues["1v1"].filter(q => q.uid !== clientUid);
      // Team queue cleanup — if this uid is in a team entry, remove the whole team and notify teammates
      for (const mode of ["2v2","4v4"]) {
        const kept = [];
        for (const entry of arenaQueues[mode]) {
          if (entry.members && entry.members.some(m => m.uid === clientUid)) {
            // Boot the whole team
            for (const m of entry.members) {
              _arenaQueueUids.delete(m.uid);
              if (m.uid !== clientUid) {
                const mc = clients.get(m.uid);
                if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_queue_left", reason:"teammate_disconnected", mode });
              }
            }
          } else { kept.push(entry); }
        }
        arenaQueues[mode] = kept;
      }
      _arenaQueueUids.delete(clientUid);
      // Cancel any pending arena match confirmation this player was part of
      for (const [matchId, pm] of _pendingMatches) {
        if (pm.allUids.includes(clientUid)) {
          clearTimeout(pm.timer);
          _pendingMatches.delete(matchId);
          pm.declined.add(clientUid);
          const acceptedUids = [...pm.accepted];
          const declinedUids = [...pm.declined];
          console.log(`[ARENA] pending match ${matchId} cancelled — ${clientUid} disconnected`);
          _cancelPendingMatch(pm, acceptedUids, declinedUids, "disconnected");
          break;
        }
      }
      // Do not remove partySubscriptions here — the player
      // may reconnect within seconds. The subscription is re-validated on party_subscribe
      // or auth_ok. If they never reconnect, stale party cleanup will eventually prune it.
    }
  });

  ws.on("error", (err) => { console.error(`[WS ERROR] uid=${clientUid||'unauthed'} ${err.message}`); });
});

// ── Helpers ──────────────────────────────────────────────────────────────────
function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
// Broadcast a single item's stock update to all connected clients
function _broadcastStockUpdate(itemId, qty) {
  const msg = JSON.stringify({ type: "market_stock", stock: { [itemId]: qty } });
  for (const client of clients.values()) {
    if (client.inMarket && client.ws && client.ws.readyState === 1) client.ws.send(msg);
  }
}
// ── Room reverse index ────────────────────────────────────────────────────────
// _uidToRoom: uid -> CombatRoom | PvPCombatRoom
// Maintained by room.start() (add) and _endCombat (delete).
// Turns findRoomForUid from O(rooms × members) to O(1).
const _uidToRoom = new Map();

function findRoomForUid(uid) {
  const room = _uidToRoom.get(uid);
  return (room && !room.ended) ? room : null;
}
function _inArenaQueue(uid) {
  return _arenaQueueUids.has(uid);
}

// ── Pending match confirm/cancel/start helpers ────────────────────────────────
function _expirePendingMatch(matchId) {
  const pm = _pendingMatches.get(matchId);
  if (!pm) return;
  _pendingMatches.delete(matchId);
  const acceptedUids = [...pm.accepted];
  const declinedUids = pm.allUids.filter(uid => !pm.accepted.has(uid));
  console.log(`[ARENA] pending match ${matchId} expired — accepted:${acceptedUids.length}/${pm.allUids.length}`);
  _cancelPendingMatch(pm, acceptedUids, declinedUids, "timeout");
}

function _cancelPendingMatch(pm, acceptedUids, declinedUids, reason) {
  if (pm.mode === "1v1") {
    for (const uid of acceptedUids) {
      const entry = uid === pm.p1e.uid ? pm.p1e : pm.p2e;
      arenaQueues["1v1"].unshift(entry);
      _arenaQueueUids.add(uid);
      const mc = clients.get(uid);
      if (mc && mc.ws.readyState === 1) {
        send(mc.ws, { type:"arena_match_cancelled", reason, requeued: true });
        send(mc.ws, { type:"arena_queue_joined", mode:"1v1" });
      }
    }
    for (const uid of declinedUids) {
      const mc = clients.get(uid);
      if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_match_cancelled", reason, requeued: false });
    }
  } else {
    const mode = pm.mode;
    for (const team of [pm.team1, pm.team2]) {
      const teamUids = team.members.map(m => m.uid);
      const allAccepted = teamUids.every(uid => acceptedUids.includes(uid));
      if (allAccepted) {
        arenaQueues[mode].unshift(team);
        for (const m of team.members) {
          _arenaQueueUids.add(m.uid);
          const mc = clients.get(m.uid);
          if (mc && mc.ws.readyState === 1) {
            send(mc.ws, { type:"arena_match_cancelled", reason, requeued: true });
            send(mc.ws, { type:"arena_queue_joined", mode, team: true });
          }
        }
      } else {
        for (const m of team.members) {
          const mc = clients.get(m.uid);
          if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_match_cancelled", reason, requeued: false });
        }
      }
    }
  }
}

async function _startPendingMatch(pm) {
  const { mode } = pm;
  if (mode === "1v1") {
    const { p1e, p2e } = pm;
    const c1 = clients.get(p1e.uid);
    const c2 = clients.get(p2e.uid);
    if (c1) send(c1.ws, { type:"arena_match_found", opponentUid: p2e.uid, opponentName: p2e.name });
    if (c2) send(c2.ws, { type:"arena_match_found", opponentUid: p1e.uid, opponentName: p1e.name });
    if (rooms.size >= ROOMS_WARN) console.warn(`[ROOMS] high room count: ${rooms.size} (pve=${rooms.size - _pvpRoomCount} pvp=${_pvpRoomCount}) — possible leak`);
    try {
      const roomId = `arena_${p1e.uid}_${p2e.uid}_${Date.now()}`;
      const room = await PvPCombatRoom.create(roomId, p1e, p2e);
      rooms.set(roomId, room); _pvpRoomCount++;
      room.start();
      console.log(`[ARENA] 1v1 room created: ${p1e.name} vs ${p2e.name} (room=${roomId})`);
    } catch (e) {
      console.error("[ARENA] Failed to create PvP room:", e.message);
      if (c1) { arenaQueues["1v1"].unshift(p1e); _arenaQueueUids.add(p1e.uid); send(c1.ws, { type:"arena_queue_joined", mode:"1v1" }); }
      if (c2) { arenaQueues["1v1"].unshift(p2e); _arenaQueueUids.add(p2e.uid); send(c2.ws, { type:"arena_queue_joined", mode:"1v1" }); }
    }
  } else {
    const { team1, team2 } = pm;
    for (const m of team1.members) {
      const mc = clients.get(m.uid);
      if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_match_found", mode, opponentTeam: team2.members.map(x=>x.name), teamMode: true });
    }
    for (const m of team2.members) {
      const mc = clients.get(m.uid);
      if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_match_found", mode, opponentTeam: team1.members.map(x=>x.name), teamMode: true });
    }
    if (rooms.size >= ROOMS_WARN) console.warn(`[ROOMS] high room count: ${rooms.size} (pve=${rooms.size - _pvpRoomCount} pvp=${_pvpRoomCount}) — possible leak`);
    try {
      const roomId = `arena_${mode}_${team1.teamId}_${team2.teamId}_${Date.now()}`;
      const room = await PvPCombatRoom.createTeam(roomId, team1.members, team2.members, mode);
      rooms.set(roomId, room); _pvpRoomCount++;
      room.start();
      console.log(`[ARENA] ${mode} room created (room=${roomId})`);
    } catch (e) {
      console.error(`[ARENA] Failed to create ${mode} PvP room:`, e.message);
      for (const team of [team1, team2]) {
        arenaQueues[mode].unshift(team);
        for (const m of team.members) {
          _arenaQueueUids.add(m.uid);
          const mc = clients.get(m.uid);
          if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_queue_joined", mode, team: true });
        }
      }
    }
  }
}

// ── Arena matchmaking ────────────────────────────────────────────────────────
// Rating window expands over time:
//   0–20s  → 100 pts max diff
//   20–30s → 200 pts max diff
//   30s+   → 200 + 50 * floor((elapsed - 30000) / 10000)
function _arenaWindow(queuedAt) {
  const elapsed = Date.now() - queuedAt;
  if (elapsed < 20000) return 100;
  if (elapsed < 30000) return 200;
  return 200 + 50 * Math.floor((elapsed - 30000) / 10000);
}

async function _tryArenaMatch(mode) {
  if (mode === "1v1") {
    // Filter disconnected players and keep _arenaQueueUids in sync — single pass
    const filtered = [];
    for (const q of arenaQueues["1v1"]) {
      const client = clients.get(q.uid);
      if (client && client.ws.readyState === 1) filtered.push(q);
      else _arenaQueueUids.delete(q.uid);
    }
    arenaQueues["1v1"] = filtered;
    if (arenaQueues["1v1"].length < 2) return;

    // Rating-aware match: scan oldest-first, find closest-rated partner within window
    const queue = arenaQueues["1v1"];
    let matched = false;
    for (let i = 0; i < queue.length && !matched; i++) {
      const p1 = queue[i];
      const window1 = _arenaWindow(p1.queuedAt || 0);
      let bestIdx = -1;
      let bestDiff = Infinity;
      for (let j = 0; j < queue.length; j++) {
        if (j === i) continue;
        const p2 = queue[j];
        const window2 = _arenaWindow(p2.queuedAt || 0);
        const maxWindow = Math.max(window1, window2);
        const diff = Math.abs((p1.rating ?? 1000) - (p2.rating ?? 1000));
        if (diff <= maxWindow && diff < bestDiff) { bestDiff = diff; bestIdx = j; }
      }
      if (bestIdx !== -1) {
        // Remove higher index first to preserve positions
        const idxA = Math.min(i, bestIdx);
        const idxB = Math.max(i, bestIdx);
        const [entryB] = queue.splice(idxB, 1);
        const [entryA] = queue.splice(idxA, 1);
        const p1e = i < bestIdx ? entryA : entryB;
        const p2e = i < bestIdx ? entryB : entryA;
        _arenaQueueUids.delete(p1e.uid);
        _arenaQueueUids.delete(p2e.uid);
        console.log(`[ARENA] 1v1 match found: ${p1e.name}(${p1e.rating ?? 1000}) vs ${p2e.name}(${p2e.rating ?? 1000}) diff=${Math.abs((p1e.rating ?? 1000) - (p2e.rating ?? 1000))} — awaiting confirm`);
        const c1 = clients.get(p1e.uid);
        const c2 = clients.get(p2e.uid);
        const matchId = `pm_1v1_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
        if (c1) send(c1.ws, { type:"arena_match_confirm", matchId, mode:"1v1", opponentName: p2e.name, opponentUid: p2e.uid, totalPlayers: 2 });
        if (c2) send(c2.ws, { type:"arena_match_confirm", matchId, mode:"1v1", opponentName: p1e.name, opponentUid: p1e.uid, totalPlayers: 2 });
        _pendingMatches.set(matchId, {
          mode: "1v1", p1e, p2e,
          allUids: [p1e.uid, p2e.uid],
          accepted: new Set(),
          declined: new Set(),
          timer: setTimeout(() => _expirePendingMatch(matchId), 20000)
        });
        matched = true;
      }
    }
    return;
  }

  // ── Team matchmaking (2v2, 4v4) ──
  if (mode !== "2v2" && mode !== "4v4") return;
  const teamQueue = arenaQueues[mode];

  // Filter teams that have any disconnected member
  const validTeams = [];
  for (const team of teamQueue) {
    const allConnected = team.members.every(m => {
      const mc = clients.get(m.uid);
      return mc && mc.ws.readyState === 1;
    });
    if (allConnected) {
      validTeams.push(team);
    } else {
      // Remove disconnected team from queue
      for (const m of team.members) {
        _arenaQueueUids.delete(m.uid);
        const mc = clients.get(m.uid);
        if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_queue_left", reason:"teammate_disconnected" });
      }
    }
  }
  arenaQueues[mode] = validTeams;
  if (validTeams.length < 2) return;

  // Rating-aware team match: oldest team tries to find closest total-rating opponent within window
  let matched = false;
  for (let i = 0; i < validTeams.length && !matched; i++) {
    const t1 = validTeams[i];
    const window1 = _arenaWindow(t1.queuedAt || 0);
    let bestIdx = -1;
    let bestDiff = Infinity;
    for (let j = 0; j < validTeams.length; j++) {
      if (j === i) continue;
      const t2 = validTeams[j];
      const window2 = _arenaWindow(t2.queuedAt || 0);
      const maxWindow = Math.max(window1, window2);
      const diff = Math.abs((t1.rating ?? 1000) - (t2.rating ?? 1000));
      if (diff <= maxWindow && diff < bestDiff) { bestDiff = diff; bestIdx = j; }
    }
    if (bestIdx !== -1) {
      const idxA = Math.min(i, bestIdx);
      const idxB = Math.max(i, bestIdx);
      const [entryB] = validTeams.splice(idxB, 1);
      const [entryA] = validTeams.splice(idxA, 1);
      const team1 = i < bestIdx ? entryA : entryB;
      const team2 = i < bestIdx ? entryB : entryA;
      arenaQueues[mode] = validTeams;
      for (const m of [...team1.members, ...team2.members]) _arenaQueueUids.delete(m.uid);
      const t1Names = team1.members.map(m => m.name).join(", ");
      const t2Names = team2.members.map(m => m.name).join(", ");
      console.log(`[ARENA] ${mode} match found: [${t1Names}](${team1.rating ?? 1000}) vs [${t2Names}](${team2.rating ?? 1000}) diff=${Math.abs((team1.rating ?? 1000) - (team2.rating ?? 1000))} — awaiting confirm`);
      const allTeamUids = [...team1.members.map(m => m.uid), ...team2.members.map(m => m.uid)];
      const matchId = `pm_${mode}_${Date.now()}_${Math.random().toString(36).slice(2,6)}`;
      // Notify all members of both teams
      for (const m of team1.members) {
        const mc = clients.get(m.uid);
        if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_match_confirm", matchId, mode, opponentTeam: team2.members.map(x=>x.name), teamMode: true, totalPlayers: allTeamUids.length });
      }
      for (const m of team2.members) {
        const mc = clients.get(m.uid);
        if (mc && mc.ws.readyState === 1) send(mc.ws, { type:"arena_match_confirm", matchId, mode, opponentTeam: team1.members.map(x=>x.name), teamMode: true, totalPlayers: allTeamUids.length });
      }
      _pendingMatches.set(matchId, {
        mode, team1, team2,
        allUids: allTeamUids,
        accepted: new Set(),
        declined: new Set(),
        timer: setTimeout(() => _expirePendingMatch(matchId), 20000)
      });
      matched = true;
    }
  }
}

// Periodic matchmaking scan — re-runs every 5s so expanding windows are checked
// even when no new players join the queue.
setInterval(() => {
  for (const mode of ["1v1", "2v2", "4v4"]) {
    if (arenaQueues[mode].length >= 2) {
      _tryArenaMatch(mode).catch(e => console.error("[ARENA] periodic scan error:", e.message));
    }
  }
}, 5000);

// ══════════════════════════════════════════════════════════════════════════════
//  Equipment passive effects helpers
// ══════════════════════════════════════════════════════════════════════════════

// Compiles all _effects from a player's equipped gear + accessories into a flat
// array. Attached to the combat member as `equipmentEffects` at room creation.
function _compileEquipmentEffects(eq) {
  const gear = eq.gear || null;
  const accessories = (eq.accessories || []).filter(Boolean);
  const allItems = gear ? [gear, ...accessories] : [...accessories];
  const compiled = [];
  for (const item of allItems) {
    if (!item || !item.id) continue;
    // Always resolve effects from the live ITEM_DB — equipped items in saves
    // only store id/name/stats, never _effects, so we must look them up here.
    const liveDef = ITEM_DB[item.id];
    const source = liveDef || item; // fallback to item itself for any future case
    if (!Array.isArray(source._effects) || source._effects.length === 0) continue;
    compiled.push(..._compileEffects(source));
  }
  return compiled;
}

// Fires on_enter_combat atoms from equipment effects for a member at room start.
// Accepts optional _log/_event helpers so effects (buffs, shields, etc.) are visible to clients.
function _fireEquipmentEnterCombat(room, member, _log, _event) {
  if (!Array.isArray(member.equipmentEffects) || member.equipmentEffects.length === 0) return;
  const synthAbility = { _effects: member.equipmentEffects };
  const _noop = () => {};
  _evalEffects(room, member, member, 'on_enter_combat', { ability: synthAbility, actionId: 'equipment_passive', wasHit: true }, _log || _noop, _event || _noop);
}

// Fires equipment effect atoms for a given trigger phase.
// caster  = member whose equipment is firing
// target  = the effect target (enemy or member depending on trigger)
// trigger = 'on_hit' | 'on_miss' | 'on_kill' | 'on_damage_taken'
function _evalEquipEffects(room, caster, target, trigger, _log, _event, extraContext) {
  if (!Array.isArray(caster.equipmentEffects) || caster.equipmentEffects.length === 0) return;
  const synthAbility = { _effects: caster.equipmentEffects };
  _evalEffects(room, caster, target, trigger, { ability: synthAbility, actionId: 'equipment_passive', wasHit: true, ...extraContext }, _log, _event);
}

// ══════════════════════════════════════════════════════════════════════════════
//  PvP COMBAT ROOM (unchanged logic)
// ══════════════════════════════════════════════════════════════════════════════
class PvPCombatRoom {
  static async create(roomId, p1Entry, p2Entry) {
    const members = [];
    for (const entry of [p1Entry, p2Entry]) {
      const save = _getCachedSave(entry.uid);
      const p = save?.player || {};
      const result = _validateSave(entry.uid, save);
      if (!result.valid) {
        result.anomalies.forEach(a => flagAnomaly(entry.uid, a.reason, a));
        if (Object.keys(result.fixes).length > 0) {
          _applyFixes(save, result.fixes);
          _writeSave(entry.uid, save);
          Object.assign(p, save?.player || {});
        }
      }
      _setExpectedGold(entry.uid, p.gold || 0);
      const eq = p.equipment || {};
      const gear = eq.gear || null;
      const accessories = (eq.accessories || []).filter(Boolean);
      members.push({
        uid: entry.uid, name: p.name || "Adventurer",
        hp: (p.baseMaxHp||100) + (gear?.maxHp||0) + accessories.reduce((s,a)=>s+((a?.maxHp)||0),0),
        maxHp: (p.baseMaxHp||100) + (gear?.maxHp||0) + accessories.reduce((s,a)=>s+((a?.maxHp)||0),0),
        baseMaxHp: p.baseMaxHp || 100,
        energy: 0, cooldowns: {},
        equipmentDmg: ((gear?.dmg)||0) + accessories.reduce((s,a)=>s+((a?.dmg)||0),0),
        equipmentAcc: ((gear?.acc)||0) + accessories.reduce((s,a)=>s+((a?.acc)||0),0),
        equipmentEffects: _compileEquipmentEffects(eq),
        gearType: gear?.gearType || null,
        learnedActions: p.learnedActions || [],
        inventory: p.inventory || {},
        gold: p.gold || 0,
        respawnZone: p.respawnZone || _defaultRespawnZone(),
        alive: true, fled: false,
      });
    }
    return new PvPCombatRoom(roomId, members[0], members[1]);
  }

  // ── Team arena: create a room from two arrays of member entries ──
  static async createTeam(roomId, team1Entries, team2Entries, mode) {
    const allEntries = [...team1Entries, ...team2Entries];
    const members = [];
    for (const entry of allEntries) {
      const save = _getCachedSave(entry.uid);
      const p = save?.player || {};
      const result = _validateSave(entry.uid, save);
      if (!result.valid) {
        result.anomalies.forEach(a => flagAnomaly(entry.uid, a.reason, a));
        if (Object.keys(result.fixes).length > 0) {
          _applyFixes(save, result.fixes);
          _writeSave(entry.uid, save);
          Object.assign(p, save?.player || {});
        }
      }
      _setExpectedGold(entry.uid, p.gold || 0);
      const eq = p.equipment || {};
      const gear = eq.gear || null;
      const accessories = (eq.accessories || []).filter(Boolean);
      members.push({
        uid: entry.uid, name: p.name || "Adventurer",
        hp: (p.baseMaxHp||100) + (gear?.maxHp||0) + accessories.reduce((s,a)=>s+((a?.maxHp)||0),0),
        maxHp: (p.baseMaxHp||100) + (gear?.maxHp||0) + accessories.reduce((s,a)=>s+((a?.maxHp)||0),0),
        baseMaxHp: p.baseMaxHp || 100,
        energy: 0, cooldowns: {},
        equipmentDmg: ((gear?.dmg)||0) + accessories.reduce((s,a)=>s+((a?.dmg)||0),0),
        equipmentAcc: ((gear?.acc)||0) + accessories.reduce((s,a)=>s+((a?.acc)||0),0),
        equipmentEffects: _compileEquipmentEffects(eq),
        gearType: gear?.gearType || null,
        learnedActions: p.learnedActions || [],
        inventory: p.inventory || {},
        gold: p.gold || 0,
        respawnZone: p.respawnZone || _defaultRespawnZone(),
        alive: true, fled: false,
        team: allEntries.indexOf(entry) < team1Entries.length ? 1 : 2,
      });
    }
    return new PvPCombatRoom(roomId, members[0], members[1], members, mode);
  }

  constructor(roomId, m1, m2, allMembers, mode) {
    this.partyId = roomId; this.isPvP = true;
    this.members = allMembers || [m1, m2];
    this.mode = mode || "1v1";
    this.logs = []; this.events = []; this.lgSeq = 0; this.evSeq = 0;
    this.combatAt = Date.now(); this.lastTickTs = this.combatAt;
    this.ticker = null; this.ended = false;
    this.dots = []; this.energyStops = []; this.energyOvertime = []; this.heals = [];
    this._snapP = {};
    this._allDisconnectedAt = null;
    // Slot map: built dynamically to support 2, 4, 8 members
    this.slotMap = this.members.map((m, i) => ({ i, uid: m.uid, name: m.name, t: "m", team: m.team || null }));
    this._uidToSlot = new Map(this.members.map((m, i) => [m.uid, i]));
    this._memberUidSet = new Set(this.members.map(m => m.uid));
    this.statuses = new Map();
    // Fire on_enter_combat equipment effects for all members (with event broadcasting)
    { const _ecLog = (tp, tx, au, vu) => this.seqLog(tp, tx, au, vu);
      const _ecEvent = (ev) => this.seqEvent(ev);
      for (const m of this.members) _fireEquipmentEnterCombat(this, m, _ecLog, _ecEvent); }
  }

  // Shared short ability name codes — identical to CombatRoom
  static _AN = { basic_attack:"ba", flee:"fl", provisions:"pr", dot:"dt" };

  // Identical to CombatRoom.seqLog — drops ph/pm/eh/em/hl (client generates from events)
  seqLog(type, text, au, vu) {
    if (type === "ph" || type === "pm" || type === "eh" || type === "em" || type === "hl") return null;
    this.lgSeq++;
    const entry = { sq:this.lgSeq, tp:type, tx:text };
    if (au) entry.au = au;
    if (vu) entry.vu = vu;
    this.logs.push(entry); if (this.logs.length > 50) this.logs.shift(); return entry;
  }

  // Identical to CombatRoom.seqEvent — UID storage + ef auto-assignment
  seqEvent(ev) {
    this.evSeq++;
    const stored = { sq: this.evSeq };
    const kindMap = { player_strike:"pa", enemy_strike:"ea", heal_tick:"ht", dot_tick:"dt", energy_gain:"eg" };
    stored.k = kindMap[ev.k] || ev.k;
    if (ev.au   != null) stored.au = ev.au;
    if (ev.auid != null) stored.au = ev.auid;
    if (ev.vu   != null) stored.vu = ev.vu;
    if (ev.an) stored.an = PvPCombatRoom._AN[ev.an] || ev.an;
    const isHit = ev.h === 1 || ev.h === true;
    const isHealTick = ev.k === "heal_tick";
    const isEnergyGain = ev.k === "energy_gain";
    if ((isHit || isHealTick || isEnergyGain) && ev.d != null) stored.d = ev.d;
    if (isHit) stored.h = 1;
    if (ev.dot != null) stored.dot = ev.dot;
    if (ev.ef != null) stored.ef = ev.ef;
    else if (ev.k === "heal_tick") stored.ef = "heal";
    else if (!isHit) stored.ef = "miss";
    else stored.ef = "dmg";
    if (ev.tx != null) stored.tx = ev.tx;
    this.events.push(stored); if (this.events.length > 100) this.events.shift();
    const { sq: _sq, ...live } = stored;
    return live;
  }

  hasMember(uid) { return this._memberUidSet.has(uid); }

  _sendToPlayer(playerUid, obj) { const client = clients.get(playerUid); if (client?.ws.readyState === 1) client.ws.send(JSON.stringify(obj)); }
  _sendTickToPlayer(playerUid, pkt) {
    const c = {};
    if (pkt.members) c.m = pkt.members; if (pkt.enemies) c.e = pkt.enemies;
    if (pkt.logs) c.l = pkt.logs; if (pkt.events) c.v = pkt.events;
    const client = clients.get(playerUid);
    if (client?.ws.readyState === 1) client.ws.send(JSON.stringify(c));
  }

  _projectAsEnemy(m) { return { uid: m.uid, type: "pvp_player", name: m.name, h: m.hp, mx: m.maxHp, al: m.alive ? 1 : 0, isPvP: true }; }
  _projectAsMember(m) {
    const eLock = this.energyStops.some(es => es.uid === m.uid) ? 1 : 0;
    return { uid: m.uid, name: m.name, h: m.hp, mx: m.maxHp, en: Math.floor(m.energy), al: m.alive ? 1 : 0, fl: m.fled ? 1 : 0, eLock };
  }

  _deltaForPlayer(player, opponent) {
    const sp = this._snapP[player.uid + "_self"] || {};
    const so = this._snapP[player.uid + "_opp"] || {};
    let mChanged = false, eChanged = false;
    const md = { uid: player.uid }; const ed = { uid: opponent.uid };
    if (sp.h !== player.hp) { md.h = player.hp; sp.h = player.hp; mChanged = true; }
    if (sp.mx !== player.maxHp) { md.mx = player.maxHp; sp.mx = player.maxHp; mChanged = true; }
    const enSelf = Math.floor(player.energy);
    if (Math.abs(enSelf - (sp.en||0)) >= 15 || ((sp.en||0) < 80 && enSelf >= 80) || ((sp.en||0) >= 80 && enSelf < 80)) { md.en = enSelf; sp.en = enSelf; mChanged = true; }
    const selfLock = this.energyStops.some(es => es.uid === player.uid) ? 1 : 0;
    if (sp.eLock !== selfLock) { md.eLock = selfLock; sp.eLock = selfLock; mChanged = true; }
    if (sp.al !== (player.alive?1:0)) { md.al = player.alive?1:0; sp.al = player.alive?1:0; mChanged = true; }
    if (so.h !== opponent.hp) { ed.h = opponent.hp; so.h = opponent.hp; eChanged = true; }
    if (so.mx !== opponent.maxHp) { ed.mx = opponent.maxHp; so.mx = opponent.maxHp; eChanged = true; }
    if (so.al !== (opponent.alive?1:0)) { ed.al = opponent.alive?1:0; so.al = opponent.alive?1:0; eChanged = true; }
    this._snapP[player.uid + "_self"] = sp; this._snapP[player.uid + "_opp"] = so;
    return { member: mChanged ? md : null, enemy: eChanged ? ed : null };
  }

  start() {
    // Send each member every opposing-team member as "enemies" — for 1v1
    // (team is unset on every member) this is exactly the one other player,
    // matching prior behaviour; for 2v2/4v4 this is every member of the
    // other team, letting the client's existing multi-target UI (the same
    // enemy-grid + ti/tu slot targeting used in PvE) pick any of them.
    for (const m of this.members) {
      const opponents = this.members.filter(x => x.uid !== m.uid && (m.team == null || x.team !== m.team));
      this._sendToPlayer(m.uid, {
        type: "combat_start", isPvP: true,
        members: [this._projectAsMember(m)],
        enemies: opponents.map(o => this._projectAsEnemy(o)),
        sm: this.slotMap.map(s => [s.uid, s.name, s.t]),
      });
    }
    for (const m of this.members) { _combatActiveUids.add(m.uid); _uidToRoom.set(m.uid, this); }
    this.ticker = setInterval(() => this._tick(), 1000);
  }

  sendFullState(uid) {
    const player = this.members.find(m => m.uid === uid);
    if (!player) return;
    // Same opposing-team resolution as start() — every other-team member,
    // not just a single hardcoded opponent.
    const opponents = this.members.filter(x => x.uid !== uid && (player.team == null || x.team !== player.team));
    if (opponents.length === 0) return;
    this._sendToPlayer(uid, {
      type: "full_state", isPvP: true,
      members: [this._projectAsMember(player)],
      enemies: opponents.map(o => this._projectAsEnemy(o)),
      logs: this.logs, events: this.events, combatAt: this.combatAt,
      sm: this.slotMap.map(s => [s.uid, s.name, s.t]),
    });
  }

  handleAction(uid, msg) {
    if (this.ended) return;
    const actionId = msg.a || msg.actionId || msg.action || "basic_attack";
    const member = this.members.find(m => m.uid === uid);
    if (!member || !member.alive) return;
    // Resolve every living opposing-team member (for 1v1, team is unset on
    // every member, so this is exactly the one other player — identical to
    // prior behaviour).
    const aliveOpponents = this.members.filter(m => m.alive && m.uid !== uid && (member.team == null || m.team !== member.team));
    if (aliveOpponents.length === 0) return;
    // Resolve the player-selected opponent via the same ti (slot index) /
    // tu (uid) protocol PvE already uses, instead of always grabbing
    // whichever other member happens to be first in the array. This is what
    // makes 2v2/4v4 actually target the chosen enemy instead of always the
    // same single hardcoded "opponent".
    let opponent = null;
    if (msg.ti != null) {
      const ti = msg.ti;
      if (typeof ti !== "number" || ti < 0 || ti >= this.slotMap.length) return;
      const slot = this.slotMap[ti];
      if (!slot) return;
      opponent = aliveOpponents.find(o => o.uid === slot.uid) || null;
    } else if (msg.tu != null) {
      opponent = aliveOpponents.find(o => o.uid === msg.tu) || null;
    }
    if (!opponent) opponent = aliveOpponents[0];
    const ability = ACTION_DB[actionId]; if (!ability) return;
    if (actionId === "provisions") { flagAnomaly(uid, "pvp_provision_attempt", { actionId }); return; }
    if (!member.learnedActions.includes(actionId)) { flagAnomaly(uid, "pvp_unlearned_action", { actionId }); return; }
    if (ability.requiresGearType && member.gearType !== ability.requiresGearType) return;
    if (member.energy < 80) return;
    if (ability.cooldown && member.cooldowns[actionId] && member.cooldowns[actionId] > Date.now()) return;
    const now = Date.now();
    member.energy -= 80;
    if (ability.cooldown) member.cooldowns[actionId] = now + ability.cooldown;
    const newLogs = []; const newEvents = [];
    const _log = (tp, tx, au, vu) => { const e = this.seqLog(tp, tx, au, vu); if (e !== null) newLogs.push(e); };
    const _event = (ev) => { const e = this.seqEvent(ev); newEvents.push(e); };
    _fireAmbient(this, member, "on_energy_spent", _log, _event);
    _fireAmbient(this, member, "on_energy_threshold", _log, _event, null, { metricValue: member.energy });
    if (!member._hasActed) { member._hasActed = true; _fireAmbient(this, member, "on_first_action", _log, _event); }
    // on_use fires unconditionally before the hit roll
    _evalEffects(this, member, member, "on_use", { ability, actionId, wasHit: true }, _log, _event);
    _evalEquipEffects(this, member, member, "on_use", _log, _event);
    _fireAmbient(this, member, "on_ability_use", _log, _event);
    const accMod = _getStatMod(this, member.uid, "acc");
    const adjustedDiff = Math.max(1, (ability.difficulty || 10) - (member.equipmentAcc || 0) - accMod);
    // Resolve every entity the base damage roll actually applies to —
    // honours area_enemy/area_all/random_enemy etc., not just the one
    // opponent. For single-target modes (enemy/self/ally/random_ally) this
    // is still exactly one entity, matching prior behaviour.
    const dmgTargets = _resolveTargets(this, member, opponent, ability.targeting || "enemy");
    const dmgMod = _getStatMod(this, member.uid, "dmgDealt") + _getStatMod(this, member.uid, "dmg");
    for (const dmgTarget of dmgTargets) {
      const dmgTargetAccMod = _getStatMod(this, dmgTarget.uid, "acc");
      const targetHit = rng() > Math.max(1, adjustedDiff + dmgTargetAccMod);
      if (targetHit) {
        const _baseDmg = roll(ability.damage[0], ability.damage[1]);
        const rawDmg = _baseDmg > 0 ? Math.max(1, _baseDmg + (member.equipmentDmg || 0) + dmgMod) : 0;
        const dmg = rawDmg > 0 ? _applyDamage(this, dmgTarget, rawDmg, _event, _log) : 0;
        _log("ph", `${member.name}'s ${ability.name} hits ${dmgTarget.name}` + (dmg > 0 ? ` for ${dmg} damage` : "") + `.`, member.uid);
        _event({ k:"player_strike", au:member.uid, vu:dmgTarget.uid, d:dmg, h:1, an:ability.name });
        // ── Effect atoms (Layers 1-4) replaces legacy boolean flags ──────────
        _evalEffects(this, member, dmgTarget, "on_hit", { ability, actionId, wasHit: true }, _log, _event);
        // Equipment on_hit effects
        _evalEquipEffects(this, member, dmgTarget, "on_hit", _log, _event);
        // Target's action _effects on_damage_taken (e.g. thorns, counter) — fire their currently equipped actions
        if (dmgTarget.learnedActions && dmgTarget.learnedActions.length > 0) {
          for (const oppActionId of dmgTarget.learnedActions) {
            const oppAbility = ACTION_DB[oppActionId];
            if (oppAbility && Array.isArray(oppAbility._effects) && oppAbility._effects.length > 0) {
              _evalEffects(this, dmgTarget, member, "on_damage_taken", { ability: oppAbility, actionId: oppActionId, wasHit: true }, _log, _event);
            }
          }
        }
        // Equipment on_damage_taken for the target (they took damage)
        _evalEquipEffects(this, dmgTarget, member, "on_damage_taken", _log, _event);
        if (!dmgTarget.alive) {
          // Action _effects on_kill (e.g. lifesteal, chain kill bonus)
          _evalEffects(this, member, dmgTarget, "on_kill", { ability, actionId, wasHit: true }, _log, _event);
          // Equipment on_kill effects for the attacker
          _evalEquipEffects(this, member, dmgTarget, "on_kill", _log, _event);
          _registerKill(this, member, _log, _event);
        }
      } else {
        _log("pm", `${member.name}'s ${ability.name} misses ${dmgTarget.name}.`, member.uid);
        _event({ k:"player_strike", au:member.uid, vu:dmgTarget.uid, d:0, h:0, an:ability.name });
        _evalEffects(this, member, dmgTarget, "on_miss", { ability, actionId, wasHit: false }, _log, _event);
        // Equipment on_miss effects
        _evalEquipEffects(this, member, dmgTarget, "on_miss", _log, _event);
      }
    }
    this._broadcastPvPTick(newLogs, newEvents);
    // End immediately on a kill — in 1v1 this is just the single opponent;
    // in team mode only end once the *entire* enemy team is dead (a downed
    // teammate of theirs doesn't end the match). The per-second _tick() wipe
    // check is a fallback for kills that happen via DoT ticks rather than a
    // direct hit, so this isn't the only place team wipes are detected.
    if (member.team == null) {
      if (!opponent.alive) this._endCombat(member, opponent);
    } else {
      const enemyTeamWiped = this.members.filter(m => m.team === opponent.team).every(m => !m.alive);
      if (enemyTeamWiped) this._endCombat(member, opponent);
    }
  }

  _tick() {
    if (this.ended) return;
    const now = Date.now();
    // Max combat duration: 30 minutes - draw, no stats recorded
    if (now - this.combatAt > 1800000) {
      const drawLog = this.seqLog("eh", "The arena match has timed out. Draw!");
      this._broadcastPvPTick(drawLog ? [drawLog] : [], []);
      this._endCombat(null, null, true);
      return;
    }
    // 1-minute auto-close if all players disconnected - loss for each
    const allOffline = this.members.every(m => {
      const c = clients.get(m.uid);
      return !c || c.ws.readyState !== 1;
    });
    if (allOffline) {
      if (!this._allDisconnectedAt) this._allDisconnectedAt = now;
      else if (now - this._allDisconnectedAt > 60000) {
        console.log(`[ARENA] All players disconnected for 1m - closing room ${this.partyId}`);
        this._endCombat(null, null, false);
        return;
      }
    } else {
      this._allDisconnectedAt = null; // someone reconnected, reset
    }
    const prevTickTs = this.lastTickTs || now;
    this.lastTickTs = now;
    const newLogs = []; const newEvents = [];
    const _log = (tp, tx, au, vu) => { const e = this.seqLog(tp, tx, au, vu); if (e !== null) newLogs.push(e); };
    const _event = (ev) => { const e = this.seqEvent(ev); newEvents.push(e); };
    // Timed effects (DoTs, HoTs, energy boosts) + onExpire chains
    _tickTimedEffects(this, now, _log, _event);
    // Energy regen — use elapsed-time catch-up (same as PvE) so a late tick
    // doesn't short-change energy gain. ticksThisFrame handles any drift.
    if (now - this.combatAt >= 3000) {
      const elapsed = now - prevTickTs;
      const ticksThisFrame = Math.max(1, Math.floor(elapsed / 1000));
      const gain = 10 * ticksThisFrame;
      this.members.forEach(m => { if (!m.alive) return; if (this.energyStops.some(es => es.uid === m.uid)) return; m.energy = clamp(m.energy + gain, 0, 100); });
    }
    this._broadcastPvPTick(newLogs, newEvents);
    if (this.mode === "1v1") {
      const dead = this.members.find(m => !m.alive);
      if (dead) { const winner = this.members.find(m => m.alive); this._endCombat(winner, dead); }
    } else {
      // Team mode: end when all members of a team are dead
      const teams = [...new Set(this.members.map(m => m.team))];
      const wipedTeam = teams.find(t => this.members.filter(m => m.team === t).every(m => !m.alive));
      if (wipedTeam !== undefined) {
        const winnerTeam = teams.find(t => t !== wipedTeam);
        const winnerMember = this.members.find(m => m.team === winnerTeam && m.alive);
        const loserMember = this.members.find(m => m.team === wipedTeam);
        this._endCombat(winnerMember, loserMember);
      }
    }
  }

  _broadcastPvPTick(newLogs, newEvents) {
    const remapEventsFor = (myUid) => newEvents.map(ev => {
      const pa = "pa", ea = "ea";
      if (ev.k === pa) return ev.au === myUid ? ev : { ...ev, k: ea };
      if (ev.k === ea) return ev.au === myUid ? { ...ev, k: pa } : ev;
      return ev;
    });
    const filterLogsFor = (myUid) => newLogs.filter(l => {
      if (l.au == null && l.vu == null) return true;
      return l.au === myUid || l.vu === myUid;
    });

    for (const m of this.members) {
      // Every opposing-team member: in 1v1 every member's team is unset, so
      // this is simply "every other member" (one player); in team mode it's
      // every member of the other team.
      const opponents = this.members.filter(x => x.uid !== m.uid && (m.team == null || x.team !== m.team));
      // Self/member delta only needs one opponent reference to compute against
      // (the member-half of _deltaForPlayer doesn't depend on which opponent
      // is passed) — the enemy-half is computed properly per-opponent below.
      const d = this._deltaForPlayer(m, opponents[0] || m);
      const oppDeltas = opponents.map(opp => this._deltaForPlayer(m, opp).enemy).filter(Boolean);
      const hasContent = d.member || oppDeltas.length > 0 || newEvents.length > 0 || newLogs.length > 0;
      if (!hasContent) continue;
      const pkt = { type: "tick" };
      if (d.member) pkt.members = [d.member];
      if (oppDeltas.length > 0) pkt.enemies = oppDeltas;
      const evs = remapEventsFor(m.uid);
      if (evs.length > 0) pkt.events = evs;
      const logs = filterLogsFor(m.uid);
      if (logs.length > 0) pkt.logs = logs;
      this._sendTickToPlayer(m.uid, pkt);
    }
  }

  async _endCombat(winner, loser, isDraw) {
    if (this.ended) return;
    this.ended = true;
    if (this.ticker) { clearInterval(this.ticker); this.ticker = null; }
    for (const m of this.members) { _combatActiveUids.delete(m.uid); _lastHpBroadcast.delete(m.uid); _uidToRoom.delete(m.uid); }
    // Ambient combat-flow triggers (Gear/Accessories only) — fired before the
    // save/persistence work below since none of it depends on combat-room state.
    {
      const _endLogs = []; const _endEvents = [];
      const _log = (tp, tx, au, vu) => { const e = this.seqLog(tp, tx, au, vu); if (e !== null) _endLogs.push(e); };
      const _event = (ev) => { const e = this.seqEvent(ev); _endEvents.push(e); };
      for (const m of this.members) {
        _fireAmbient(this, m, "on_exit_combat", _log, _event);
        if (!isDraw && winner) {
          const onWinnerTeam = this.mode === "1v1" ? (m.uid === winner.uid) : (m.team === winner.team);
          _fireAmbient(this, m, onWinnerTeam ? "on_combat_win" : "on_combat_loss", _log, _event);
        }
      }
      if (_endLogs.length || _endEvents.length) this._broadcastPvPTick(_endLogs, _endEvents);
    }
    // Collect mutations first, then commit all member saves in one transaction
    const _pvpSaves = [];
    for (const m of this.members) {
      try {
        const save = _getCachedSave(m.uid);
        if (!save?.player) continue;
        const p = save.player;
        p.hp = m.hp > 0 ? m.hp : 1;
        p.stats = p.stats || {};
        p.stats.arena = p.stats.arena || { wins1v1:0, losses1v1:0, wins2v2:0, losses2v2:0, wins4v4:0, losses4v4:0, rating1v1:1000, rating2v2:1000, rating4v4:1000 };
        // Ensure rating fields exist on older saves
        const _arenaMode = this.mode || "1v1";
        const _rKey = `rating${_arenaMode}`;
        if (p.stats.arena[_rKey] == null) p.stats.arena[_rKey] = 1000;
        const _wKey = `wins${_arenaMode}`;
        const _lKey = `losses${_arenaMode}`;
        p.stats.arena[_wKey] = p.stats.arena[_wKey] || 0;
        p.stats.arena[_lKey] = p.stats.arena[_lKey] || 0;
        // In team mode: winning team members all get a win; update ratings.
        // isDraw (the 30-minute stalemate timeout) is a true no-fault draw —
        // skip the win/loss/rating mutation entirely, matching the "no stats
        // recorded" comment at that call site. This is distinct from the
        // all-players-disconnected abandonment timeout, which explicitly
        // passes isDraw=false and intentionally keeps the loss-for-all below.
        if (isDraw) {
          // no stats change
        } else if (winner) {
          const onWinnerTeam = this.mode === "1v1" ? (m.uid === winner.uid) : (m.team === winner.team);
          if (onWinnerTeam) {
            p.stats.arena[_wKey] = (p.stats.arena[_wKey] || 0) + 1;
            p.stats.arena[_rKey] = Math.max(0, (p.stats.arena[_rKey] || 1000) + 10);
          } else {
            p.stats.arena[_lKey] = (p.stats.arena[_lKey] || 0) + 1;
            p.stats.arena[_rKey] = Math.max(0, (p.stats.arena[_rKey] || 1000) - 10);
          }
        } else {
          // No winner and not a true draw (e.g. mutual disconnect timeout) — loss for all
          p.stats.arena[_lKey] = (p.stats.arena[_lKey] || 0) + 1;
          p.stats.arena[_rKey] = Math.max(0, (p.stats.arena[_rKey] || 1000) - 10);
        }
        _saveCacheSet(m.uid, save);
        _setExpectedGold(m.uid, p.gold);
        _pvpSaves.push({ uid: m.uid, data: JSON.stringify(save) });
      } catch (e) { console.error(`[PVP SAVE] error uid=${m.uid}:`, e.message); }
    }
    if (_pvpSaves.length) {
      try {
        db.transaction(() => { for (const { uid, data } of _pvpSaves) stmt.upsertSave.run(uid, data); })();
      } catch (e) { console.error("[PVP SAVE] transaction error:", e.message); }
    }
    // Broadcast updated arena ratings to each player
    for (const m of this.members) {
      const mc = clients.get(m.uid);
      if (mc && mc.ws.readyState === 1) {
        const mSave = _getCachedSave(m.uid);
        const mArena = mSave?.player?.stats?.arena || {};
        send(mc.ws, { type: "arena_rating_update", arena: mArena });
      }
    }
    const [m1, m2] = this.members;
    if (this.mode === "1v1") {
      // 1v1: simple two-player end message
      const _endMsg = (player, opp) => ({ type: "combat_end", isPvP: true, outcome: isDraw ? "draw" : (winner?.uid === player.uid ? "victory" : "death"), forfeit: player.fled || undefined, goldEach: 0, kills: {}, respawnZone: player.respawnZone || _defaultRespawnZone(), members: [this._projectAsMember(player)], enemies: [this._projectAsEnemy(opp)] });
      this._sendToPlayer(m1.uid, _endMsg(m1, m2));
      this._sendToPlayer(m2.uid, _endMsg(m2, m1));
    } else {
      // Team mode: each member gets outcome based on their team
      for (const m of this.members) {
        const isWinner = winner ? (m.team === winner.team) : false;
        const outcome = isDraw ? "draw" : (isWinner ? "victory" : "death");
        const enemies = this.members.filter(x => x.team !== m.team).map(x => this._projectAsEnemy(x));
        const allies = this.members.filter(x => x.uid !== m.uid && x.team === m.team).map(x => this._projectAsMember(x));
        this._sendToPlayer(m.uid, { type: "combat_end", isPvP: true, outcome, forfeit: m.fled || undefined, goldEach: 0, kills: {}, respawnZone: m.respawnZone || _defaultRespawnZone(), members: [this._projectAsMember(m), ...allies], enemies });
      }
    }
    console.log(`[ARENA] ${this.mode||"1v1"} ${isDraw ? "draw" : (winner?.name||"no winner") + " team defeats " + (loser?.name||"no loser")} (room=${this.partyId})`);
    setTimeout(() => { if (rooms.delete(this.partyId)) _pvpRoomCount = Math.max(0, _pvpRoomCount - 1); }, 10000);
  }

  handleFlee(uid) {
    if (this.ended) return;
    const fleer = this.members.find(m => m.uid === uid); if (!fleer || !fleer.alive) return;
    fleer.alive = false; fleer.fled = true;
    const fleeLogs = [];
    const e1 = this.seqLog("ci", `Fled from the arena.`, fleer.uid);
    if (e1) fleeLogs.push(e1);
    // Notify every opposing-team member (1v1: the single opponent; team
    // mode: the whole enemy team) that this player has forfeited.
    const opponents = this.members.filter(m => m.uid !== uid && (fleer.team == null || m.team !== fleer.team));
    for (const opp of opponents) {
      const e2 = this.seqLog("ph", `${fleer.name} has forfeited. You win!`, opp.uid);
      if (e2) fleeLogs.push(e2);
    }
    this._broadcastPvPTick(fleeLogs, []);
    // End immediately only once the fleer's entire team has fled/died — a
    // single teammate fleeing in team mode doesn't end the match by itself.
    if (fleer.team == null) {
      this._endCombat(opponents[0] || null, fleer);
    } else {
      const fleerTeamGone = this.members.filter(m => m.team === fleer.team).every(m => !m.alive);
      if (fleerTeamGone) this._endCombat(opponents[0] || null, fleer);
    }
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  PvE COMBAT ROOM (unchanged logic, SQLite backend)
// ══════════════════════════════════════════════════════════════════════════════
class CombatRoom {
  constructor(partyId, members, enemies) {
    this.partyId = partyId; this.members = members; this.enemies = enemies;
    this.logs = []; this.events = []; this.lgSeq = 0; this.evSeq = 0;
    this.combatAt = Date.now(); this.lastTickTs = this.combatAt;
    this.ticker = null; this.ended = false;
    this.dots = []; this.energyStops = []; this.energyOvertime = []; this.heals = [];
    this.threat = {}; this._snapM = {}; this._snapE = {};
    this._clearedEnemies = [];
    this.slotMap = [
      ...members.map((m, i) => ({ i, uid: m.uid, name: m.name, t: "m" })),
      ...enemies.map((e, i) => ({ i: members.length + i, uid: e.uid, name: e.name, t: "e" })),
    ];
    this._uidToSlot = new Map(this.slotMap.map(s => [s.uid, s.i]));
    this._memberUidSet = new Set(members.map(m => m.uid));
    this.statuses = new Map();
    // Fire on_enter_combat equipment effects for all members (with event broadcasting)
    { const _ecLog = (tp, tx, au, vu) => this.seqLog(tp, tx, au, vu);
      const _ecEvent = (ev) => this.seqEvent(ev);
      for (const m of this.members) _fireEquipmentEnterCombat(this, m, _ecLog, _ecEvent); }
  }

  static async create(partyId, memberUids, enemyList) {
    const members = [];
    const ejectedUids = []; // UIDs with anomaly fixes — removed before combat starts
    for (const uid of memberUids) {
      const save = _getCachedSave(uid);
      const p = save?.player || {};
      const result = _validateSave(uid, save);
      if (!result.valid) {
        result.anomalies.forEach(a => flagAnomaly(uid, a.reason, a));
        if (Object.keys(result.fixes).length > 0) {
          _applyFixes(save, result.fixes);
          _writeSave(uid, save);
          _seedItemOwners(uid, save);
          // Eject this player — don't add to combat, handle after room is returned
          const respawnZone = (save.player?.respawnZone && ZONE_DB[save.player.respawnZone]?.safe)
            ? save.player.respawnZone : _defaultRespawnZone();
          ejectedUids.push({ uid, respawnZone, name: p.name || "Adventurer" });
          continue; // skip adding to members array
        }
      }
      _setExpectedGold(uid, p.gold || 0);
      const eq = p.equipment || {};
      const gear = eq.gear || null;
      const accessories = (eq.accessories || []).filter(Boolean);
      const equipmentDmg = ((gear?.dmg)||0) + accessories.reduce((s,a)=>s+((a?.dmg)||0),0);
      const equipmentAcc = ((gear?.acc)||0) + accessories.reduce((s,a)=>s+((a?.acc)||0),0);
      const equipmentMhp = (gear?.maxHp||0) + accessories.reduce((s,a)=>s+((a?.maxHp)||0),0);
      const baseMaxHp = p.baseMaxHp || 100;
      const maxHp = baseMaxHp + equipmentMhp;
      members.push({ uid, name: p.name || "Adventurer", hp: Math.min(p.hp != null ? p.hp : maxHp, maxHp), maxHp, baseMaxHp, energy: 0, cooldowns: {}, equipmentDmg, equipmentAcc, equipmentEffects: _compileEquipmentEffects(eq), gearType: gear?.gearType || null, learnedActions: [...new Set(["basic_attack", "flee", "provisions", ...(p.learnedActions || [])])], inventory: p.inventory || {}, gold: p.gold || 0, respawnZone: p.respawnZone || _defaultRespawnZone(), alive: true });
      // Sync maxHp to save if equipment changed it
      if (p.maxHp !== maxHp) { save.player.maxHp = maxHp; _writeSave(uid, save, { skipOwnerSeed: true }); } // maxHp only
    }
    const enemies = enemyList.map((e, i) => {
      const def = ENEMY_DB[e.type];
      if (!def) return null; // hostile was deleted — skip it
      return { uid: e.uid || `${e.type}_${i}`, type: e.type, name: def.name, hp: def.maxHp, maxHp: def.maxHp, energy: 0, cooldowns: {}, attackDelay: null, alive: true };
    }).filter(Boolean);
    const room = new CombatRoom(partyId, members, enemies);
    room._ejectedUids = ejectedUids;
    return room;
  }

  hasMember(uid) { return this._memberUidSet.has(uid); }
  broadcast(obj) {
    const str = JSON.stringify(obj);
    const remoteUids = [];
    for (const m of this.members) {
      const client = clients.get(m.uid);
      if (client?.ws.readyState === 1) client.ws.send(str);
      else remoteUids.push(m.uid); // member is on another machine
    }
    if (remoteUids.length > 0 && redisPub) {
      _redisPublish({ t: "combat_msg", uids: remoteUids, msg: obj, _src: MY_MACHINE_ID });
    }
  }

  broadcastTick(pkt) {
    const c = {};
    if (pkt.members) c.m = pkt.members;
    if (pkt.enemies) c.e = pkt.enemies;
    // Strip sq from live logs — sq is only needed in full_state replay, not live ticks
    if (pkt.logs) c.l = pkt.logs.filter(Boolean).map(({ sq: _sq, ...rest }) => rest);
    if (pkt.events) c.v = pkt.events; // events already have sq stripped by seqEvent()
    const str = JSON.stringify(c);
    const remoteUids = [];
    for (const m of this.members) {
      const client = clients.get(m.uid);
      if (client?.ws.readyState === 1) client.ws.send(str);
      else remoteUids.push(m.uid);
    }
    // Fan out ticks to remote machines via Redis
    if (remoteUids.length > 0 && redisPub) {
      _redisPublish({ t: "combat_tick", uids: remoteUids, tick: c, _src: MY_MACHINE_ID });
    }
  }

  sendFullState(uid) {
    const client = clients.get(uid);
    const fullState = { type: "full_state", members: this.members.map(m => this._projectMember(m)), enemies: this.enemies.map(e => this._projectEnemy(e)), logs: this.logs, events: this.events, combatAt: this.combatAt, sm: this.slotMap.map(s => [s.uid, s.name, s.t]) };
    if (client?.ws.readyState === 1) {
      send(client.ws, fullState);
    } else if (redisPub) {
      // Member is on another machine — route full_state there
      _redisPublish({ t: "combat_msg", uids: [uid], msg: fullState, _src: MY_MACHINE_ID });
    }
  }

  _projectMember(m) { return { uid:m.uid, name:m.name, h:m.hp, mx:m.maxHp, en:m.energy, al:m.alive?1:0, fl:m.fled?1:0 }; }
  _projectEnemy(e) { return { uid:e.uid, type:e.type, name:e.name, h:e.hp, mx:e.maxHp, al:e.alive?1:0 }; }

  _deltaMember(m) {
    const si = this._uidToSlot.get(m.uid); if (si == null) return null;
    const s = this._snapM[m.uid] || {}; const d = { i: si }; let changed = false;
    if (s.h !== m.hp) { d.h = m.hp; s.h = m.hp; changed = true; }
    if (s.mx !== m.maxHp) { d.mx = m.maxHp; s.mx = m.maxHp; changed = true; }
    const enNow = Math.floor(m.energy); const enPrev = s.en || 0;
    if ((enPrev < ENERGY_TO_PLAYER && enNow >= ENERGY_TO_PLAYER) || (enPrev >= ENERGY_TO_PLAYER && enNow < ENERGY_TO_PLAYER) || Math.abs(enNow - enPrev) >= 25) { d.en = enNow; s.en = enNow; changed = true; }
    const locked = this.energyStops.some(es => es.uid === m.uid) ? 1 : 0;
    if (s.eLock !== locked) { d.eLock = locked; s.eLock = locked; changed = true; }
    if (s.al !== (m.alive?1:0)) { d.al = m.alive?1:0; s.al = m.alive?1:0; changed = true; }
    if (s.fl !== (m.fled?1:0)) { d.fl = m.fled?1:0; s.fl = m.fled?1:0; changed = true; }
    this._snapM[m.uid] = s; return changed ? d : null;
  }
  _deltaEnemy(e) {
    const si = this._uidToSlot.get(e.uid); if (si == null) return null;
    const s = this._snapE[e.uid] || {}; const d = { i: si }; let changed = false;
    if (s.h !== e.hp) { d.h = e.hp; s.h = e.hp; changed = true; }
    const enNow = Math.floor(e.energy); const enPrev = s.en || 0;
    if ((enPrev < ENERGY_TO_ACT && enNow >= ENERGY_TO_ACT) || (enPrev >= ENERGY_TO_ACT && enNow < ENERGY_TO_ACT) || Math.abs(enNow - enPrev) >= 25) { d.en = enNow; s.en = enNow; changed = true; }
    if (s.al !== (e.alive?1:0)) { d.al = e.alive?1:0; s.al = e.alive?1:0; changed = true; }
    this._snapE[e.uid] = s; return changed ? d : null;
  }

  // Short codes for action names sent over the wire
  static _AN = { basic_attack:"ba", flee:"fl", provisions:"pr", dot:"dt" };
  seqLog(type, text, au, vu) {
    // Drop log types that duplicate event data — client generates these from events
    if (type === "ph" || type === "pm" || type === "eh" || type === "em" || type === "hl") return null;
    this.lgSeq++;
    const entry = { sq:this.lgSeq, tp:type, tx:text };
    // Store UIDs directly — no slot compression
    if (au) entry.au = au;
    if (vu) entry.vu = vu;
    this.logs.push(entry); if (this.logs.length > 50) this.logs.shift(); return entry;
  }
  seqEvent(ev) {
    this.evSeq++;
    // Store full entry with sq for full_state replay on reconnect
    const stored = { sq: this.evSeq };
    const kindMap = { player_strike:"pa", enemy_strike:"ea", heal_tick:"ht", dot_tick:"dt", energy_gain:"eg" };
    stored.k = kindMap[ev.k] || ev.k;
    // Store UIDs directly — no slot compression
    if (ev.au   != null) stored.au = ev.au;
    if (ev.auid != null) stored.au = ev.auid; // normalise auid → au
    if (ev.vu   != null) stored.vu = ev.vu;
    if (ev.an) stored.an = CombatRoom._AN[ev.an] || ev.an;
    // Omit d and h on misses (h absent = miss, d absent = 0) — saves ~16 bytes per miss
    // heal_tick always carries its amount regardless of h flag
    const isHit = ev.h === 1 || ev.h === true;
    const isHealTick = ev.k === "heal_tick";
    const isEnergyGain = ev.k === "energy_gain";
    if ((isHit || isHealTick || isEnergyGain) && ev.d != null) stored.d = ev.d;
    if (isHit) stored.h = 1;
    if (ev.dot != null) stored.dot = ev.dot;
    // ef = effect type for client colour routing:
    // "dmg"    = damage to target        → gold if attacker, red if victim
    // "heal"   = healing to target/self  → gold if attacker, green if victim
    if (ev.ef != null) stored.ef = ev.ef;
    else if (ev.k === "heal_tick") stored.ef = "heal";
    else if (!isHit) stored.ef = "miss";
    else stored.ef = "dmg";
    // tx = optional custom log text template. Supports {attacker}, {victim}, {amount} tokens.
    // If omitted, client generates standard text from event kind + ability name.
    // Use for abilities with unique descriptions e.g. "Your Shadow Strike weakens {victim}."
    if (ev.tx != null) stored.tx = ev.tx;
    this.events.push(stored); if (this.events.length > 100) this.events.shift();
    // Live broadcast copy: omit sq (redundant during live play, only needed for reconnect replay)
    const { sq: _sq, ...live } = stored;
    return live;
  }

  start() {
    const _startMsg = { type: "combat_start", members: this.members.map(m => this._projectMember(m)), enemies: this.enemies.map(e => this._projectEnemy(e)), combatAt: this.combatAt, sm: this.slotMap.map(s => [s.uid, s.name, s.t]) };
    console.log(`[ROOM] broadcasting combat_start partyId=${this.partyId} size=${JSON.stringify(_startMsg).length}B members=${this.members.map(m=>m.uid)}`);
    this.broadcast(_startMsg);
    for (const m of this.members) { _combatActiveUids.add(m.uid); _uidToRoom.set(m.uid, this); }
    this.ticker = setInterval(() => this._tick(), TICK_MS);
  }

  _tick() {
    if (this.ended) return;
    try {
    const now = Date.now(); const elapsed = now - this.lastTickTs; this.lastTickTs = now;
    // Max combat duration: 30 minutes - enemies go berserk, party wiped
    if (now - this.combatAt > 1800000) {
      const berserkLogs = [];
      const _blog = (tp, tx) => { const e = this.seqLog(tp, tx); berserkLogs.push(e); };
      _blog("ci", "The enemies enter a frenzied rage. BERSERK!");
      this.members.forEach(m => {
        if (m.alive) {
          m.hp = 0; m.alive = false;
          _blog("eh", `${m.name} is overwhelmed and slain.`);
        }
      });
      _blog("ci", "Your party has been wiped. The battle lasted too long.");
      const dm = this.members.map(m => this._deltaMember(m)).filter(Boolean);
      const pkt = { type: "tick" }; if (dm.length > 0) pkt.members = dm; pkt.logs = berserkLogs; pkt.events = [];
      this.broadcastTick(pkt);
      this._endCombat("berserk");
      return;
    }
    const newLogs = []; const newEvents = [];
    const _log = (tp, tx, au, vu) => { const e = this.seqLog(tp, tx, au, vu); if (e !== null) newLogs.push(e); };
    const _event = (ev) => { const e = this.seqEvent(ev); newEvents.push(e); };
    // Timed effects (DoTs, HoTs, energy boosts) + onExpire chains
    _tickTimedEffects(this, now, _log, _event);
    // Energy regen
    const combatAge = now - this.combatAt;
    if (combatAge >= ENERGY_DELAY_MS) {
      const ticksThisFrame = Math.max(1, Math.floor(elapsed / TICK_MS));
      const gain = ENERGY_PER_TICK * ticksThisFrame;
      this.members.forEach(m => { if (!m.alive) return; if (this.energyStops.some(es => es.uid === m.uid)) return; m.energy = clamp(m.energy + gain, 0, 100); });
      this.enemies.forEach(e => { if (!e.alive) return; if (this.energyStops.some(es => es.uid === e.uid)) return; e.energy = clamp(e.energy + gain, 0, 100); });
    }
    // Enemy AI
    const alivePlayers = this.members.filter(m => m.alive);
    this.enemies.forEach(e => {
      if (!e.alive) return;
      if (e.energy < ENERGY_TO_ACT) return;
      if (!e.attackDelay) { e.attackDelay = now + Math.floor(Math.random() * 4001); return; }
      if (now < e.attackDelay) return;
      const availActions = ENEMY_DB[e.type]?.actions || ["basic_attack"];
      // Pick a random action that is not on cooldown; fallback to first available
      let aId = null;
      const shuffled = availActions.slice().sort(() => Math.random() - 0.5);
      for (const cId of shuffled) {
        const ccd = e.cooldowns[cId];
        if (!ccd || now >= ccd) { aId = cId; break; }
      }
      if (!aId) return; // all actions on cooldown
      const ability = ACTION_DB[aId] || ACTION_DB.basic_attack;
      e.energy = Math.max(0, e.energy - ENERGY_TO_ACT); e.attackDelay = null; e.cooldowns[aId] = now + (ability.cooldown || 0);
      if (alivePlayers.length === 0) return;
      // Threat-based targeting
      const eThreat = this.threat[e.uid] || {};
      let target;
      if (Object.keys(eThreat).length === 0) {
        const maxHp = Math.max(...alivePlayers.map(m => m.hp));
        const topHp = alivePlayers.filter(m => m.hp === maxHp);
        target = topHp[Math.floor(Math.random() * topHp.length)];
      } else {
        const withThreat = alivePlayers.map(m => ({ m, dmg: eThreat[m.uid] || 0 }));
        const maxDmg = Math.max(...withThreat.map(t => t.dmg));
        const topDmg = withThreat.filter(t => t.dmg === maxDmg);
        if (topDmg.length === 1) target = topDmg[0].m;
        else { const maxHp = Math.max(...topDmg.map(t => t.m.hp)); const topHp = topDmg.filter(t => t.m.hp === maxHp); target = topHp[Math.floor(Math.random() * topHp.length)].m; }
      }
      // Hostile ability on_use fires unconditionally before the hit roll
      _evalEffects(this, e, target, "on_use", { ability, actionId: aId, wasHit: true }, _log, _event);
      // Resolve every player this attack actually applies to — honours
      // area_enemy/area_all/random_enemy etc. For "enemy" (the default,
      // single-target mode) this is just the threat-selected `target`,
      // matching prior behaviour exactly.
      const hitTargets = _resolveTargets(this, e, target, ability.targeting || "enemy");
      const attackerAccMod = _getStatMod(this, e.uid, "acc");
      for (const hTarget of hitTargets) {
        // Hostile accuracy: attacker acc mod lowers difficulty (they hit easier),
        // but target (player) acc mod raises difficulty (harder to hit = evasion).
        const targetAccMod = _getStatMod(this, hTarget.uid, "acc");
        const hit = rng() > Math.max(1, (ability.difficulty || 10) - attackerAccMod + targetAccMod);
        if (hit) {
          const _enemyDmgMod = _getStatMod(this, e.uid, "dmgDealt") + _getStatMod(this, e.uid, "dmg");
          const _eBaseDmg = roll(ability.damage[0], ability.damage[1]);
          const rawDmg = _eBaseDmg > 0 ? Math.max(1, _eBaseDmg + _enemyDmgMod) : 0;
          const dmg = _applyDamage(this, hTarget, rawDmg, _event, _log);
          _log("eh", `${e.name}'s ${ability.name} hits ${hTarget.name}` + (dmg > 0 ? ` for ${dmg} damage` : "") + `.`, null, hTarget.uid);
          _event({ k:"enemy_strike", auid:e.uid, vu:hTarget.uid, d:dmg, h:1, an:ability.name });
          // Hostile ability _effects (DoTs, debuffs, etc.) fire on hit
          _evalEffects(this, e, hTarget, "on_hit", { ability, actionId: aId, wasHit: true }, _log, _event);
          // Equipment on_damage_taken effects for the player who was hit
          _evalEquipEffects(this, hTarget, e, "on_damage_taken", _log, _event);
          // Hostile ability on_kill _effects fire when the hit kills the player
          if (!hTarget.alive) {
            _evalEffects(this, e, hTarget, "on_kill", { ability, actionId: aId, wasHit: true }, _log, _event);
          }
        } else {
          _log("em", `${e.name}'s ${ability.name} misses ${hTarget.name}.`, null, hTarget.uid);
          _event({ k:"enemy_strike", auid:e.uid, vu:hTarget.uid, d:0, h:0, an:ability.name });
          // Hostile ability _effects on miss
          _evalEffects(this, e, hTarget, "on_miss", { ability, actionId: aId, wasHit: false }, _log, _event);
        }
      }
    });
    // Death checks
    this.enemies.forEach(e => { if (e.hp <= 0 && e.alive) { e.alive = false; } }); // client generates "X is slain!" from al:0 delta
    const aliveEnemies = this.enemies.filter(e => e.alive);
    const aliveMembers = this.members.filter(m => m.alive);
    // Broadcast
    const dm = this.members.map(m => this._deltaMember(m)).filter(Boolean);
    const de = this.enemies.map(e => this._deltaEnemy(e)).filter(Boolean);
    if (dm.length > 0 || de.length > 0 || newLogs.length > 0 || newEvents.length > 0) {
      const pkt = { type: "tick" }; if (dm.length > 0) pkt.members = dm; if (de.length > 0) pkt.enemies = de; if (newLogs.length > 0) pkt.logs = newLogs; if (newEvents.length > 0) pkt.events = newEvents;
      this.broadcastTick(pkt);
    }
    if (aliveEnemies.length === 0 && this.enemies.length > 0) { this._endCombat("victory"); return; }
    if (aliveMembers.length === 0) this._endCombat("death");
    } catch(e) { console.error('[ROOM] _tick error partyId='+this.partyId+':', e.message, e.stack); }
  }

  handleAction(uid, msg) {
    if (this.ended) return;
    // Queue actions if room is waiting for all members to reconnect after restore
    if (this._waitingForMembers) {
      if (!this._queuedActions) this._queuedActions = [];
      this._queuedActions.push({ uid, msg });
      return;
    }
    const member = this.members.find(m => m.uid === uid); if (!member || !member.alive) return;
    const aId = msg.a || "basic_attack";
    const ability = ACTION_DB[aId];
    if (!ability) return; // action was deleted/unpublished — reject silently
    if (ability.isFlee) return;
    // Validate player has learned this action
    if (!member.learnedActions.includes(aId)) { flagAnomaly(uid, "pve_unlearned_action", { actionId: aId }); return; }
    if (member.energy < ENERGY_TO_PLAYER) return;
    const cd = member.cooldowns[aId]; if (cd && Date.now() < cd) return;
    if (ability.requiresGearType && member.gearType !== ability.requiresGearType) return;
    const targetingMode = ability.targeting || "enemy";
    // Self/ally-only actions (heals, buffs) don't require a live enemy on the
    // field — only enemy/area_enemy/area_all/random_enemy do.
    const needsEnemyPresence = targetingMode === "enemy" || targetingMode === "area_enemy" || targetingMode === "area_all" || targetingMode === "random_enemy";
    const aliveEnemies = this.enemies.filter(e => e.alive);
    if (needsEnemyPresence && aliveEnemies.length === 0) return;
    // Resolve the single primary target (player-selected enemy slot, when relevant)
    let target = null;
    if (msg.ti != null) {
      const ti = msg.ti;
      // Bounds check + must resolve to an enemy slot (not a member slot)
      if (typeof ti !== "number" || ti < this.members.length || ti >= this.slotMap.length) return;
      const slot = this.slotMap[ti];
      if (!slot || slot.t !== "e") return;
      target = aliveEnemies.find(e => e.uid === slot.uid) || aliveEnemies[0] || null;
    } else if (aliveEnemies.length > 0) {
      target = aliveEnemies.find(e => e.uid === msg.tu) || aliveEnemies[0];
    }
    if (needsEnemyPresence && !target) return;
    member.energy = Math.max(0, member.energy - ENERGY_TO_PLAYER);
    member.cooldowns[aId] = Date.now() + (ability.cooldown || 0);
    const newLogs = []; const newEvents = [];
    const _log = (tp, tx, au, vu) => { const e = this.seqLog(tp, tx, au, vu); if (e !== null) newLogs.push(e); };
    const _event = (ev) => { const e = this.seqEvent(ev); newEvents.push(e); };
    _fireAmbient(this, member, "on_energy_spent", _log, _event);
    _fireAmbient(this, member, "on_energy_threshold", _log, _event, null, { metricValue: member.energy });
    if (!member._hasActed) { member._hasActed = true; _fireAmbient(this, member, "on_first_action", _log, _event); }
    // on_use fires unconditionally before the hit roll (setup effects, auras, etc.)
    _evalEffects(this, member, member, "on_use", { ability, actionId: aId, wasHit: true }, _log, _event);
    _evalEquipEffects(this, member, member, "on_use", _log, _event);
    _fireAmbient(this, member, "on_ability_use", _log, _event);
    // Resolve every entity the base damage roll actually applies to — honours
    // area_enemy/area_all/area_ally/random_enemy/self/ally, not just the one
    // enemy slot the player selected.
    const dmgTargets = _resolveTargets(this, member, target, targetingMode);
    const dmgMod = _getStatMod(this, member.uid, "dmgDealt") + _getStatMod(this, member.uid, "dmg");
    const attackerAccMod = _getStatMod(this, member.uid, "acc");
    // Each target gets its own independent accuracy roll. Attacker accuracy
    // (gear + acc stat_mod buffs) lowers difficulty = easier to hit.
    // Target's acc stat_mod raises difficulty = harder to hit (evasion).
    // Rolled per target so AoE swings can land on some targets and miss others.
    for (const dmgTarget of dmgTargets) {
      const targetAccMod = _getStatMod(this, dmgTarget.uid, "acc");
      const hit = rng() > Math.max(1, (ability.difficulty || 10) - (member.equipmentAcc || 0) - attackerAccMod + targetAccMod);
      const isEnemyTarget = this.enemies.includes(dmgTarget) || (dmgTarget && dmgTarget.uid && this.enemies.some(e => e.uid === dmgTarget.uid));
      if (hit) {
        const _baseDmgPvE = roll(ability.damage[0], ability.damage[1]);
        const rawDmg = _baseDmgPvE > 0 ? Math.max(1, _baseDmgPvE + member.equipmentDmg + dmgMod) : 0;
        // Use the authoritative enemies array entry (not a stale local ref) when the target is a hostile
        const eIdx = isEnemyTarget ? this.enemies.findIndex(e => e.uid === dmgTarget.uid) : -1;
        const authoritativeTarget = eIdx >= 0 ? this.enemies[eIdx] : dmgTarget;
        const dmg = rawDmg > 0 ? _applyDamage(this, authoritativeTarget, rawDmg, _event, _log) : 0;
        if (isEnemyTarget) {
          if (!this.threat[authoritativeTarget.uid]) this.threat[authoritativeTarget.uid] = {};
          this.threat[authoritativeTarget.uid][member.uid] = (this.threat[authoritativeTarget.uid][member.uid] || 0) + dmg;
        }
        _log("ph", `${member.name}'s ${ability.name} hits ${authoritativeTarget.name}` + (dmg > 0 ? ` for ${dmg} damage` : "") + `.`, member.uid);
        _event({ k:"player_strike", au:member.uid, vu:authoritativeTarget.uid, d:dmg, h:1, an:ability.name });
        // ── Effect atoms (Layers 1-4) replaces legacy boolean flags ──────────
        _evalEffects(this, member, authoritativeTarget, "on_hit", { ability, actionId: aId, wasHit: true }, _log, _event);
        // Equipment on_hit effects
        _evalEquipEffects(this, member, authoritativeTarget, "on_hit", _log, _event);
        // Enemy action _effects on_damage_taken (thorns, counter-attacks, etc. on hostiles)
        if (isEnemyTarget) {
          const enemyDef = ENEMY_DB[authoritativeTarget.type];
          if (enemyDef && Array.isArray(enemyDef.actions)) {
            for (const eActId of enemyDef.actions) {
              const eAbility = ACTION_DB[eActId];
              if (eAbility && Array.isArray(eAbility._effects) && eAbility._effects.length > 0) {
                _evalEffects(this, authoritativeTarget, member, "on_damage_taken", { ability: eAbility, actionId: eActId, wasHit: true }, _log, _event);
              }
            }
          }
        }
        if (!authoritativeTarget.alive) {
          // Action _effects on_kill (e.g. lifesteal, chain damage)
          _evalEffects(this, member, authoritativeTarget, "on_kill", { ability, actionId: aId, wasHit: true }, _log, _event);
          // Equipment on_kill effects when this hit kills the enemy
          _evalEquipEffects(this, member, authoritativeTarget, "on_kill", _log, _event);
          _registerKill(this, member, _log, _event);
        }
      } else {
        _log("pm", `${member.name}'s ${ability.name} misses ${dmgTarget.name}.`, member.uid);
        _event({ k:"player_strike", au:member.uid, vu:dmgTarget.uid, d:0, h:0, an:ability.name });
        _evalEffects(this, member, dmgTarget, "on_miss", { ability, actionId: aId, wasHit: false }, _log, _event);
        // Equipment on_miss effects
        _evalEquipEffects(this, member, dmgTarget, "on_miss", _log, _event);
      }
    }
    const _dm = this.members.map(m => this._deltaMember(m)).filter(Boolean);
    const _de = this.enemies.map(e => this._deltaEnemy(e)).filter(Boolean);
    const _pkt = { type: "tick" }; if (_dm.length > 0) _pkt.members = _dm; if (_de.length > 0) _pkt.enemies = _de; if (newLogs.length > 0) _pkt.logs = newLogs; if (newEvents.length > 0) _pkt.events = newEvents;
    this.broadcastTick(_pkt);
    if (this.enemies.filter(e => e.alive).length === 0) this._endCombat("victory");
  }

  handleFlee(uid) {
    if (this.ended) return;
    const member = this.members.find(m => m.uid === uid); if (!member || !member.alive) return;
    if (member.energy < ENERGY_TO_PLAYER) return;
    member.energy = Math.max(0, member.energy - ENERGY_TO_PLAYER);
    const escaped = ACTION_DB.flee.difficulty < rng();
    const newLogs = []; const newEvents = [];
    const _log = (tp, tx, au, vu) => { const e = this.seqLog(tp, tx, au, vu); if (e !== null) newLogs.push(e); };
    const _event = (ev) => { const e = this.seqEvent(ev); newEvents.push(e); };
    _fireAmbient(this, member, "on_energy_spent", _log, _event);
    _fireAmbient(this, member, "on_energy_threshold", _log, _event, null, { metricValue: member.energy });
    if (escaped) {
      member.alive = false; member.fled = true;
      _log("ci", `${member.name} fled from combat!`, member.uid);
      _event({ k:"player_strike", au:member.uid, vu:member.uid, d:0, h:1, an:"Flee", ef:"flee" });
      _fireAmbient(this, member, "on_combat_flee", _log, _event);
      _fireAmbient(this, member, "on_exit_combat", _log, _event);
      const _fm = this.members.map(m => this._deltaMember(m)).filter(Boolean);
      const _pkt = { type:"tick" };
      if (_fm.length > 0) _pkt.members = _fm;
      if (newLogs.length > 0) _pkt.logs = newLogs;
      if (newEvents.length > 0) _pkt.events = newEvents;
      this.broadcastTick(_pkt);
      const client = clients.get(uid); if (client) send(client.ws, { type:"fled" });
      if (this.members.filter(m => m.alive).length === 0) this._endCombat("flee");
    } else {
      _log("ff", `${member.name} failed to flee!`, member.uid);
      _event({ k:"player_strike", au:member.uid, vu:member.uid, d:0, h:0, an:"Flee", ef:"flee_fail" });
      const _fm = this.members.map(m => this._deltaMember(m)).filter(Boolean);
      const _pkt = { type:"tick" };
      if (_fm.length > 0) _pkt.members = _fm;
      if (newLogs.length > 0) _pkt.logs = newLogs;
      if (newEvents.length > 0) _pkt.events = newEvents;
      this.broadcastTick(_pkt);
    }
  }

  async _endCombat(outcome) {
    if (this.ended) return;
    // ── Subzone level progression ──────────────────────────────────────────────
    if (outcome === "victory" && this._subzone) {
      const sz = this._subzone;
      if (sz.currentLevel < sz.totalLevels) {
        // Level cleared — advance to next level
        sz.currentLevel++;
        const nextLvl = sz.levels[sz.currentLevel - 1];
        const nextEnemies = sz.buildLevelEnemies(nextLvl);
        // Always accumulate this level's enemies before advancing or falling through,
        // so loot is rolled for every enemy killed across all levels at final victory.
        if (!this._clearedEnemies) this._clearedEnemies = [];
        this._clearedEnemies.push(...this.enemies);
        if (nextEnemies.length) {
          // Notify all members: level complete, next level incoming in 3s
          this.broadcast({
            type: "subzone_level_complete",
            level: sz.currentLevel - 1,
            totalLevels: sz.totalLevels,
            nextLevel: sz.currentLevel,
            subzoneName: sz.subzoneName,
            subzoneType: sz.subzoneType,
            countdown: 3,
          });
          // Reset room state for next level (keep members, replace enemies)
          this.enemies = nextEnemies.map((e, i) => {
            const def = ENEMY_DB[e.type] || { name: e.type, maxHp: 100, actions: ["basic_attack"], loot: [] };
            return { uid: e.uid || `${e.type}_${i}`, type: e.type, name: def.name, hp: def.maxHp, maxHp: def.maxHp, energy: 0, cooldowns: {}, attackDelay: null, alive: true };
          });
          this.dots = []; this.energyStops = []; this.energyOvertime = []; this.heals = [];
          this.statuses = new Map(); // clear all effect statuses — enemies are replaced
          this.threat = {}; this._snapE = {}; this.logs = []; this.events = []; this.lgSeq = 0; this.evSeq = 0;
          // Rebuild slot map with new enemies
          this.slotMap = [
            ...this.members.map((m, i) => ({ i, uid: m.uid, name: m.name, t: "m" })),
            ...this.enemies.map((e, i) => ({ i: this.members.length + i, uid: e.uid, name: e.name, t: "e" })),
          ];
          this._uidToSlot = new Map(this.slotMap.map(s => [s.uid, s.i]));
          // Reset member energy for fresh start on next level
          this.members.forEach(m => { m.energy = 0; m.cooldowns = {}; });
          // Stop the ticker immediately to prevent re-entry into _endCombat
          // while the 3-second level transition countdown is in progress.
          clearInterval(this.ticker); this.ticker = null; this.ended = true; this._levelTransition = true;
          setTimeout(() => {
            // Guard: if room was ended externally (e.g. all members disconnected) don't restart
            this._levelTransition = false;
            if (this.members.filter(m => m.alive).length === 0) return;
            // Send full_state so clients see the new enemy list
            this.ended = false;
            for (const m of this.members) this.sendFullState(m.uid);
            this.ticker = setInterval(() => this._tick(), TICK_MS);
            console.log(`[SUBZONE] advancing to level ${sz.currentLevel}/${sz.totalLevels} partyId=${this.partyId}`);
          }, 3000);
          return; // Don't run normal endCombat logic
        }
      }
      // All levels cleared — fall through to normal victory with subzone_complete flag
      this.broadcast({
        type: "subzone_complete",
        subzoneName: sz.subzoneName,
        subzoneType: sz.subzoneType,
        totalLevels: sz.totalLevels,
      });
    }
    // ── Normal end combat ──────────────────────────────────────────────────────
    if (this.ended) return;
    this.ended = true; clearInterval(this.ticker);
    for (const m of this.members) { _combatActiveUids.delete(m.uid); _lastHpBroadcast.delete(m.uid); _uidToRoom.delete(m.uid); }
    // Ambient combat-flow triggers (Gear/Accessories only). Members who
    // already fled via handleFlee() got on_combat_flee there — guard with
    // !m.fled here so a solo flee doesn't double-fire it.
    {
      const _ecLogs = []; const _ecEvents = [];
      const _log = (tp, tx, au, vu) => { const e = this.seqLog(tp, tx, au, vu); if (e !== null) _ecLogs.push(e); };
      const _event = (ev) => { const e = this.seqEvent(ev); _ecEvents.push(e); };
      for (const m of this.members) {
        if (!m.fled) _fireAmbient(this, m, "on_exit_combat", _log, _event);
        if (outcome === "victory") _fireAmbient(this, m, "on_combat_win", _log, _event);
        else if (outcome === "death" || outcome === "berserk") _fireAmbient(this, m, "on_combat_loss", _log, _event);
        else if (outcome === "flee" && !m.fled) _fireAmbient(this, m, "on_combat_flee", _log, _event);
      }
      if (_ecLogs.length || _ecEvents.length) {
        const _ecPkt = { type: "tick" };
        if (_ecLogs.length) _ecPkt.logs = _ecLogs;
        if (_ecEvents.length) _ecPkt.events = _ecEvents;
        this.broadcastTick(_ecPkt);
      }
    }
    // Delete persisted room record if it exists (written on SIGTERM)
    try { stmt.deleteCombatRoom.run(this.partyId); } catch(e) {}
    console.log(`[ROOM] end partyId=${this.partyId} outcome=${outcome}`);
    const kills = {};
    const memberLoot = {}; // uid -> { gold, items: [{id,name,rarity,type,marketValue,qty}] }
    if (outcome === "victory") {
      // For dungeon/raid/trial: include all enemies from cleared levels + current level
      const _allLootEnemies = [...(this._clearedEnemies || []), ...this.enemies];
      _allLootEnemies.forEach(e => { kills[e.type] = (kills[e.type] || 0) + 1; });
      this.members.forEach(m => {
        let memberGold = 0;
        const droppedItems = []; // collapsed per-item-id drops for this member
        _allLootEnemies.forEach(e => {
          const def = ENEMY_DB[e.type]; if (!def) return;
          def.loot.forEach(entry => {
            if (!(rng() <= entry.chance)) return; // entry.chance is 0-100, rng() returns 1-100
            if (entry.type === "gold") {
              memberGold += roll(entry.qty[0], entry.qty[1]);
            } else if (entry.item) {
              // Resolve item definition — skip silently if unknown
              const itemDef = ITEM_DB[entry.item];
              if (!itemDef) return;
              const qty = roll(entry.qty[0], entry.qty[1]);
              // Collapse duplicate drops of the same item
              const existing = droppedItems.find(d => d.id === entry.item);
              if (existing) existing.qty += qty;
              else droppedItems.push({ id: entry.item, name: itemDef.name, rarity: itemDef.rarity || "common", type: itemDef.type || "material", marketValue: itemDef.marketValue || 0, qty });
            }
          });
        });
        memberLoot[m.uid] = { gold: memberGold, items: droppedItems };
      });
    }
    const isDeath = outcome === "death" || outcome === "berserk";
    const isRespawn = isDeath || outcome === "flee"; // flee also forces zone travel but preserves HP
    const _rawRespawn = this.members[0]?.respawnZone || _defaultRespawnZone();
    const _rzData = ZONE_DB[_rawRespawn];
    const leaderRespawn = (_rzData && _rzData.safe) ? _rawRespawn : _defaultRespawnZone();
    const _baseMsg = { type: "combat_end", outcome, kills, respawnZone: isRespawn ? leaderRespawn : undefined, members: this.members.map(mb => this._projectMember(mb)), enemies: this.enemies.map(e => this._projectEnemy(e)) };
    // Pre-update expected gold
    if (outcome === "victory") {
      for (const m of this.members) {
        try { const save = _getCachedSave(m.uid); const cg = (save?.player?.gold) || 0; _setExpectedGold(m.uid, cg + (memberLoot[m.uid]?.gold || 0)); } catch {}
      }
    }
    this.members.forEach(m => {
      const client = clients.get(m.uid); if (!client || client.ws.readyState !== 1) return;
      const loot = memberLoot[m.uid] || { gold: 0, items: [] };
      client.ws.send(JSON.stringify({ ..._baseMsg, goldEach: loot.gold, itemsEach: loot.items }));
    });
    // Write results to SQLite — collect all mutations then commit in one transaction
    const _pveSaves = [];
    const _pveLogs  = [];
    for (const m of this.members) {
      try {
        const save = _getCachedSave(m.uid); if (!save?.player) continue;
        const p = save.player;
        if (!p.respawnZone || !(ZONE_DB[p.respawnZone]?.safe)) p.respawnZone = _defaultRespawnZone();
        p.hp = isDeath ? 1 : Math.max(1, m.hp);
        if (isRespawn) p.lastZone = leaderRespawn;
        if (outcome === "victory" && memberLoot[m.uid]) {
          p.gold = (p.gold || 0) + memberLoot[m.uid].gold;
          // Apply material item drops to inventory
          const drops = memberLoot[m.uid].items || [];
          if (drops.length) {
            p.inventory = p.inventory || {};
            p.inventory.materials = p.inventory.materials || [];
            for (const drop of drops) {
              const invKey = getInvKey(drop); // routes to 'materials', 'provisions', etc.
              p.inventory[invKey] = p.inventory[invKey] || [];
              const idx = p.inventory[invKey].findIndex(it => it.id === drop.id);
              if (idx >= 0) p.inventory[invKey][idx] = { ...p.inventory[invKey][idx], qty: (p.inventory[invKey][idx].qty || 1) + drop.qty };
              else p.inventory[invKey].push({ id: drop.id, name: drop.name, rarity: drop.rarity, type: drop.type, marketValue: drop.marketValue, qty: drop.qty });
            }
          }
        }
        if (outcome === "victory") {
          p.stats = p.stats || {}; p.stats.kills = p.stats.kills || {};
          Object.entries(kills).forEach(([type, n]) => { p.stats.kills[type] = (p.stats.kills[type] || 0) + n; });
          const lootDetails = { kills, goldAfter: p.gold };
          if (memberLoot[m.uid]?.gold) lootDetails.gold = memberLoot[m.uid].gold;
          if (memberLoot[m.uid]?.items?.length) lootDetails.items = memberLoot[m.uid].items;
          _pveLogs.push({ uid: m.uid, action: "loot", details: lootDetails });
        }
        _saveCacheSet(m.uid, save);
        _setExpectedGold(m.uid, p.gold);
        _pveSaves.push({ uid: m.uid, data: JSON.stringify(save) });
      } catch (e) { console.error(`[SAVE] error uid=${m.uid}:`, e.message); }
    }
    if (_pveSaves.length) {
      try {
        db.transaction(() => {
          for (const { uid, data } of _pveSaves) stmt.upsertSave.run(uid, data);
          for (const { uid, action, details } of _pveLogs) {
            const c = clients.get(uid);
            stmt.insertTxLog.run(uid, action, JSON.stringify({ ...details, ip: c?.ip||c?.ws?._ip||"offline", deviceId: c?.deviceId||"", fingerprint: c?.fingerprint||"" }), Date.now());
          }
        })();
      } catch (e) { console.error("[SAVE] endCombat transaction error:", e.message); }
    }
    // Broadcast updated HP to party members via lightweight per-member patches
    try {
      const partyDoc = dbGetParty(this.partyId);
      if (partyDoc && partyDoc.members && partyDoc.members.length > 1) {
        for (const m of this.members) {
          const save = _getCachedSave(m.uid);
          if (save?.player) _broadcastPartyHpPatch(this.partyId, m.uid, save.player.hp, save.player.maxHp);
        }
      }
    } catch (e) { console.error(`[PARTY HP SYNC] error partyId=${this.partyId}:`, e.message); }
    setTimeout(() => rooms.delete(this.partyId), 10000);
  }
}

// ── Start server ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Project Void server listening on port ${PORT}`);
  console.log(`[VERSION] CLIENT_VERSION=${CLIENT_VERSION} — IMPORTANT: kill_timeout in fly.toml must be >= 25 seconds`);
  // Restore any combat rooms saved on previous SIGTERM
  _restoreCombatRoomsFromSQLite();
_redisConnect();
});

const ROOMS_WARN = 500;    // log a warning if combined room count exceeds this — likely a leak
let _pvpRoomCount = 0;     // maintained at PvP rooms.set / rooms.delete sites

// ── Rate limiter ─────────────────────────────────────────────────────────────
const _RL_WINDOW  = 5000;
const _RL_MAX_ACT = 30;     // max general actions per window
const _RL_MAX_INV = 10;     // max invite actions per window
const _RL_COMBAT_CD = 500;  // minimum ms between combat actions (prevents console spam)
const _rateBuckets = new Map();

// Ring-buffer factory — fixed-size circular array, zero allocations per call.
// head points to the next write slot; count tracks how many slots are filled.
function _makeRing(size) { return { buf: new Float64Array(size), head: 0, count: 0, size }; }
function _ringOk(ring, now, window) {
  // Evict the oldest slot if it has fallen outside the window
  if (ring.count === ring.size) {
    const oldest = ring.buf[(ring.head - ring.count + ring.size) % ring.size];
    if (now - oldest < window) return false; // ring full and oldest still in window
    ring.count--; // oldest has expired — evict it, freeing a slot
  }
  ring.buf[ring.head] = now;
  ring.head = (ring.head + 1) % ring.size;
  ring.count++;
  return true;
}

function _rateOk(uid, type) {
  const now = Date.now();
  if (!_rateBuckets.has(uid)) {
    _rateBuckets.set(uid, {
      ring:        _makeRing(_RL_MAX_ACT), // general action ring buffer
      invRing:     _makeRing(_RL_MAX_INV), // invite ring buffer
      fishRing:    _makeRing(40),          // fish_input ring buffer — generous for hold/release taps
      lastCombat:  0,
      lastAction:  0,
    });
  }
  const b = _rateBuckets.get(uid);
  if (type === "start_combat") { if (now - b.lastCombat < _RL_WINDOW) return false; b.lastCombat = now; return true; }
  // Social invite bucket — separate from general actions, no per-action cooldown.
  if (type === "invite") return _ringOk(b.invRing, now, _RL_WINDOW);
  // Fishing reel hold/release toggles — frequent by nature, own generous bucket,
  // no per-action cooldown (a real player can legitimately tap fast).
  if (type === "fish_input") return _ringOk(b.fishRing, now, _RL_WINDOW);
  // Combat action minimum cooldown — prevents firing faster than the UI allows
  if (type === "action") {
    if (now - b.lastAction < _RL_COMBAT_CD) return false;
    b.lastAction = now;
  }
  return _ringOk(b.ring, now, _RL_WINDOW);
}

// ── Ping/pong ────────────────────────────────────────────────────────────────
// 30s interval keeps connections alive through Fly proxy and mobile networks.
// Two consecutive missed pongs required before termination (60s total grace).
// Combat players are never terminated on missed pong — reset and retry instead.
const _pingInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) {
      if (ws._uid) {
        // O(1) Set lookup — _combatActiveUids is maintained by room start/end
        if (_combatActiveUids.has(ws._uid)) { ws.isAlive = true; ws._missedPings = 0; ws.ping(); return; }
      }
      // Two-strike rule: only terminate after 2 consecutive missed pongs
      ws._missedPings = (ws._missedPings || 0) + 1;
      if (ws._missedPings < 2) { ws.ping(); return; }
      ws.terminate();
      return;
    }
    ws._missedPings = 0;
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// ── Idle disconnect — safety net for connections that go silent ──────────────
// Primary offline detection: ws.on("close") fires _broadcastPresence(offline) immediately.
// This safety net catches edge cases where the client socket stays open but goes silent
// (e.g. phone sleeps without firing visibilitychange, or JS timer killed by OS).
// Client pings every 20s when active — 60s of silence = definitely gone.
const IDLE_TIMEOUT_MS = 60 * 1000;
const _idleInterval = setInterval(() => {
  const now = Date.now();
  wss.clients.forEach(ws => {
    if (!ws.lastActivity) return;
    if (now - ws.lastActivity < IDLE_TIMEOUT_MS) return;
    // Don't disconnect players in active combat — O(1) Set lookup instead of
    // O(players × rooms) scan. _combatActiveUids is maintained by room start/end.
    if (ws._uid && _combatActiveUids.has(ws._uid)) return;
    console.log(`[IDLE] Disconnecting idle client ip=${ws._ip||"?"}`);
    ws.terminate();
  });
}, 15000); // Check every 15s

// ── Purge stale in-memory maps ──────────────────────────────────────────────
setInterval(() => {
  const live = new Set(clients.keys());
  const liveUsernames = new Set([...clients.values()].map(c => c.username).filter(Boolean));
  for (const uid of _rateBuckets.keys())      { if (!live.has(uid)) _rateBuckets.delete(uid); }
  for (const uid of _exploreCooldowns.keys()) { if (!live.has(uid)) _exploreCooldowns.delete(uid); }
  for (const uid of _travelCooldowns.keys())  { if (!live.has(uid)) _travelCooldowns.delete(uid); }
  for (const uid of _fishCooldowns.keys())    { if (!live.has(uid)) _fishCooldowns.delete(uid); }
  for (const uid of _fishRareLog.keys())      { if (!live.has(uid)) _fishRareLog.delete(uid); }
  for (const uid of _fishReelSessions.keys()) { if (!live.has(uid)) _fishClearSession(uid); }
  // Fix 5: purge stale friend/username maps for abnormally disconnected clients
  // whose ws.on('close') fired before auth completed (clientUid was still null)
  for (const uid of [..._uidToFriends.keys()]) { if (!live.has(uid)) _deleteFriends(uid); }
  for (const [uname] of _usernameToUid)        { if (!liveUsernames.has(uname)) _usernameToUid.delete(uname); }
  // _combatActiveUids is maintained by start() (add) and _endCombat() (delete) — no scan needed.
}, 10 * 60 * 1000);

// ── Room leak sweep ──────────────────────────────────────────────────────────
// Runs every 5 minutes. Evicts rooms that are stuck and would never self-delete:
//   1. ended=true but still in the map (setTimeout eviction failed or was skipped)
//   2. ticker still running but all members are offline and have been for > 2 minutes
//      (combat end never fired — e.g. crash mid-tick or abnormal disconnect path)
// Never touches rooms with any live member. Zero player impact.
const ROOM_ORPHAN_MS = 2 * 60 * 1000; // grace period before evicting memberless rooms
setInterval(() => {
  if (!rooms.size) return;
  const now = Date.now();
  let evicted = 0;
  for (const [roomId, room] of rooms) {
    // Case 1: room is already ended but wasn't removed from the map
    if (room.ended) {
      if (room.ticker) { clearInterval(room.ticker); room.ticker = null; }
      rooms.delete(roomId);
      if (room.isPvP) _pvpRoomCount = Math.max(0, _pvpRoomCount - 1);
      evicted++;
      continue;
    }
    // Case 2: room is still "active" but every member has been offline long enough
    const hasLiveMember = (room.members || []).some(m => clients.has(m.uid));
    if (!hasLiveMember) {
      if (!room._allOfflineSince) {
        room._allOfflineSince = now; // start the grace clock
        continue;
      }
      if (now - room._allOfflineSince < ROOM_ORPHAN_MS) continue; // still within grace
      // Grace period expired — this room is orphaned
      room.ended = true;
      if (room.ticker) { clearInterval(room.ticker); room.ticker = null; }
      for (const m of (room.members || [])) {
        _combatActiveUids.delete(m.uid);
        _uidToRoom.delete(m.uid);
      }
      rooms.delete(roomId);
      if (room.isPvP) _pvpRoomCount = Math.max(0, _pvpRoomCount - 1);
      evicted++;
    } else {
      // At least one member is live — reset the offline clock if it was set
      if (room._allOfflineSince) room._allOfflineSince = null;
    }
  }
  if (evicted > 0) console.warn(`[ROOM SWEEP] Evicted ${evicted} orphaned room(s). Remaining: ${rooms.size}`);
  if (rooms.size >= ROOMS_WARN) console.warn(`[ROOMS] high room count after sweep: ${rooms.size} (pve=${rooms.size - _pvpRoomCount} pvp=${_pvpRoomCount})`);
}, 5 * 60 * 1000);

// ── Graceful shutdown ────────────────────────────────────────────────────────
let _shutdownInProgress = false;

function _broadcastToAll(obj) {
  const str = JSON.stringify(obj);
  for (const client of clients.values()) {
    try { if (client.ws.readyState === 1) client.ws.send(str); } catch(e) {}
  }
}

function _saveCombatRoomsToSQLite() {
  const now = Date.now();
  // Include rooms mid-level-transition: they have ended=true temporarily during the 3s countdown
  const activeRooms = [...rooms.values()].filter(r => !r.ended || (r._subzone && r._levelTransition));
  if (activeRooms.length === 0) { console.log("[SHUTDOWN] No active combat rooms to save."); return; }
  console.log(`[SHUTDOWN] Saving ${activeRooms.length} combat room(s) to SQLite...`);
  for (const room of activeRooms) {
    try {
      // Validate member data before saving
      const validMembers = room.members.filter(m => m && m.uid && m.hp != null && m.maxHp != null);
      const roomData = {
        partyId: room.partyId,
        isPvP: room.isPvP || false,
        members: validMembers.map(m => ({
          uid: m.uid, name: m.name, hp: m.hp, maxHp: m.maxHp, baseMaxHp: m.baseMaxHp,
          energy: m.energy, cooldowns: m.cooldowns, equipmentDmg: m.equipmentDmg, equipmentAcc: m.equipmentAcc,
          gearType: m.gearType, learnedActions: m.learnedActions, inventory: m.inventory,
          gold: m.gold, respawnZone: m.respawnZone, alive: m.alive, fled: m.fled || false,
        })),
        enemies: (room.enemies || []).map(e => ({
          uid: e.uid, type: e.type, name: e.name, hp: e.hp, maxHp: e.maxHp,
          energy: 0, cooldowns: {}, attackDelay: null, alive: e.alive, // reset timers on restore
        })),
        // Subzone progression state — needed to continue dungeon/raid/trial after restart
        subzone: room._subzone ? {
          zoneId: room._subzone.zoneId, subzoneId: room._subzone.subzoneId,
          subzoneType: room._subzone.subzoneType, subzoneName: room._subzone.subzoneName,
          levels: room._subzone.levels, currentLevel: room._subzone.currentLevel,
          totalLevels: room._subzone.totalLevels,
        } : null,
        clearedEnemies: (room._clearedEnemies || []).map(e => ({
          uid: e.uid, type: e.type,
        })),
        dots: room.dots || [],
        energyStops: (room.energyStops || []).map(es => ({ uid: es.uid, until: es.until })),
        energyOvertime: room.energyOvertime || [],
        heals: room.heals || [],
        lgSeq: room.lgSeq,
        evSeq: room.evSeq,
        combatAt: room.combatAt,
        // Save berserk timer: record how much time remains
        berserksAt: room.combatAt + 1800000,
      };
      stmt.saveCombatRoom.run(room.partyId, JSON.stringify(roomData), now);
      console.log(`[SHUTDOWN] Saved room partyId=${room.partyId} members=${validMembers.length}`);
    } catch(e) {
      console.error(`[SHUTDOWN] Failed to save room partyId=${room.partyId}:`, e.message);
    }
  }
}

async function _gracefulShutdown(signal) {
  if (_shutdownInProgress) return;
  _shutdownInProgress = true;
  console.log(`[SHUTDOWN] ${signal} received — starting 15s countdown`);

  // Step 1: Clear cleanup interval immediately so nothing gets pruned during shutdown
  clearInterval(_pingInterval);
  clearInterval(_idleInterval);

  // Step 2: Cancel all pending party votes and notify affected players
  try {
    const allParties = db.prepare("SELECT party_id, data FROM parties").all();
    for (const row of allParties) {
      try {
        const pd = JSON.parse(row.data);
        if (pd.voteRequest) {
          stmt.deleteVotes.run(row.party_id);
          // Notify members
          if (pd.members) {
            for (const m of pd.members) {
              if (m.uid) {
                const mc = clients.get(m.uid);
                try { if (mc?.ws.readyState === 1) mc.ws.send(JSON.stringify({ type:"zone_chat_msg", name:"⚠ SERVER", msg:"Vote cancelled: server update incoming.", zone:"system" })); } catch(e) {}
              }
            }
          }
        }
      } catch(e) {}
    }
  } catch(e) {}

  // Step 3: Clear arena queue and notify queued players
  for (const [mode, queue] of Object.entries(arenaQueues)) {
    for (const entry of queue) {
      try { const c = clients.get(entry.uid); if (c?.ws.readyState === 1) c.ws.send(JSON.stringify({ type:"arena_queue_left" })); } catch(e) {}
    }
    arenaQueues[mode] = [];
  }
  _arenaQueueUids.clear();

  // Allow a brief pause for any in-flight SQLite writes to complete
  await new Promise(r => setTimeout(r, 150));

  // Step 4: Broadcast countdown to all clients
  const shutdownAt = Date.now() + 15000;
  _broadcastAll({ type:"zone_chat_msg", name:"⚠ SERVER UPDATE", msg:"A server update is starting in 15 seconds...", zone:"__system__", isSystemUpdate: true, shutdownAt });

  const countdownTicks = [10, 5, 4, 3, 2, 1];
  for (const secs of countdownTicks) {
    const delayUntil = shutdownAt - (secs * 1000);
    const waitMs = Math.max(0, delayUntil - Date.now());
    await new Promise(r => setTimeout(r, waitMs));
    if (secs <= 5) {
      _broadcastAll({ type:"zone_chat_msg", name:"⚠ SERVER UPDATE", msg:`Server restarting in ${secs}...`, zone:"__system__", isSystemUpdate: true });
    }
  }

  // Wait until shutdownAt
  await new Promise(r => setTimeout(r, Math.max(0, shutdownAt - Date.now())));

  // Step 5: Broadcast client_update so clients know to reload
  _serverNeedsReload = true;
  _broadcastAll({ type:"client_update", serverVersion: CLIENT_VERSION });

  // Step 6: Save all active combat rooms to SQLite (each in own try/catch)
  _saveCombatRoomsToSQLite();

  // Step 7: Close all WS connections with code 1001 (Going Away) so clients
  // know this is intentional and can skip the 2-second reconnect delay
  for (const client of clients.values()) {
    try { if (client.ws.readyState === 1) client.ws.close(1001, "Server updating"); } catch(e) {}
  }

  // Step 8: Stop combat ticks
  for (const room of rooms.values()) {
    if (!room.ended) { room.ended = true; clearInterval(room.ticker); }
  }

  // Step 9: Shut down server
  // Flush any pending dirty saves and market stock writes before closing DB
  _flushSaveDirty();
  if (_stockDirty.size) {
    try {
      db.transaction(() => {
        for (const itemId of _stockDirty) _stmtUpsertStock.run(itemId, MARKET_STOCK.get(itemId) || 0);
      })();
      _stockDirty.clear();
    } catch(e) { console.error("[SHUTDOWN] stock flush error:", e.message); }
  }
  wss.close();
  db.close();
  server.close(() => { console.log("[SHUTDOWN] clean exit"); process.exit(0); });
  setTimeout(() => { console.log("[SHUTDOWN] force exit"); process.exit(1); }, 8000);
}
process.on("SIGTERM", () => _gracefulShutdown("SIGTERM"));
process.on("SIGINT",  () => _gracefulShutdown("SIGINT"));

// ── Stale party cleanup ─────────────────────────────────────────────────────
const STALE_PARTY_MS = 10 * 60 * 1000;
const SOLO_GHOST_PARTY_MS = 2 * 60 * 1000;
function _cleanStaleParties() {
  try {
    const now = Date.now();
    const cutoff = now - STALE_PARTY_MS;
    const stale = stmt.getStaleParties.all(cutoff);
    for (const row of stale) {
      if (rooms.has(row.party_id)) continue;
      stmt.deleteParty.run(row.party_id);
      stmt.deleteVotes.run(row.party_id);
      for (const [uid, pid] of partySubscriptions.entries()) {
        if (pid === row.party_id) partySubscriptions.delete(uid);
      }
      console.log(`[CLEANUP] Deleted stale party ${row.party_id}`);
    }
    // Dissolve solo ghost parties where the single member is offline.
    // SQL filters to member_count=1 rows — no JSON parsing needed.
    const soloParties = stmt.getSoloParties.all();
    for (const row of soloParties) {
      if (rooms.has(row.party_id)) continue;
      if (now - (row.updated_at || 0) < SOLO_GHOST_PARTY_MS) continue;
      const pd = dbGetParty(row.party_id);
      if (!pd) continue;
      const members = pd.members || [];
      if (members.length !== 1) continue;
      const soloUid = members[0].uid;
      if (!soloUid || clients.has(soloUid)) continue;
      console.log(`[CLEANUP] Dissolving solo ghost party ${row.party_id}`);
      _dissolveParty(row.party_id);
    }
  } catch (e) { console.error("[CLEANUP] stale party error:", e.message); }
}
// Jitter the interval start by up to 60s so multi-machine deployments don't
// all query and dissolve solo ghost parties at the exact same moment.
setTimeout(() => setInterval(_cleanStaleParties, 5 * 60 * 1000), Math.random() * 60000);
setTimeout(_cleanStaleParties, 30000);

// ── Zone chat pruning ────────────────────────────────────────────────────────
function _pruneZoneChat() {
  try {
    const cutoff = Date.now() - 3600000;
    const info = stmt.pruneZoneChat.run(cutoff);
    if (info.changes > 0) console.log(`[CHAT PRUNE] removed ${info.changes} old messages`);
  } catch (e) { console.error("[CHAT PRUNE] error:", e.message); }
}
setInterval(_pruneZoneChat, 600000);
setTimeout(_pruneZoneChat, 60000);
