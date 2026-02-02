const fs = require('fs');
const path = require('path');
const express = require('express');
const bodyParser = require('body-parser');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');

let Database;
try {
  Database = require('better-sqlite3');
} catch (e) {
  Database = null;
}

function getEnv(name, fallback) {
  const v = process.env[name];
  return (v === undefined || v === null || v === '') ? fallback : v;
}

function nowIso() {
  return new Date().toISOString();
}

function randomId() {
  return `${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}

function jsonError(res, status, error, extra) {
  res.status(status).json({ ok: false, error, ...(extra || {}) });
}

class IdentityStore {
  constructor(dbPath) {
    if (!Database) {
      throw new Error('better-sqlite3 not installed/available');
    }
    this.dbPath = dbPath;
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this._init();
  }

  _init() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user',
        created_at TEXT NOT NULL,
        last_login_at TEXT
      );

      CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        revoked_at TEXT,
        ip TEXT,
        ua TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id)
      );

      CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
      CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
    `);
  }

  hasAnyUser() {
    const row = this.db.prepare('SELECT COUNT(1) as c FROM users').get();
    return (row?.c || 0) > 0;
  }

  createUser({ username, password, role = 'admin' }) {
    const id = randomId();
    const passwordHash = bcrypt.hashSync(String(password), 10);
    const createdAt = nowIso();
    this.db
      .prepare('INSERT INTO users (id, username, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?)')
      .run(id, username, passwordHash, role, createdAt);
    return { id, username, role, createdAt };
  }

  getUserByUsername(username) {
    return this.db.prepare('SELECT * FROM users WHERE username = ?').get(username) || null;
  }

  getUserById(id) {
    return this.db.prepare('SELECT * FROM users WHERE id = ?').get(id) || null;
  }

  verifyPassword(user, password) {
    return bcrypt.compareSync(String(password), String(user.password_hash));
  }

  createSession({ userId, ttlSeconds = 60 * 60 * 24 * 7, ip, ua }) {
    const id = randomId();
    const createdAt = nowIso();
    const expiresAt = new Date(Date.now() + ttlSeconds * 1000).toISOString();
    this.db
      .prepare('INSERT INTO sessions (id, user_id, created_at, expires_at, ip, ua) VALUES (?, ?, ?, ?, ?, ?)')
      .run(id, userId, createdAt, expiresAt, ip || null, ua || null);
    return { id, userId, createdAt, expiresAt };
  }

  revokeSession(sessionId) {
    const revokedAt = nowIso();
    this.db.prepare('UPDATE sessions SET revoked_at = ? WHERE id = ?').run(revokedAt, sessionId);
    return { id: sessionId, revokedAt };
  }

  getSession(sessionId) {
    const row = this.db.prepare('SELECT * FROM sessions WHERE id = ?').get(sessionId);
    return row || null;
  }
}

function parseBearer(req) {
  const h = req.headers.authorization || '';
  const m = /^Bearer\s+(.+)$/i.exec(String(h));
  return m ? m[1] : '';
}

function start() {
  const port = Number(getEnv('AUTH_PORT', '5081'));
  const host = getEnv('AUTH_HOST', '127.0.0.1');
  const webRoot = getEnv('WEB_ROOT', path.join(__dirname, '079project_frontend', 'build'));
  const dbPath = getEnv('AUTH_DB_PATH', path.join(__dirname, 'runtime_store', 'auth.sqlite'));
  const jwtSecret = getEnv('AUTH_JWT_SECRET', 'dev-secret-change-me');
  const aiApiBase = getEnv('AI_API_BASE', 'http://127.0.0.1:5080');
  const tokenTtlSeconds = Number(getEnv('AUTH_TOKEN_TTL_SECONDS', String(60 * 60 * 24 * 7)));

  if (!fs.existsSync(path.dirname(dbPath))) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  }

  const store = new IdentityStore(dbPath);

  const app = express();
  app.use(bodyParser.json({ limit: '2mb' }));

  // Lightweight reverse proxy for /api/* to main AI process.
  // This avoids CORS and keeps frontend API_BASE as same-origin (:5081).
  app.all(/^\/api\/.*/, async (req, res) => {
    try {
      const base = aiApiBase.endsWith('/') ? aiApiBase.slice(0, -1) : aiApiBase;
      const targetUrl = new URL(`${base}${req.originalUrl}`);
      const headers = { ...req.headers };
      delete headers.host;

      let body;
      if (req.method !== 'GET' && req.method !== 'HEAD') {
        body = JSON.stringify(req.body ?? {});
      }

      const r = await fetch(targetUrl.toString(), {
        method: req.method,
        headers: {
          ...headers,
          'content-type': 'application/json'
        },
        body
      });

      const text = await r.text();
      res.status(r.status);
      // pass through content-type if present
      const ct = r.headers.get('content-type');
      if (ct) res.setHeader('content-type', ct);
      res.send(text);
    } catch (e) {
      jsonError(res, 502, 'ai-proxy-failed', { message: e.message });
    }
  });

  // health + config
  app.get('/auth/health', (req, res) => {
    res.json({ ok: true, ts: Date.now() });
  });

  app.get('/auth/config', (req, res) => {
    res.json({ ok: true, aiApiBase });
  });

  // First-run bootstrap: create initial admin if no users.
  app.post('/auth/bootstrap', (req, res) => {
    try {
      if (store.hasAnyUser()) {
        jsonError(res, 409, 'already-bootstrapped');
        return;
      }
      const username = String(req.body?.username || 'admin').trim();
      const password = String(req.body?.password || '').trim();
      if (!username) {
        jsonError(res, 400, 'username-required');
        return;
      }
      if (!password || password.length < 6) {
        jsonError(res, 400, 'password-too-short', { minLen: 6 });
        return;
      }
      const user = store.createUser({ username, password, role: 'admin' });
      res.json({ ok: true, user: { id: user.id, username: user.username, role: user.role } });
    } catch (e) {
      jsonError(res, 500, 'bootstrap-failed', { message: e.message });
    }
  });

  app.post('/auth/login', (req, res) => {
    try {
      const username = String(req.body?.username || '').trim();
      const password = String(req.body?.password || '');
      if (!username || !password) {
        jsonError(res, 400, 'username-password-required');
        return;
      }
      const user = store.getUserByUsername(username);
      if (!user || !store.verifyPassword(user, password)) {
        jsonError(res, 401, 'invalid-credentials');
        return;
      }
      const session = store.createSession({
        userId: user.id,
        ttlSeconds: tokenTtlSeconds,
        ip: req.ip,
        ua: req.headers['user-agent']
      });
      const token = jwt.sign(
        { sub: user.id, username: user.username, role: user.role, sid: session.id },
        jwtSecret,
        { expiresIn: tokenTtlSeconds }
      );
      res.json({
        ok: true,
        token,
        user: { id: user.id, username: user.username, role: user.role },
        expiresAt: session.expires_at
      });
    } catch (e) {
      jsonError(res, 500, 'login-failed', { message: e.message });
    }
  });

  app.post('/auth/logout', (req, res) => {
    try {
      const token = parseBearer(req);
      if (!token) {
        jsonError(res, 401, 'missing-token');
        return;
      }
      const payload = jwt.verify(token, jwtSecret);
      if (payload?.sid) {
        store.revokeSession(String(payload.sid));
      }
      res.json({ ok: true });
    } catch (e) {
      jsonError(res, 401, 'invalid-token', { message: e.message });
    }
  });

  app.get('/auth/me', (req, res) => {
    try {
      const token = parseBearer(req);
      if (!token) {
        jsonError(res, 401, 'missing-token');
        return;
      }
      const payload = jwt.verify(token, jwtSecret);
      const user = store.getUserById(String(payload.sub));
      if (!user) {
        jsonError(res, 401, 'user-not-found');
        return;
      }
      const session = payload?.sid ? store.getSession(String(payload.sid)) : null;
      if (session && session.revoked_at) {
        jsonError(res, 401, 'session-revoked');
        return;
      }
      res.json({ ok: true, user: { id: user.id, username: user.username, role: user.role } });
    } catch (e) {
      jsonError(res, 401, 'invalid-token', { message: e.message });
    }
  });

  // Serve CRA build (separate from main AI process)
  if (fs.existsSync(webRoot)) {
    app.use(express.static(webRoot));

    // SPA fallback
    app.get(/^\/(?!api\/|auth\/|robots\/).*/, (req, res) => {
      const indexFile = path.join(webRoot, 'index.html');
      if (!fs.existsSync(indexFile)) {
        res.status(404).send('index.html not found');
        return;
      }
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.send(fs.readFileSync(indexFile, 'utf8'));
    });
  } else {
    console.warn('[auth+web] WEB_ROOT does not exist:', webRoot);
  }

  app.listen(port, host, () => {
    console.log(`[auth+web] listening on http://${host}:${port}`);
    console.log(`[auth+web] webRoot: ${webRoot}`);
    console.log(`[auth+web] db: ${dbPath}`);
    console.log(`[auth+web] AI_API_BASE: ${aiApiBase}`);
  });
}

start();
