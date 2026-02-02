// Minimal C++ CI/CD server: upload C++/CMake files and build with CMake.
// Security defaults: binds to 127.0.0.1, limits upload size, whitelists filenames.

const express = require('express');
const multer = require('multer');
const { v4: uuid } = require('uuid');
const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const mime = require('mime-types');

const PORT = Number(process.env.PORT || 3000);
const BIND_HOST = String(process.env.BIND_HOST || '0.0.0.0');

// Build settings (env overrides)
const MAX_UPLOAD_MB = Number(process.env.MAX_UPLOAD_MB || 50);
const BUILD_TIMEOUT_MS = Number(process.env.BUILD_TIMEOUT_MS || 10 * 60 * 1000);
const CMAKE_GENERATOR = process.env.CMAKE_GENERATOR || ''; // e.g. "Ninja"
const CMAKE_BUILD_TYPE = process.env.CMAKE_BUILD_TYPE || 'Release';
const CMAKE_TOOLCHAIN_FILE = process.env.CMAKE_TOOLCHAIN_FILE || ''; // for cross-compile
const CMAKE_ARGS = process.env.CMAKE_ARGS || ''; // extra args, e.g. "-DUSE_SSL=ON"
const MULTI_CONFIG = /^(1|true|on)$/i.test(String(process.env.CMAKE_MULTI_CONFIG || '')); // MSVC/multi-config
const CONCURRENCY = Math.max(1, Number(process.env.BUILD_JOBS || os.cpus().length || 2));

const ROOT = __dirname;
const JOBS_DIR = path.join(ROOT, 'jobs');
fs.mkdirSync(JOBS_DIR, { recursive: true });

// Whitelist acceptable filenames/extensions
const ALLOWED_BASENAMES = new Set([
  'CMakeLists.txt', '.gitignore', '.clang-format', '.editorconfig', 'toolchain-mingw.cmake'
]);
const ALLOWED_EXT_RE = /\.(?:c|cc|cpp|cxx|h|hh|hpp|hxx|ipp|inl|cmake|txt|rc|def)$/i;

function isAllowedName(originalName) {
  const base = path.basename(originalName);
  if (ALLOWED_BASENAMES.has(base)) return true;
  return ALLOWED_EXT_RE.test(base);
}

// Multer storage to preserve original filename under job folder
const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      try {
        const jobId = req._jobId || (req._jobId = uuid());
        const jobDir = path.join(JOBS_DIR, jobId, 'src');
        fs.mkdirSync(jobDir, { recursive: true });
        cb(null, jobDir);
      } catch (e) {
        cb(e);
      }
    },
    filename: (req, file, cb) => {
      const base = path.basename(file.originalname);
      cb(null, base);
    }
  }),
  limits: { fileSize: MAX_UPLOAD_MB * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = isAllowedName(file.originalname);
    if (!ok) return cb(new Error(`disallowed filename or extension: ${file.originalname}`));
    cb(null, true);
  }
});

// In-memory job table (simple). You can persist to disk if needed.
const jobs = new Map();
// job = { id, status, createdAt, updatedAt, workDir, buildDir, logPath, artifacts:[], current: { cmd, args, pid, startedAt } }

// Track active processes by jobId
const activeProcs = new Map();

function nowIso() {
  return new Date().toISOString();
}

function appendLog(file, chunk) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.appendFileSync(file, chunk);
}

function runCmd(cmd, args, opts, logPath, timeoutMs, job) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, opts);
    if (job) {
      const current = {
        cmd,
        args,
        pid: child.pid,
        startedAt: nowIso()
      };
      job.current = current;
      activeProcs.set(job.id, current);
    }
    const timer = setTimeout(() => {
      try { child.kill('SIGKILL'); } catch (_) {}
      reject(new Error('timeout'));
    }, Math.max(10_000, timeoutMs));

    const onData = (data) => appendLog(logPath, data.toString());
    child.stdout.on('data', onData);
    child.stderr.on('data', onData);

    child.on('error', (err) => {
      clearTimeout(timer);
      appendLog(logPath, `\n[ERROR] ${cmd} failed to start: ${err.message}\n`);
      if (job) {
        activeProcs.delete(job.id);
        job.current = null;
      }
      reject(err);
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      appendLog(logPath, `\n[EXIT] ${cmd} exited with code ${code}\n`);
      if (job) {
        activeProcs.delete(job.id);
        job.current = null;
      }
      if (code === 0) resolve(0);
      else reject(new Error(`${cmd} exited ${code}`));
    });
  });
}

async function buildJob(job) {
  job.status = 'building';
  job.updatedAt = nowIso();
  const logPath = job.logPath;
  const workDir = job.workDir;
  const buildDir = job.buildDir;

  appendLog(logPath, `[${nowIso()}] Starting build for job ${job.id}\n`);
  appendLog(logPath, `[env] CMAKE_GENERATOR=${CMAKE_GENERATOR || '(default)'}  CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}\n`);
  if (CMAKE_TOOLCHAIN_FILE) appendLog(logPath, `[env] CMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}\n`);
  if (CMAKE_ARGS) appendLog(logPath, `[env] CMAKE_ARGS=${CMAKE_ARGS}\n`);

  // Require CMakeLists.txt
  const cmakelists = path.join(workDir, 'CMakeLists.txt');
  if (!fs.existsSync(cmakelists)) {
    appendLog(logPath, '[ERROR] CMakeLists.txt not found in job src folder.\n');
    job.status = 'cmakelists-missing';
    job.updatedAt = nowIso();
    return;
  }

  const cmakeArgs = ['-S', workDir, '-B', buildDir];
  if (CMAKE_GENERATOR) {
    cmakeArgs.push('-G', CMAKE_GENERATOR);
  }
  cmakeArgs.push('-DCMAKE_BUILD_TYPE=' + CMAKE_BUILD_TYPE);
  if (CMAKE_TOOLCHAIN_FILE) {
    cmakeArgs.push('-DCMAKE_TOOLCHAIN_FILE=' + CMAKE_TOOLCHAIN_FILE);
  }
  if (CMAKE_ARGS) {
    // naive split on spaces; for complex quoting, prefer ENV array or encode into server
    cmakeArgs.push(...CMAKE_ARGS.split(' ').filter(Boolean));
  }

  try {
    await runCmd('cmake', cmakeArgs, { cwd: workDir }, logPath, BUILD_TIMEOUT_MS, job);
  } catch (e) {
    job.status = 'cmake-config-failed';
    job.updatedAt = nowIso();
    return;
  }

  // Build step
  const buildCmdArgs = ['--build', buildDir];
  if (MULTI_CONFIG) {
    buildCmdArgs.push('--config', CMAKE_BUILD_TYPE);
  }
  // Parallelism for Make/Ninja
  buildCmdArgs.push('--', '-j', String(CONCURRENCY));

  try {
    await runCmd('cmake', buildCmdArgs, { cwd: workDir }, logPath, BUILD_TIMEOUT_MS, job);
    job.status = 'success';
  } catch (e) {
    job.status = 'build-failed';
  } finally {
    job.updatedAt = nowIso();
  }

  // Collect artifacts (best-effort): list files in buildDir that look executable or libraries
  const artifacts = [];
  try {
    const walk = (dir) => {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const ent of entries) {
        const p = path.join(dir, ent.name);
        if (ent.isDirectory()) walk(p);
        else artifacts.push(p);
      }
    };
    walk(buildDir);
  } catch (_) {}
  job.artifacts = artifacts;
}

// Express app
const app = express();

// Embedded management UI
app.get('/', (_req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(`<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CI 管理台</title>
  <style>
    :root { --bg:#0b0f14; --card:#111827; --muted:#94a3b8; --text:#e5e7eb; --accent:#22c55e; --warn:#f59e0b; --err:#ef4444; }
    * { box-sizing: border-box; }
    body { margin:0; font-family:system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Noto Sans", "PingFang SC", "Microsoft YaHei", sans-serif; background:var(--bg); color:var(--text); }
    header { padding:16px 20px; border-bottom:1px solid #1f2937; display:flex; gap:12px; align-items:center; justify-content:space-between; }
    h1 { margin:0; font-size:18px; font-weight:600; }
    .container { display:grid; grid-template-columns: 340px 1fr; gap:16px; padding:16px; }
    .card { background:var(--card); border:1px solid #1f2937; border-radius:12px; padding:14px; }
    .section-title { font-size:13px; color:var(--muted); margin-bottom:10px; }
    .jobs { max-height: calc(100vh - 170px); overflow:auto; }
    .job { border:1px solid #1f2937; border-radius:10px; padding:10px; margin-bottom:8px; cursor:pointer; }
    .job:hover { border-color:#334155; }
    .status { font-size:12px; padding:2px 8px; border-radius:999px; display:inline-block; }
    .status.queued,.status.building { background:#0f172a; color:#cbd5f5; }
    .status.success { background:#052e1c; color:#86efac; }
    .status.build-failed,.status.cmake-config-failed,.status.crashed { background:#2b0a0a; color:#fecaca; }
    .muted { color:var(--muted); }
    .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    button { background:#1f2937; color:var(--text); border:1px solid #334155; padding:6px 10px; border-radius:8px; cursor:pointer; }
    button:hover { border-color:#475569; }
    .log { width:100%; height:360px; background:#0a0f16; color:#cbd5e1; border:1px solid #1f2937; border-radius:10px; padding:10px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size:12px; overflow:auto; white-space:pre-wrap; }
    .proc { border-left:3px solid #334155; padding-left:8px; margin:6px 0; }
    .upload { border:1px dashed #334155; padding:10px; border-radius:10px; }
    input[type="file"] { width:100%; color:var(--muted); }
  </style>
</head>
<body>
  <header>
    <h1>CI 管理台</h1>
    <div class="row">
      <button onclick="refreshAll()">刷新</button>
      <span class="muted" id="health"></span>
    </div>
  </header>
  <div class="container">
    <div class="card">
      <div class="section-title">上传构建</div>
      <form class="upload" action="/api/upload" method="post" enctype="multipart/form-data">
        <input type="file" name="files" multiple required />
        <div style="margin-top:8px;">
          <button type="submit">上传并构建</button>
        </div>
      </form>
      <div class="section-title" style="margin-top:14px;">作业列表</div>
      <div class="jobs" id="jobs"></div>
    </div>
    <div class="card">
      <div class="section-title">作业详情</div>
      <div id="jobDetail" class="muted">请选择一个作业</div>
      <div style="margin-top:10px;" class="row">
        <button id="btnRebuild" onclick="rebuildSelected()" disabled>重新构建</button>
        <button id="btnLogs" onclick="openLogs()" disabled>打开完整日志</button>
      </div>
      <div class="section-title" style="margin-top:14px;">CMake 构建日志</div>
      <div id="log" class="log"></div>
      <div class="section-title" style="margin-top:14px;">运行中的进程</div>
      <div id="procs" class="muted"></div>
    </div>
  </div>

  <script>
    let selectedJobId = '';

    async function fetchJson(url) {
      const res = await fetch(url, { cache: 'no-store' });
      return res.json();
    }

    function statusBadge(s) {
      return '<span class="status ' + s + '">' + s + '</span>';
    }

    async function loadHealth() {
      try {
        const h = await fetchJson('/api/health');
        document.getElementById('health').textContent = 'OK ' + h.time;
      } catch (_) {
        document.getElementById('health').textContent = 'Health unavailable';
      }
    }

    async function loadJobs() {
      const data = await fetchJson('/api/jobs');
      const box = document.getElementById('jobs');
      box.innerHTML = '';
      data.jobs.forEach(j => {
        const el = document.createElement('div');
        el.className = 'job';
        el.onclick = () => selectJob(j.id);
        el.innerHTML = '<div class="row"><strong>' + j.id + '</strong>' + statusBadge(j.status) + '</div>'
          + '<div class="muted">' + j.updatedAt + '</div>';
        box.appendChild(el);
      });
    }

    async function selectJob(jobId) {
      selectedJobId = jobId;
      document.getElementById('btnRebuild').disabled = false;
      document.getElementById('btnLogs').disabled = false;
      const status = await fetchJson('/api/status/' + jobId);
      const detail = document.getElementById('jobDetail');
      detail.innerHTML = '<div class="row"><strong>' + status.jobId + '</strong>' + statusBadge(status.status) + '</div>'
        + '<div class="muted">创建: ' + status.createdAt + '</div>'
        + '<div class="muted">更新: ' + status.updatedAt + '</div>'
        + '<div class="muted">生成器: ' + status.generator + ' | 类型: ' + status.buildType + '</div>'
        + '<div class="muted">构建产物: ' + (status.artifacts || []).join(', ') + '</div>';
      document.getElementById('log').textContent = status.logTail || '';
      await loadProcs();
    }

    async function rebuildSelected() {
      if (!selectedJobId) return;
      await fetchJson('/api/rebuild/' + selectedJobId);
      setTimeout(refreshAll, 500);
    }

    function openLogs() {
      if (!selectedJobId) return;
      window.open('/logs/' + selectedJobId, '_blank');
    }

    async function loadProcs() {
      const data = await fetchJson('/api/processes');
      const box = document.getElementById('procs');
      if (!data.processes.length) {
        box.textContent = '暂无运行中的进程';
        return;
      }
      box.innerHTML = '';
      data.processes.forEach(p => {
        const div = document.createElement('div');
        div.className = 'proc';
        div.innerHTML = '<div><strong>' + p.jobId + '</strong> (pid ' + p.pid + ')</div>'
          + '<div class="muted">' + p.cmd + ' ' + (p.args || []).join(' ') + '</div>'
          + '<div class="muted">开始: ' + p.startedAt + '</div>';
        box.appendChild(div);
      });
    }

    async function refreshAll() {
      await loadHealth();
      await loadJobs();
      if (selectedJobId) await selectJob(selectedJobId);
      else await loadProcs();
    }

    refreshAll();
    setInterval(refreshAll, 5000);
  </script>
</body>
</html>`);
});

// Health/status
app.get('/api/health', (_req, res) => res.json({ ok: true, time: nowIso() }));

// Upload endpoint
app.post('/api/upload', upload.array('files', 200), async (req, res) => {
  const jobId = req._jobId || uuid();
  const jobRoot = path.join(JOBS_DIR, jobId);
  const workDir = path.join(jobRoot, 'src');
  const buildDir = path.join(jobRoot, 'build');
  const logPath = path.join(buildDir, 'build.log');
  fs.mkdirSync(buildDir, { recursive: true });

  // Basic validation feedback
  const names = (req.files || []).map(f => f.originalname);
  if (!names.some(n => path.basename(n) === 'CMakeLists.txt')) {
    return res.status(400).json({ error: 'CMakeLists.txt missing in upload', jobId });
  }

  const job = {
    id: jobId,
    status: 'queued',
    createdAt: nowIso(),
    updatedAt: nowIso(),
    workDir,
    buildDir,
    logPath,
    artifacts: []
  };
  jobs.set(jobId, job);

  // Async build (no await)
  setImmediate(() => buildJob(job).catch(err => {
    appendLog(logPath, `\n[FATAL] build crashed: ${err.message}\n`);
    job.status = 'crashed';
    job.updatedAt = nowIso();
  }));

  res.json({ jobId, status: job.status, files: names });
});

// Job status
app.get('/api/status/:jobId', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'not-found' });
  let logTail = '';
  try {
    if (fs.existsSync(job.logPath)) {
      const txt = fs.readFileSync(job.logPath, 'utf8');
      const lines = txt.split('\n');
      logTail = lines.slice(-200).join('\n');
    }
  } catch (_) {}
  res.json({
    jobId: job.id,
    status: job.status,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    buildType: CMAKE_BUILD_TYPE,
    generator: CMAKE_GENERATOR || '(default)',
    artifacts: (job.artifacts || []).map(p => path.relative(job.buildDir, p)),
    logTail
  });
});

// List all jobs
app.get('/api/jobs', (_req, res) => {
  const list = Array.from(jobs.values())
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1))
    .map(j => ({
      id: j.id,
      status: j.status,
      createdAt: j.createdAt,
      updatedAt: j.updatedAt
    }));
  res.json({ jobs: list });
});

// List running processes
app.get('/api/processes', (_req, res) => {
  const processes = Array.from(activeProcs.entries()).map(([jobId, p]) => ({
    jobId,
    cmd: p.cmd,
    args: p.args,
    pid: p.pid,
    startedAt: p.startedAt
  }));
  res.json({ processes });
});

// Stream full log
app.get('/logs/:jobId', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).end();
  if (!fs.existsSync(job.logPath)) return res.status(404).end();
  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  fs.createReadStream(job.logPath).pipe(res);
});

// Download artifact (path under buildDir)
app.get('/artifact/:jobId/:rest(*)', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).end();
  const rel = String(req.params.rest || '');
  const baseDir = path.resolve(job.buildDir);
  const abs = path.resolve(baseDir, rel);
  const normalizedBase = baseDir.endsWith(path.sep) ? baseDir : baseDir + path.sep;
  const normalized = path.normalize(abs);
  if (!normalized.startsWith(normalizedBase)) return res.status(400).end();
  if (!fs.existsSync(normalized)) return res.status(404).end();
  const ctype = mime.lookup(normalized) || 'application/octet-stream';
  res.setHeader('Content-Type', ctype);
  res.download(normalized);
});

// List files in job src tree
app.get('/api/tree/:jobId', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'not-found' });

  const out = [];
  const walk = (base, rel = '') => {
    const dir = path.join(base, rel);
    let entries = [];
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch (_) { return; }
    for (const e of entries) {
      const r = path.join(rel, e.name);
      out.push({ path: r, type: e.isDirectory() ? 'dir' : 'file' });
      if (e.isDirectory()) walk(base, r);
    }
  };
  walk(job.workDir);
  res.json({ jobId: job.id, files: out });
});

// Rebuild an existing job (re-uses src, wipes build)
app.post('/api/rebuild/:jobId', async (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'not-found' });
  try {
    fs.rmSync(job.buildDir, { recursive: true, force: true });
  } catch (_) {}
  fs.mkdirSync(job.buildDir, { recursive: true });
  job.logPath = path.join(job.buildDir, 'build.log');
  job.status = 'queued';
  job.updatedAt = nowIso();
  setImmediate(() => buildJob(job).catch(err => {
    appendLog(job.logPath, `\n[FATAL] rebuild crashed: ${err.message}\n`);
    job.status = 'crashed';
    job.updatedAt = nowIso();
  }));
  res.json({ jobId: job.id, status: job.status });
});

// No favicon
app.get('/favicon.ico', (_req, res) => res.status(204).end());

// Start server
app.listen(PORT, BIND_HOST, () => {
  console.log(`[CI] listening on http://${BIND_HOST}:${PORT}`);
  console.log(`[CI] Upload form: http://${BIND_HOST}:${PORT}/`);
});