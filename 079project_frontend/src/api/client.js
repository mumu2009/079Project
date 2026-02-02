const API_BASE = process.env.REACT_APP_API_BASE || '';

const TOKEN_KEY = 'phoenix_auth_token';

export function getAuthToken() {
  try {
    return localStorage.getItem(TOKEN_KEY) || '';
  } catch (_e) {
    return '';
  }
}

export function setAuthToken(token) {
  try {
    if (!token) localStorage.removeItem(TOKEN_KEY);
    else localStorage.setItem(TOKEN_KEY, String(token));
  } catch (_e) {
    // ignore
  }
}

async function request(path, { method = 'GET', body, headers } = {}) {
  const token = getAuthToken();
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(headers || {})
    },
    body: body !== undefined ? JSON.stringify(body) : undefined
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_e) {
    json = { ok: false, error: 'invalid-json', raw: text };
  }
  if (!res.ok) {
    const msg = json && typeof json === 'object' ? (json.error || json.message || res.statusText) : res.statusText;
    const err = new Error(msg);
    err.status = res.status;
    err.payload = json;
    throw err;
  }
  return json;
}

export const api = {
  authConfig: () => request('/auth/config'),
  authBootstrap: (username, password) => request('/auth/bootstrap', { method: 'POST', body: { username, password } }),
  authLogin: async (username, password) => {
    const out = await request('/auth/login', { method: 'POST', body: { username, password } });
    if (out?.token) setAuthToken(out.token);
    return out;
  },
  authMe: () => request('/auth/me'),
  authLogout: async () => {
    const out = await request('/auth/logout', { method: 'POST', body: {} });
    setAuthToken('');
    return out;
  },
  chat: (text, sessionId) => request('/api/chat', { method: 'POST', body: { text, sessionId } }),
  arrayChat: (text, sessionId, options) => request('/api/array/chat', { method: 'POST', body: { text, sessionId, options } }),
  runtimeFeatures: () => request('/api/runtime/features'),
  runtimePatch: (patch) => request('/api/runtime/features', { method: 'PATCH', body: patch || {} }),
  addons: () => request('/api/addons'),
  addonAdd: (type, name) => request('/api/addons/add', { method: 'POST', body: { type, name } }),
  addonRemove: (name) => request('/api/addons/remove', { method: 'POST', body: { name } }),
  studyStatus: () => request('/api/study/status'),
  dialogReset: () => request('/api/learn/dialog/reset', { method: 'POST', body: {} }),
  searchConfig: () => request('/api/search/config'),
  setSearchConfig: (config) => request('/api/search/config', { method: 'PUT', body: config || {} }),
  searchEndpointAdd: (url) => request('/api/search/endpoints/add', { method: 'POST', body: { url } }),
  searchEndpointRemove: (url) => request('/api/search/endpoints/remove', { method: 'POST', body: { url } }),
  getParams: () => request('/api/model/params'),
  setParams: (params) => request('/api/model/params', { method: 'POST', body: params }),
  resetParams: () => request('/api/model/params/reset', { method: 'POST', body: {} }),
  snapshots: () => request('/api/snapshots'),
  snapshotCreate: (name) => request('/api/snapshots/create', { method: 'POST', body: { name } }),
  snapshotRestore: (id) => request(`/api/snapshots/restore/${encodeURIComponent(id)}`, { method: 'POST', body: {} }),
  snapshotDelete: (id) => request(`/api/snapshots/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  systemStatus: () => request('/api/system/status'),
  systemConfig: () => request('/api/system/config'),
  groups: () => request('/api/groups'),
  groupMetrics: (gid) => request(`/api/groups/${encodeURIComponent(gid)}/metrics`),
  requestOnline: (query, options) => request('/api/corpus/online', { method: 'POST', body: { query, options: options || {} } }),
  shards: () => request('/api/shards'),
  robotsList: () => request('/robots/list'),
  robotsIngest: (options) => request('/robots/ingest', { method: 'POST', body: options || {} }),
  robotsRetrain: (options) => request('/api/robots/retrain', { method: 'POST', body: options || {} }),
  testsList: () => request('/api/tests/list'),
  testsCase: (name, content) => request('/api/tests/case', { method: 'POST', body: { name, content } }),
  testsRefresh: () => request('/api/tests/refresh', { method: 'POST', body: {} }),
  exportGraph: (seeds, radius) => request('/api/export/graph', { method: 'POST', body: { seeds, radius } }),
  exportGraphGroup: (groupId, seeds, radius) => request('/api/export/graph/group', { method: 'POST', body: { groupId, seeds, radius } }),
  rlLearn: (cycles) => request('/api/learn/reinforce', { method: 'POST', body: { cycles } }),
  rlLatest: () => request('/api/learn/reinforce/latest'),
  advLearn: (samples) => request('/api/learn/adversarial', { method: 'POST', body: { samples } }),
  advLatest: () => request('/api/learn/adversarial/latest'),
  learnThresholds: (rlEvery, advEvery) => request('/api/learn/thresholds', { method: 'POST', body: { rlEvery, advEvery } }),
  barrierStart: (maliciousThreshold) => request('/api/memebarrier/start', { method: 'POST', body: { maliciousThreshold } }),
  barrierStop: () => request('/api/memebarrier/stop', { method: 'POST', body: {} }),
  barrierStats: () => request('/api/memebarrier/stats')
};
