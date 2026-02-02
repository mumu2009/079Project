import React, { useEffect, useMemo, useRef, useState } from 'react';
import './App.css';
import AuthGate from './components/AuthGate';
import { api } from './api/client';
import ConfigPanel from './components/ConfigPanel';

const loadJson = (key, fallback) => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    return JSON.parse(raw);
  } catch (_e) {
    return fallback;
  }
};

const saveJson = (key, value) => {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch (_e) {}
};

function App() {
  const [account, setAccount] = useState(() => loadJson('phoenix.account', { id: 'local', name: 'Local User' }));
  const [sessions, setSessions] = useState(() => loadJson('phoenix.sessions', []));
  const [activeSessionId, setActiveSessionId] = useState(() => loadJson('phoenix.activeSessionId', null));
  const [activePage, setActivePage] = useState(() => loadJson('phoenix.activePage', 'chat'));
  const [message, setMessage] = useState('');
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState(null);
  const [error, setError] = useState(null);
  const inputRef = useRef(null);

  const activeSession = useMemo(() => {
    const found = sessions.find((s) => s.id === activeSessionId);
    return found || null;
  }, [sessions, activeSessionId]);

  useEffect(() => {
    saveJson('phoenix.account', account);
  }, [account]);
  useEffect(() => {
    saveJson('phoenix.sessions', sessions);
  }, [sessions]);
  useEffect(() => {
    saveJson('phoenix.activeSessionId', activeSessionId);
  }, [activeSessionId]);
  useEffect(() => {
    saveJson('phoenix.activePage', activePage);
  }, [activePage]);

  useEffect(() => {
    let cancelled = false;
    api.systemStatus()
      .then((r) => {
        if (!cancelled) setStatus(r);
      })
      .catch((e) => {
        if (!cancelled) setStatus({ ok: false, error: e.message });
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const ensureSession = () => {
    if (activeSession) return activeSession;
    const id = `s_${Date.now().toString(36)}_${Math.random().toString(16).slice(2)}`;
    const session = { id, title: 'New chat', createdAt: Date.now(), messages: [] };
    setSessions((prev) => [session, ...prev]);
    setActiveSessionId(id);
    return session;
  };

  const appendMessage = (sessionId, msg) => {
    setSessions((prev) =>
      prev.map((s) =>
        s.id === sessionId
          ? {
              ...s,
              messages: [...s.messages, msg],
              title: s.title === 'New chat' ? (msg.role === 'user' ? msg.text.slice(0, 24) || 'Chat' : s.title) : s.title
            }
          : s
      )
    );
  };

  const onSend = async () => {
    const text = message.trim();
    if (!text || busy) return;
    setError(null);
    setBusy(true);
    const session = ensureSession();
    const sid = session.id;
    appendMessage(sid, { id: `m_${Date.now()}`, role: 'user', text, ts: Date.now() });
    setMessage('');
    try {
      const r = await api.chat(text, sid);
      const reply = r?.result?.reply ?? '';
      appendMessage(sid, {
        id: `m_${Date.now()}_ai`,
        role: 'assistant',
        text: reply,
        ts: Date.now(),
        meta: {
          latency: r?.result?.latency,
          seeds: r?.result?.seeds,
          memes: r?.result?.memes,
          addon: r?.result?.addon
        }
      });
    } catch (e) {
      setError(e.message);
      appendMessage(sid, { id: `m_${Date.now()}_err`, role: 'system', text: `Error: ${e.message}`, ts: Date.now() });
    } finally {
      setBusy(false);
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  };

  const newSession = () => {
    const id = `s_${Date.now().toString(36)}_${Math.random().toString(16).slice(2)}`;
    const session = { id, title: 'New chat', createdAt: Date.now(), messages: [] };
    setSessions((prev) => [session, ...prev]);
    setActiveSessionId(id);
    setTimeout(() => inputRef.current?.focus(), 0);
  };

  const deleteSession = (id) => {
    setSessions((prev) => prev.filter((s) => s.id !== id));
    if (activeSessionId === id) {
      setActiveSessionId(null);
    }
  };

  return (
    <AuthGate>
      <div className="phoenix-root">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-title">079 Phoenix</div>
          <div className="brand-sub">AI workspace</div>
        </div>

        <div className="account">
          <div className="account-row">
            <div className="avatar">{(account?.name || 'U').slice(0, 1).toUpperCase()}</div>
            <div className="account-meta">
              <div className="account-name">{account?.name || 'User'}</div>
              <div className="account-id">{account?.id || 'local'}</div>
            </div>
          </div>
          <div className="account-actions">
            <button className="btn btn-ghost" onClick={() => setAccount((a) => ({ ...a, name: a.name === 'Local User' ? 'Operator' : 'Local User' }))}>
              切换昵称
            </button>
            <button className="btn" onClick={newSession}>
              新会话
            </button>
          </div>
        </div>

        <div className="sessions">
          <div className="section-title">会话</div>
          <div className="session-list">
            {sessions.length === 0 ? (
              <div className="muted">暂无会话</div>
            ) : (
              sessions.map((s) => (
                <div key={s.id} className={`session-item ${s.id === activeSessionId ? 'active' : ''}`}>
                  <button className="session-main" onClick={() => setActiveSessionId(s.id)}>
                    <div className="session-title">{s.title || 'Chat'}</div>
                    <div className="session-sub">{new Date(s.createdAt).toLocaleString()}</div>
                  </button>
                  <button className="session-del" onClick={() => deleteSession(s.id)} title="删除">
                    ×
                  </button>
                </div>
              ))
            )}
          </div>
        </div>

        <div className="nav">
          <div className="section-title">导航</div>
          <div className="nav-list">
            <button className={`nav-item ${activePage === 'chat' ? 'active' : ''}`} onClick={() => setActivePage('chat')}>
              Chat
            </button>
            <button className={`nav-item ${activePage === 'config' ? 'active' : ''}`} onClick={() => setActivePage('config')}>
              Config
            </button>
          </div>
        </div>

        <div className="status">
          <div className="section-title">后端</div>
          <div className="status-row">
            <span className={`dot ${status?.ok ? 'ok' : 'bad'}`} />
            <span className="muted">{status?.ok ? 'connected' : 'disconnected'}</span>
          </div>
        </div>
      </aside>

      <main className="main">
        {activePage === 'chat' ? (
          <>
            <header className="topbar">
              <div className="topbar-title">{activeSession ? activeSession.title : '请选择或新建会话'}</div>
              <div className="topbar-actions">
                <button
                  className="btn btn-ghost"
                  onClick={() => api.snapshotCreate('ui').then(() => api.systemStatus().then(setStatus)).catch((e) => setError(e.message))}
                >
                  保存快照
                </button>
                <button className="btn btn-ghost" onClick={() => api.barrierStats().then(() => {}).catch((e) => setError(e.message))}>
                  Barrier
                </button>
              </div>
            </header>

            <section className="chat">
              <div className="chat-scroll">
                {(activeSession?.messages || []).map((m) => (
                  <div key={m.id} className={`msg ${m.role}`}>
                    <div className="msg-role">{m.role}</div>
                    <div className="msg-bubble">
                      <div className="msg-text">{m.text}</div>
                      {m.meta ? (
                        <div className="msg-meta">
                          {m.meta.latency != null ? <span>latency: {m.meta.latency}ms</span> : null}
                          {Array.isArray(m.meta.seeds) ? <span> seeds: {m.meta.seeds.length}</span> : null}
                          {Array.isArray(m.meta.memes) ? <span> memes: {m.meta.memes.length}</span> : null}
                          {m.meta.addon ? (
                            <span> addon: {m.meta.addon.name || m.meta.addon.addon || m.meta.addon.type || 'custom'}</span>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>

              <div className="composer">
                <input
                  ref={inputRef}
                  className="composer-input"
                  value={message}
                  placeholder={busy ? '生成中…' : '输入消息，Enter 发送'}
                  onChange={(e) => setMessage(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                      e.preventDefault();
                      onSend();
                    }
                  }}
                />
                <button className="btn" disabled={busy || !message.trim()} onClick={onSend}>
                  发送
                </button>
              </div>

              {error ? <div className="error">{error}</div> : null}
            </section>
          </>
        ) : (
          <>
            <header className="topbar">
              <div className="topbar-title">Config</div>
              <div className="topbar-actions">
                <button className="btn btn-ghost" onClick={() => api.systemStatus().then(setStatus).catch((e) => setError(e.message))}>
                  刷新后端状态
                </button>
              </div>
            </header>
            <section className="cfg-wrap">
              <ConfigPanel onError={(msg) => setError(msg)} />
              {error ? <div className="error">{error}</div> : null}
            </section>
          </>
        )}
      </main>
    </div>
    </AuthGate>
  );
}

export default App;
