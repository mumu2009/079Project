import React, { useEffect, useState } from 'react';
import { api } from '../api/client';

export default function AuthGate({ children }) {
  const [state, setState] = useState({ loading: true, user: null, error: null, needsBootstrap: false });

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const out = await api.authMe();
        if (!alive) return;
        setState({ loading: false, user: out.user, error: null, needsBootstrap: false });
      } catch (e) {
        if (!alive) return;
        // If backend hasn't been bootstrapped yet, login will 401 but bootstrap should be allowed.
        setState({ loading: false, user: null, error: e, needsBootstrap: false });
      }
    })();
    return () => {
      alive = false;
    };
  }, []);

  if (state.loading) {
    return (
      <div style={{ padding: 24, color: '#e5e7eb' }}>
        <div style={{ fontSize: 18, fontWeight: 700 }}>正在验证身份…</div>
      </div>
    );
  }

  if (state.user) {
    return children;
  }

  return <LoginPanel onAuthed={(user) => setState({ loading: false, user, error: null, needsBootstrap: false })} />;
}

function LoginPanel({ onAuthed }) {
  const [mode, setMode] = useState('login'); // login | bootstrap
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('');
  const [status, setStatus] = useState({ busy: false, error: null, info: null });

  async function handleLogin() {
    setStatus({ busy: true, error: null, info: null });
    try {
      const out = await api.authLogin(username, password);
      const me = await api.authMe();
      onAuthed(me.user);
      setStatus({ busy: false, error: null, info: `欢迎 ${out.user?.username || username}` });
    } catch (e) {
      setStatus({ busy: false, error: e?.message || '登录失败', info: null });
    }
  }

  async function handleBootstrap() {
    setStatus({ busy: true, error: null, info: null });
    try {
      await api.authBootstrap(username, password);
      await handleLogin();
    } catch (e) {
      setStatus({ busy: false, error: e?.message || '初始化失败', info: null });
    }
  }

  return (
    <div style={{ minHeight: '100vh', background: '#0b0f19', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <div style={{ width: 420, background: '#121a2a', border: '1px solid rgba(255,255,255,0.08)', borderRadius: 12, padding: 18 }}>
        <div style={{ color: '#e5e7eb', fontSize: 18, fontWeight: 800, marginBottom: 8 }}>身份验证</div>
        <div style={{ color: '#9aa4b2', fontSize: 12, marginBottom: 12 }}>
          {mode === 'login' ? '请输入用户名与密码登录。' : '首次启动：创建管理员账号（只允许一次）。'}
        </div>

        <div style={{ display: 'grid', gap: 10 }}>
          <label style={{ color: '#cbd5e1', fontSize: 12 }}>
            用户名
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              style={inputStyle}
              placeholder="admin"
              autoComplete="username"
            />
          </label>
          <label style={{ color: '#cbd5e1', fontSize: 12 }}>
            密码
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              style={inputStyle}
              placeholder="至少 6 位"
              autoComplete={mode === 'login' ? 'current-password' : 'new-password'}
            />
          </label>

          {status.error ? <div style={{ color: '#fca5a5', fontSize: 12 }}>{String(status.error)}</div> : null}
          {status.info ? <div style={{ color: '#86efac', fontSize: 12 }}>{String(status.info)}</div> : null}

          <div style={{ display: 'flex', gap: 10, alignItems: 'center', justifyContent: 'space-between' }}>
            <button
              onClick={mode === 'login' ? handleLogin : handleBootstrap}
              disabled={status.busy}
              style={primaryBtn}
            >
              {status.busy ? '处理中…' : mode === 'login' ? '登录' : '创建管理员并登录'}
            </button>

            <button
              onClick={() => setMode(mode === 'login' ? 'bootstrap' : 'login')}
              disabled={status.busy}
              style={linkBtn}
            >
              {mode === 'login' ? '首次启动？去初始化' : '返回登录'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

const inputStyle = {
  width: '100%',
  marginTop: 6,
  padding: '10px 12px',
  borderRadius: 10,
  border: '1px solid rgba(255,255,255,0.12)',
  background: '#0b1220',
  color: '#e5e7eb',
  outline: 'none'
};

const primaryBtn = {
  padding: '10px 12px',
  borderRadius: 10,
  border: '1px solid rgba(255,255,255,0.10)',
  background: '#2563eb',
  color: 'white',
  fontWeight: 700,
  cursor: 'pointer'
};

const linkBtn = {
  padding: '10px 8px',
  borderRadius: 10,
  border: 'none',
  background: 'transparent',
  color: '#93c5fd',
  cursor: 'pointer'
};
