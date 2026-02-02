import React, { useEffect, useMemo, useState } from 'react';
import { api } from '../api/client';

const asNum = (v, fallback) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

function FieldRow({ label, hint, children }) {
  return (
    <div className="cfg-row">
      <div className="cfg-label">
        <div className="cfg-label-title">{label}</div>
        {hint ? <div className="cfg-label-hint">{hint}</div> : null}
      </div>
      <div className="cfg-control">{children}</div>
    </div>
  );
}

export default function ConfigPanel({ onError }) {
  const [loading, setLoading] = useState(false);
  const [features, setFeatures] = useState(null);
  const [patchBusy, setPatchBusy] = useState(false);
  const [studyStatus, setStudyStatus] = useState(null);
  const [systemConfig, setSystemConfig] = useState(null);
  const [groupsInfo, setGroupsInfo] = useState(null);

  const [addons, setAddons] = useState([]);
  const [addonType, setAddonType] = useState('math');
  const [addonName, setAddonName] = useState('');
  const [addonBusy, setAddonBusy] = useState(false);

  const [maliciousThreshold, setMaliciousThreshold] = useState('');
  const [rlEvery, setRlEvery] = useState('');
  const [advEvery, setAdvEvery] = useState('');

  const [testsDir, setTestsDir] = useState('');
  const [testsFiles, setTestsFiles] = useState([]);
  const [newTestName, setNewTestName] = useState('');
  const [newTestContent, setNewTestContent] = useState('');

  const [robotsFiles, setRobotsFiles] = useState([]);
  const [robotsSelected, setRobotsSelected] = useState([]);
  const [robotsLimit, setRobotsLimit] = useState('10');
  const [robotsShuffle, setRobotsShuffle] = useState(true);
  const [robotsEnqueueStudy, setRobotsEnqueueStudy] = useState(true);

  const [searchConfig, setSearchConfig] = useState(null);
  const [searchAddUrl, setSearchAddUrl] = useState('');
  const [searchTestQuery, setSearchTestQuery] = useState('');
  const [searchTestResult, setSearchTestResult] = useState(null);

  const [graphGroupId, setGraphGroupId] = useState('');
  const [graphSeedsText, setGraphSeedsText] = useState('');
  const [graphRadius, setGraphRadius] = useState('2');
  const [graphExportBusy, setGraphExportBusy] = useState(false);
  const [graphExportResult, setGraphExportResult] = useState(null);

  const featureSummary = useMemo(() => {
    if (!features) return null;
    return {
      memebarrier: features.memebarrier,
      rl: features.rl,
      adv: features.adv,
      thresholds: features.dialogThresholds
    };
  }, [features]);

  const refreshAll = async () => {
    setLoading(true);
    try {
      const [f, t, r, s, sc, cfg, groups, addonsRes] = await Promise.all([
        api.runtimeFeatures(),
        api.testsList(),
        api.robotsList(),
        api.studyStatus(),
        api.searchConfig(),
        api.systemConfig().catch(() => null),
        api.groups().catch(() => null),
        api.addons().catch(() => null)
      ]);
      setFeatures(f?.features || null);
      setMaliciousThreshold(String(f?.features?.memebarrier?.threshold ?? ''));
      setRlEvery(String(f?.features?.dialogThresholds?.rlEvery ?? ''));
      setAdvEvery(String(f?.features?.dialogThresholds?.advEvery ?? ''));

      setTestsDir(t?.directory || '');
      setTestsFiles(Array.isArray(t?.files) ? t.files : []);

      setRobotsFiles(Array.isArray(r?.files) ? r.files : []);
      setStudyStatus(s || null);
      setSearchConfig(sc?.config || null);

      setSystemConfig(cfg?.config || null);
      setGroupsInfo(groups || null);
      setAddons(Array.isArray(addonsRes?.addons) ? addonsRes.addons : []);

      const firstGroup = (groups?.groups && groups.groups[0]?.gid) || (cfg?.config?.groupIds && cfg.config.groupIds[0]) || '';
      if (!graphGroupId && firstGroup) setGraphGroupId(firstGroup);
    } catch (e) {
      onError?.(e.message);
    } finally {
      setLoading(false);
    }
  };

  const parseSeeds = (text) => {
    const raw = String(text || '')
      .split(/\r?\n|,|;/g)
      .map((s) => s.trim())
      .filter(Boolean);
    return raw.length ? raw : undefined;
  };

  const exportGraphForGroup = async () => {
    if (!graphGroupId) {
      onError?.('groupId required');
      return;
    }
    setGraphExportBusy(true);
    try {
      const seeds = parseSeeds(graphSeedsText);
      const radius = asNum(graphRadius, 2);
      const r = await api.exportGraphGroup(graphGroupId, seeds, radius);
      setGraphExportResult(r || null);
    } catch (e) {
      onError?.(e.message);
    } finally {
      setGraphExportBusy(false);
    }
  };

  const downloadGraphTxt = () => {
    const content = graphExportResult?.content;
    if (!content) return;
    const nameFromApi = graphExportResult?.file;
    const safeGroup = String(graphGroupId || 'group').replace(/[^a-zA-Z0-9_-]/g, '_');
    const filename = nameFromApi ? String(nameFromApi).replace(/\.(json|txt)$/i, '') + '.txt' : `graph_${safeGroup}.txt`;
    const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  const updateSearchConfig = async (patch) => {
    try {
      const current = searchConfig || { enabled: true, active: '', endpoints: [] };
      const next = { ...current, ...(patch || {}) };
      const r = await api.setSearchConfig(next);
      setSearchConfig(r?.config || null);
    } catch (e) {
      onError?.(e.message);
    }
  };

  useEffect(() => {
    refreshAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const applyPatch = async (patch) => {
    setPatchBusy(true);
    try {
      const r = await api.runtimePatch(patch);
      setFeatures(r?.features || null);
      setMaliciousThreshold(String(r?.features?.memebarrier?.threshold ?? ''));
      setRlEvery(String(r?.features?.dialogThresholds?.rlEvery ?? ''));
      setAdvEvery(String(r?.features?.dialogThresholds?.advEvery ?? ''));
      if (Array.isArray(r?.result?.warnings) && r.result.warnings.length) {
        onError?.(r.result.warnings.join(' | '));
      }
    } catch (e) {
      onError?.(e.message);
    } finally {
      setPatchBusy(false);
    }
  };

  const saveThresholds = async () => {
    await applyPatch({ rlEvery: asNum(rlEvery, undefined), advEvery: asNum(advEvery, undefined) });
  };

  const saveBarrierThreshold = async () => {
    await applyPatch({ maliciousThreshold: asNum(maliciousThreshold, undefined) });
  };

  const createTestCase = async () => {
    if (!newTestName.trim()) {
      onError?.('name required');
      return;
    }
    try {
      await api.testsCase(newTestName.trim(), newTestContent);
      setNewTestName('');
      setNewTestContent('');
      const t = await api.testsList();
      setTestsDir(t?.directory || '');
      setTestsFiles(Array.isArray(t?.files) ? t.files : []);
    } catch (e) {
      onError?.(e.message);
    }
  };

  const refreshTests = async () => {
    try {
      await api.testsRefresh();
    } catch (e) {
      onError?.(e.message);
    }
  };

  const retrainRobots = async () => {
    try {
      await api.robotsRetrain({
        files: robotsSelected.length ? robotsSelected : undefined,
        limit: asNum(robotsLimit, undefined),
        shuffle: Boolean(robotsShuffle),
        enqueueStudy: Boolean(robotsEnqueueStudy)
      });
    } catch (e) {
      onError?.(e.message);
    }
  };

  const refreshAddons = async () => {
    try {
      const r = await api.addons();
      setAddons(Array.isArray(r?.addons) ? r.addons : []);
    } catch (e) {
      onError?.(e.message);
    }
  };

  const addAddon = async () => {
    setAddonBusy(true);
    try {
      const r = await api.addonAdd(addonType, addonName.trim());
      setAddons(Array.isArray(r?.addons) ? r.addons : []);
      setAddonName('');
    } catch (e) {
      onError?.(e.message);
    } finally {
      setAddonBusy(false);
    }
  };

  const removeAddon = async (name) => {
    setAddonBusy(true);
    try {
      const r = await api.addonRemove(name);
      setAddons(Array.isArray(r?.addons) ? r.addons : []);
    } catch (e) {
      onError?.(e.message);
    } finally {
      setAddonBusy(false);
    }
  };

  return (
    <div className="cfg">
      <div className="cfg-head">
        <div>
          <div className="cfg-title">Config</div>
          <div className="cfg-sub">运行时开关 / tests 刷新 / robots 重训</div>
        </div>
        <div className="cfg-actions">
          <button className="btn btn-ghost" disabled={loading} onClick={refreshAll}>
            刷新
          </button>
        </div>
      </div>

      <div className="cfg-grid">
        <section className="card">
          <div className="card-title">插件管理</div>
          <div className="muted">运行时添加/删除插件，用于加速固定任务</div>

          <div className="addon-form">
            <select className="input" value={addonType} onChange={(e) => setAddonType(e.target.value)}>
              <option value="math">math</option>
            </select>
            <input
              className="input"
              value={addonName}
              placeholder="可选：自定义名称"
              onChange={(e) => setAddonName(e.target.value)}
            />
            <button className="btn" disabled={addonBusy} onClick={addAddon}>
              添加
            </button>
            <button className="btn btn-ghost" disabled={addonBusy} onClick={refreshAddons}>
              刷新
            </button>
          </div>

          <div className="addon-list">
            {addons.length === 0 ? (
              <div className="muted">暂无插件</div>
            ) : (
              addons.map((a) => (
                <div key={`${a.name}-${a.type}`} className="addon-row">
                  <div>
                    <div className="addon-name">{a.name}</div>
                    <div className="addon-type">{a.type}</div>
                  </div>
                  <button className="btn btn-ghost" disabled={addonBusy} onClick={() => removeAddon(a.name)}>
                    移除
                  </button>
                </div>
              ))
            )}
          </div>
        </section>

        <section className="card">
          <div className="card-title">工作组 / 图导出</div>
          <div className="muted">组数/组大小为启动配置（修改需重启）</div>

          <div className="cfg-kv" style={{ marginTop: 10 }}>
            <div className="muted">groupCount / groupSize</div>
            <div className="mono">{String(systemConfig?.groupCount ?? '-')}/{String(systemConfig?.groupSize ?? '-')}</div>
          </div>
          <div className="cfg-kv">
            <div className="muted">groupIds</div>
            <div className="mono">{Array.isArray(systemConfig?.groupIds) ? systemConfig.groupIds.join(', ') : '-'}</div>
          </div>

          <div className="hr" />

          <div className="card-subtitle">按组导出 graph JSON</div>
          <FieldRow label="Group" hint="选择一个工作组导出其 graph">
            <select className="input" value={graphGroupId} onChange={(e) => setGraphGroupId(e.target.value)}>
              {(groupsInfo?.groups || []).map((g) => (
                <option key={g.gid} value={g.gid}>
                  {g.gid}
                </option>
              ))}
            </select>
          </FieldRow>
          <FieldRow label="Radius" hint="图窗口半径">
            <input className="input" value={graphRadius} onChange={(e) => setGraphRadius(e.target.value)} placeholder="2" />
          </FieldRow>
          <FieldRow label="Seeds（可选）" hint="逗号/分号/换行分隔；留空表示导出默认窗口">
            <textarea className="textarea" value={graphSeedsText} onChange={(e) => setGraphSeedsText(e.target.value)} placeholder="seedA, seedB\nseedC" />
          </FieldRow>
          <div className="cfg-inline">
            <button className="btn" disabled={graphExportBusy || !graphGroupId} onClick={exportGraphForGroup}>
              {graphExportBusy ? '导出中…' : '导出'}
            </button>
            <button
              className="btn btn-ghost"
              disabled={!graphExportResult?.content}
              onClick={() => {
                if (!graphExportResult?.content) return;
                navigator.clipboard?.writeText(graphExportResult.content).catch(() => {});
              }}
            >
              复制 JSON
            </button>
            <button className="btn btn-ghost" disabled={!graphExportResult?.content} onClick={downloadGraphTxt}>
              下载 .txt
            </button>
          </div>
          {graphExportResult?.content ? (
            <pre className="mono" style={{ whiteSpace: 'pre-wrap', marginTop: 10, maxHeight: 260, overflow: 'auto' }}>
              {graphExportResult.content}
            </pre>
          ) : null}
        </section>

        <section className="card">
          <div className="card-title">运行时开关</div>

          <FieldRow label="MemeBarrier" hint="启用/停用隔离扫描">
            <div className="cfg-inline">
              <button className="btn" disabled={patchBusy} onClick={() => applyPatch({ memebarrierEnabled: true })}>
                启用
              </button>
              <button className="btn btn-ghost" disabled={patchBusy} onClick={() => applyPatch({ memebarrierEnabled: false })}>
                停用
              </button>
              <span className="pill">{featureSummary?.memebarrier?.enabled ? 'running' : 'stopped'}</span>
            </div>
          </FieldRow>

          <FieldRow label="Barrier 阈值" hint="maliciousThreshold">
            <div className="cfg-inline">
              <input className="input" value={maliciousThreshold} onChange={(e) => setMaliciousThreshold(e.target.value)} placeholder="e.g. 0.82" />
              <button className="btn" disabled={patchBusy} onClick={saveBarrierThreshold}>
                保存
              </button>
            </div>
          </FieldRow>

          <FieldRow label="RL" hint="强化学习端点与对话触发">
            <div className="cfg-inline">
              <button className="btn" disabled={patchBusy} onClick={() => applyPatch({ rlEnabled: true })}>
                启用
              </button>
              <button className="btn btn-ghost" disabled={patchBusy} onClick={() => applyPatch({ rlEnabled: false })}>
                停用
              </button>
              <span className="pill">{featureSummary?.rl?.enabled ? 'enabled' : 'disabled'}</span>
            </div>
          </FieldRow>

          <FieldRow label="ADV" hint="对抗学习端点与对话触发">
            <div className="cfg-inline">
              <button className="btn" disabled={patchBusy} onClick={() => applyPatch({ advEnabled: true })}>
                启用
              </button>
              <button className="btn btn-ghost" disabled={patchBusy} onClick={() => applyPatch({ advEnabled: false })}>
                停用
              </button>
              <span className="pill">{featureSummary?.adv?.enabled ? 'enabled' : 'disabled'}</span>
            </div>
          </FieldRow>

          <FieldRow label="对话触发学习" hint="开启后才会按阈值触发 RL/ADV">
            <div className="cfg-inline">
              <button
                className="btn"
                disabled={patchBusy}
                onClick={() => applyPatch({ dialogLearningEnabled: true })}
              >
                启用
              </button>
              <button
                className="btn btn-ghost"
                disabled={patchBusy}
                onClick={() => applyPatch({ dialogLearningEnabled: false })}
              >
                停用
              </button>
              <span className="pill">{features?.dialogLearning?.enabled ? 'enabled' : 'disabled'}</span>
              <button
                className="btn btn-ghost"
                disabled={patchBusy}
                onClick={async () => {
                  try {
                    await api.dialogReset();
                    const f = await api.runtimeFeatures();
                    setFeatures(f?.features || null);
                  } catch (e) {
                    onError?.(e.message);
                  }
                }}
              >
                重置计数
              </button>
            </div>
          </FieldRow>

          <div className="hr" />

          <FieldRow label="对话触发阈值" hint="每 N 次对话触发学习">
            <div className="cfg-inline">
              <input className="input" value={rlEvery} onChange={(e) => setRlEvery(e.target.value)} placeholder="rlEvery" />
              <input className="input" value={advEvery} onChange={(e) => setAdvEvery(e.target.value)} placeholder="advEvery" />
              <button className="btn" disabled={patchBusy} onClick={saveThresholds}>
                保存
              </button>
            </div>
          </FieldRow>

          <div className="cfg-kv">
            <div className="muted">对话计数</div>
            <div className="mono">total={features?.dialogCounters?.total ?? 0}, lastRL={features?.dialogCounters?.lastRL ?? 0}, lastADV={features?.dialogCounters?.lastADV ?? 0}</div>
          </div>
        </section>

        <section className="card">
          <div className="card-title">Study 状态</div>
          <div className="muted">显示 study 队列与 worker 错误（用于排查为何未学习）</div>

          <div className="cfg-kv" style={{ marginTop: 10 }}>
            <div className="muted">running</div>
            <div className="mono">{String(studyStatus?.running ?? false)}</div>
          </div>
          <div className="cfg-kv">
            <div className="muted">queue</div>
            <div className="mono">{String(studyStatus?.queue ?? 0)}</div>
          </div>
          <div className="cfg-kv">
            <div className="muted">metrics</div>
            <div className="mono">
              enqueued={studyStatus?.metrics?.enqueued ?? 0}, processed={studyStatus?.metrics?.processed ?? 0}, lastError={studyStatus?.metrics?.lastError ?? 'null'}
            </div>
          </div>

          <div className="cfg-inline" style={{ marginTop: 10 }}>
            <button
              className="btn"
              onClick={async () => {
                try {
                  const s = await api.studyStatus();
                  setStudyStatus(s || null);
                } catch (e) {
                  onError?.(e.message);
                }
              }}
            >
              刷新 Study 状态
            </button>
          </div>
        </section>

        <section className="card">
          <div className="card-title">Tests 用例（运行时刷新）</div>
          <div className="muted">目录：{testsDir || '-'}</div>

          <div className="cfg-inline" style={{ marginTop: 10 }}>
            <button className="btn" onClick={refreshTests}>
              刷新 tests 词表
            </button>
            <button
              className="btn btn-ghost"
              onClick={async () => {
                try {
                  const t = await api.testsList();
                  setTestsDir(t?.directory || '');
                  setTestsFiles(Array.isArray(t?.files) ? t.files : []);
                } catch (e) {
                  onError?.(e.message);
                }
              }}
            >
              重新列出
            </button>
          </div>

          <div className="cfg-list">
            {(testsFiles || []).slice(0, 50).map((f) => (
              <div key={f} className="cfg-list-item">
                <span className="mono">{f}</span>
              </div>
            ))}
            {testsFiles.length > 50 ? <div className="muted">仅显示前 50 个</div> : null}
          </div>

          <div className="hr" />

          <div className="card-subtitle">新增/覆盖用例</div>
          <FieldRow label="文件名" hint="自动补 .txt，非法字符会被替换">
            <input className="input" value={newTestName} onChange={(e) => setNewTestName(e.target.value)} placeholder="case_001" />
          </FieldRow>
          <FieldRow label="内容" hint="任意文本；RL 侧按你的实现读取">
            <textarea className="textarea" value={newTestContent} onChange={(e) => setNewTestContent(e.target.value)} placeholder="输入测试文本…" />
          </FieldRow>
          <button className="btn" onClick={createTestCase}>
            写入用例
          </button>
        </section>

        <section className="card">
          <div className="card-title">Robots 重训（重新 ingest）</div>
          <div className="muted">选择 robots 文件并触发重新训练/重建词表</div>

          <FieldRow label="limit" hint="本次 ingest 文档数量（可空）">
            <input className="input" value={robotsLimit} onChange={(e) => setRobotsLimit(e.target.value)} placeholder="10" />
          </FieldRow>
          <FieldRow label="shuffle" hint="随机抽样">
            <label className="toggle">
              <input type="checkbox" checked={robotsShuffle} onChange={(e) => setRobotsShuffle(e.target.checked)} />
              <span>启用</span>
            </label>
          </FieldRow>
          <FieldRow label="enqueueStudy" hint="同时推送到 study 队列">
            <label className="toggle">
              <input type="checkbox" checked={robotsEnqueueStudy} onChange={(e) => setRobotsEnqueueStudy(e.target.checked)} />
              <span>启用</span>
            </label>
          </FieldRow>

          <div className="card-subtitle">选择文件（可多选）</div>
          <div className="cfg-checklist">
            {(robotsFiles || []).slice(0, 80).map((f) => {
              const checked = robotsSelected.includes(f);
              return (
                <label key={f} className="check">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => {
                      const on = e.target.checked;
                      setRobotsSelected((prev) => (on ? Array.from(new Set([...prev, f])) : prev.filter((x) => x !== f)));
                    }}
                  />
                  <span className="mono">{f}</span>
                </label>
              );
            })}
            {robotsFiles.length > 80 ? <div className="muted">仅显示前 80 个</div> : null}
          </div>

          <div className="cfg-inline" style={{ marginTop: 10 }}>
            <button className="btn" onClick={retrainRobots}>
              触发重训
            </button>
            <button className="btn btn-ghost" onClick={() => setRobotsSelected([])}>
              清空选择
            </button>
          </div>
        </section>

        <section className="card">
          <div className="card-title">线上搜索（Online Research）</div>
          <div className="muted">运行时开关 + 搜索网址库（endpoint 列表）</div>

          <FieldRow label="启用线上搜索" hint="关闭后 /api/corpus/online 将直接走本地 fallback">
            <div className="cfg-inline">
              <button className="btn" onClick={() => updateSearchConfig({ enabled: true })}>
                启用
              </button>
              <button className="btn btn-ghost" onClick={() => updateSearchConfig({ enabled: false })}>
                停用
              </button>
              <span className="pill">{searchConfig?.enabled ? 'enabled' : 'disabled'}</span>
            </div>
          </FieldRow>

          <FieldRow label="当前 endpoint" hint="用于远程 GET 请求的 base URL">
            <div className="cfg-inline">
              <select
                className="input"
                value={searchConfig?.active || ''}
                onChange={(e) => updateSearchConfig({ active: e.target.value })}
              >
                <option value="">(空)</option>
                {(searchConfig?.endpoints || []).map((u) => (
                  <option key={u} value={u}>
                    {u}
                  </option>
                ))}
              </select>
              <button
                className="btn btn-ghost"
                onClick={() => {
                  if (!searchConfig?.active) return;
                  navigator.clipboard?.writeText(searchConfig.active).catch(() => {});
                }}
              >
                复制
              </button>
            </div>
          </FieldRow>

          <div className="hr" />

          <div className="card-subtitle">添加 endpoint</div>
          <div className="cfg-inline">
            <input className="input" value={searchAddUrl} onChange={(e) => setSearchAddUrl(e.target.value)} placeholder="https://example.com/search" />
            <button
              className="btn"
              onClick={async () => {
                const url = searchAddUrl.trim();
                if (!url) return;
                try {
                  const r = await api.searchEndpointAdd(url);
                  setSearchConfig(r?.config || null);
                  setSearchAddUrl('');
                } catch (e) {
                  onError?.(e.message);
                }
              }}
            >
              添加
            </button>
          </div>

          <div className="cfg-list">
            {(searchConfig?.endpoints || []).map((u) => (
              <div key={u} className="cfg-list-item" style={{ display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'center' }}>
                <span className="mono" style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>{u}</span>
                <button
                  className="btn btn-ghost"
                  onClick={async () => {
                    try {
                      const r = await api.searchEndpointRemove(u);
                      setSearchConfig(r?.config || null);
                    } catch (e) {
                      onError?.(e.message);
                    }
                  }}
                >
                  删除
                </button>
              </div>
            ))}
            {!searchConfig?.endpoints?.length ? <div className="muted">暂无 endpoint</div> : null}
          </div>

          <div className="hr" />

          <div className="card-subtitle">快速测试</div>
          <div className="cfg-inline">
            <input className="input" value={searchTestQuery} onChange={(e) => setSearchTestQuery(e.target.value)} placeholder="输入 query，然后走 /api/corpus/online" />
            <button
              className="btn"
              onClick={async () => {
                try {
                  const q = searchTestQuery.trim();
                  if (!q) return;
                  const r = await api.requestOnline(q);
                  setSearchTestResult(r?.result || r);
                } catch (e) {
                  onError?.(e.message);
                }
              }}
            >
              测试
            </button>
          </div>
          {searchTestResult ? <pre className="mono" style={{ whiteSpace: 'pre-wrap', marginTop: 10 }}>{JSON.stringify(searchTestResult, null, 2)}</pre> : null}
        </section>
      </div>
    </div>
  );
}
