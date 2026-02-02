'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');
const vm = require('vm');
const http = require('http');
const crypto = require('crypto');
const childProcess = require('child_process');
const express = require('express');
const bodyParser = require('body-parser');
const redis = require('redis');
const workerpool = require('workerpool');
const EventEmitter = require('events');

const safeRequire = (name) => {
    try {
        return require(name);
    } catch (err) {
        return null;
    }
};

// 启动性能优化：重依赖懒加载（避免启动即加载 pdf-parse/cheerio/natural 等）。
const __lazyModules = new Map();
const lazyRequire = (name) => {
    if (__lazyModules.has(name)) {
        return __lazyModules.get(name);
    }
    const mod = safeRequire(name);
    __lazyModules.set(name, mod);
    return mod;
};

const getNatural = () => lazyRequire('natural');
const getCsvParse = () => lazyRequire('csv-parse/sync');
const getUmap = () => lazyRequire('umap-js');
const getAxios = () => lazyRequire('axios');
const getCheerio = () => lazyRequire('cheerio');
const getPdfParse = () => lazyRequire('pdf-parse');
const getMatrix = (() => {
    let loaded = false;
    let cached = null;
    return () => {
        if (loaded) return cached;
        loaded = true;
        const MatrixLib = lazyRequire('ml-matrix');
        cached = MatrixLib?.Matrix ?? MatrixLib ?? null;
        return cached;
    };
})();

const getStopWords = (() => {
    let loaded = false;
    let cached = [];
    return () => {
        if (loaded) return cached;
        loaded = true;
        const natural = getNatural();
        const list = natural?.stopwords ?? [];
        cached = Array.isArray(list) ? list : [];
        return cached;
    };
})();
const DEFAULT_CHANNEL = process.env.AI_REDIS_CHANNEL || 'AI-model-workspace';

/**
 * 外参列表（所有“外部可控参数”的入口汇总）
 *
 * 1) 启动时参数（CLI flags，形如 --k=v 或 --flag=true）
 * - --base-dir: 运行时数据目录（默认：./runtime_store；对应 ENV: AI_BASE_DIR）
 * - --gateway-host: 网关监听 host（默认：127.0.0.1；对应 ENV: AI_GATEWAY_HOST）
 * - --port: 网关端口（默认：5080；对应 ENV: CONTROLLER_PORT）
 * - --study-port: study/前端进程端口（默认：5081；对应 ENV: AI_STUDY_PORT）
 * - --ai-count: 旧版兼容字段；用于 aiCount（默认：7；对应 ENV: AI_COUNT / AI_NUM）
 * - --group-size: 每个工作组 AI 数量（默认：ai-count；对应 ENV: AI_GROUP_SIZE / GROUP_SIZE）
 * - --group-count: 组数量（默认：3；对应 ENV: AI_GROUP_COUNT / GROUP_COUNT）
 * - --spark-num-ai: SparkArray 每组参与汇聚的 AI 数量（默认：group-size；对应 ENV: AI_SPARK_NUM_AI）
 * - --spark-budget: SparkArray 低精度预算预设（default/low/high 或 JSON；对应 ENV: AI_SPARK_BUDGET）
 * - --robots-limit: robots 预热/导入条数上限（默认：200；对应 ENV: AI_ROBOTS_LIMIT）
 * - --redis-url: Redis 连接串（默认：redis://127.0.0.1:6379；对应 ENV: REDIS_URL）
 * - --channel: Redis pubsub 频道（默认：AI_REDIS_CHANNEL 或 'AI-model-workspace'）
 * - --snapshot-dir: 快照目录（默认：./snapshots）
 * - --lmdb-dir: LMDB 根目录（默认：./lmdb；对应 ENV: LMDB_DIR）
 * - --search-endpoint: 在线检索/搜索服务端点（默认：''；对应 ENV: AI_SEARCH_ENDPOINT）
 * - --robots-dir: robots 语料目录（默认：./robots；对应 ENV: AI_ROBOTS_DIR）
 * - --lemma-csv: lemma 词形还原表路径（默认：./lemma.csv；对应 ENV: AI_LEMMA_CSV）
 * - --robots-autoload: 启动时是否自动加载 robots（默认：true；对应 ENV: AI_ROBOTS_AUTOLOAD）
 * - --disable-memebarrier: 启动默认禁用 MemeBarrier（对应 ENV: AI_DISABLE_MEMEBARRIER）
 * - --disable-rl: 启动默认禁用 RL（对应 ENV: AI_DISABLE_RL）
 * - --disable-adv: 启动默认禁用 ADV（对应 ENV: AI_DISABLE_ADV）
 * - --disable-learning: 启动默认禁用学习总开关（对应 ENV: AI_DISABLE_LEARNING）
 * - --export-dir: 图导出目录（默认：./runtime_store；对应 ENV: AI_EXPORT_DIR）
 *
 * 2) 环境变量（ENV）
 * - AI_REDIS_CHANNEL: Redis 频道默认值（被 --channel 覆盖）
 * - AI_AUTH_ENABLED: 是否启用 /api/* 鉴权（默认：true；'false' 关闭）
 * - AI_AUTH_JWT_SECRET / AUTH_JWT_SECRET: JWT 密钥（默认：'dev-secret-change-me'）
 *   - 说明：鉴权默认保护 /api/*，仅 /api/system/status 在 publicPaths 白名单
 *
 * 3) 运行时参数（HTTP API，可在不重启的情况下调整）
 * - POST /api/chat
 *   - body.text / body.message: 输入文本
 *   - body.sessionId: 会话 ID（可选；未提供则自动分配）
 *   - body.tokens / body.words / body.vocab: 直接提供分词（可选；否则对 text 做 tokenize）
 *
 * - GET  /api/model/params: 读取当前模型参数
 * - POST /api/model/params: patch 模型参数（部分字段）
 * - POST /api/model/params/reset: 重置为 modelDefaults
 *   - 可 patch 的 key（见下方 modelDefaults）：
 *     decayFactor, maxMemeWords, minOverlapThreshold, memeNgramMin, memeNgramMax,
 *     maliciousThreshold, learningIterations, iteration, threshold, decay, decayK,
 *     maxLen, edgeWeight, activationType, transferType, activationCustom, transferCustom
 *
 * - GET  /api/runtime/features: 读取运行时功能开关状态
 * - PATCH /api/runtime/features: patch 运行时功能开关（字段如下）
 *   - memebarrierEnabled: boolean
 *   - maliciousThreshold: number
 *   - learningEnabled: boolean（总开关；false 会同时关闭 rl/adv/dialogLearning）
 *   - rlEnabled / advEnabled: boolean
 *   - dialogLearningEnabled: boolean
 *   - rlEvery / advEvery: number（对话触发学习的阈值）
 *   - 注意：CLI 的 disable* 表示“启动默认禁用”，运行时仍允许 override（会返回 warnings）
 *
 * - POST /api/learn/thresholds: { rlEvery, advEvery }
 * - POST /api/learn/reinforce: { cycles }（默认：3）
 */

const ensureDir = (dir) => {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
};

const parseArgs = (argv) => {
    const out = {};
    argv.forEach((item) => {
        const [k, v] = item.split('=');
        if (k.startsWith('--')) {
            out[k.slice(2)] = v ?? true;
        }
    });
    return out;
};

const CONFIG = (() => {
    const args = parseArgs(process.argv.slice(2));
    const boolFrom = (value, fallback = true) => {
        if (value === undefined || value === null) {
            return fallback;
        }
        const normalized = String(value).trim().toLowerCase();
        return !(normalized === '0' || normalized === 'false' || normalized === 'off' || normalized === 'no');
    };
    const robotsLimitRaw = args['robots-limit'] || process.env.AI_ROBOTS_LIMIT || 200;
    const robotsChunkMinRaw = args['robots-chunk-min'] || process.env.AI_ROBOTS_CHUNK_MIN || 3;
    const robotsChunkMaxRaw = args['robots-chunk-max'] || process.env.AI_ROBOTS_CHUNK_MAX || 20;
    const lmdbMapMbRaw = args['lmdb-map-mb'] || process.env.AI_LMDB_MAP_MB || 512;
    const kvmCacheMaxRaw = args['kvm-cache-max'] || process.env.AI_KVM_CACHE_MAX || 50_000;
    const lemmaMaxMbRaw = args['lemma-max-mb'] || process.env.AI_LEMMA_MAX_MB || 64;
    const aiCountRaw = args['ai-count'] || process.env.AI_COUNT || process.env.AI_NUM || 7;
    const groupCountRaw = args['group-count'] || process.env.AI_GROUP_COUNT || process.env.GROUP_COUNT || 3;
    const groupSizeRaw = args['group-size'] || process.env.AI_GROUP_SIZE || process.env.GROUP_SIZE || aiCountRaw;
    const sparkNumAiRaw = args['spark-num-ai'] || process.env.AI_SPARK_NUM_AI || groupSizeRaw;
    const sparkBudgetRaw = args['spark-budget'] || process.env.AI_SPARK_BUDGET || 'default';
    const groupProcRaw = args['group-proc'] ?? process.env.AI_GROUP_PROC;
    const inferMpRaw = args['infer-mp'] ?? process.env.AI_INFER_MP;
    const inferWorkersRaw = args['infer-workers'] || process.env.AI_INFER_WORKERS;
    const singleProcRaw = args['single-proc'] ?? process.env.AI_SINGLE_PROC;
    const redisTimeoutRaw = args['redis-timeout-ms'] || process.env.AI_REDIS_CONNECT_TIMEOUT_MS || 1500;
    const groupProcTimeoutRaw = args['group-proc-timeout-ms'] || process.env.AI_GROUP_PROC_TIMEOUT_MS || 12_000;
    const singleProc = boolFrom(singleProcRaw, false);
    return {
        baseDir: path.resolve(args['base-dir'] || process.env.AI_BASE_DIR || path.join(__dirname, 'runtime_store')),
        gatewayHost: String(args['gateway-host'] || process.env.AI_GATEWAY_HOST || '127.0.0.1'),
        portGateway: Number(args.port || process.env.CONTROLLER_PORT || 5080),
        portStudy: Number(args['study-port'] || process.env.AI_STUDY_PORT || 5081),
        aiCount: Math.max(3, Number(aiCountRaw) || 7),
        groupCount: Math.max(1, Number(groupCountRaw) || 3),
        // 兼容：若只传了 --ai-count，则默认 groupSize=aiCount
        groupSize: Math.max(1, Number(groupSizeRaw) || (Number(aiCountRaw) || 7)),
        // SparkArray：每组参与汇聚的实例数量（不超过该组实际 controller 数量）
        sparkNumAI: Math.max(1, Number(sparkNumAiRaw) || (Number(groupSizeRaw) || (Number(aiCountRaw) || 7))),
        // SparkArray：低精度预算（可传 preset：default/low/high，或 JSON 对象）
        sparkBudget: sparkBudgetRaw,
        // 每组一个进程：将每个 group 的 controller/runtime 放到独立子进程（fork）中，主进程仅做网关与汇聚调度。
        // 目标：让 BigSparkArray 的多组并行真正吃到多核。
        singleProc,
        groupProc: singleProc ? false : boolFrom(groupProcRaw, false),
        groupProcTimeoutMs: Math.max(500, Number(groupProcTimeoutRaw) || 12_000),
        // 多进程推理（将 SparkArray 的每个 controller.respond 分发到子进程，利用多核加速）
        inferMP: singleProc ? false : boolFrom(inferMpRaw, false),
        inferWorkers: Math.max(1, Number(inferWorkersRaw) || Math.max(1, (os.cpus()?.length ?? 2) - 1)),
        redisConnectTimeoutMs: Math.max(200, Number(redisTimeoutRaw) || 1500),
        redisUrl: process.env.REDIS_URL || args['redis-url'] || 'redis://127.0.0.1:6379',
        redisChannel: args.channel || DEFAULT_CHANNEL,
        snapshotDir: args['snapshot-dir'] || path.join(__dirname, 'snapshots'),
        workerFile: path.join(__dirname, 'memeMergeWorker.cjs'),
        maxWorkers: Math.max(1, (os.cpus()?.length ?? 2) - 1),
        shardCache: path.join(__dirname, 'shards_cache.json'),
        lmdbRoot: path.join(args['lmdb-dir'] || process.env.LMDB_DIR || path.join(__dirname, 'lmdb')),
        lmdbMapSizeBytes: Math.max(64, Number(lmdbMapMbRaw) || 512) * 1024 * 1024,
        searchEndpoint: args['search-endpoint'] || process.env.AI_SEARCH_ENDPOINT || '',
        robotsDir: path.resolve(args['robots-dir'] || process.env.AI_ROBOTS_DIR || path.join(__dirname, 'robots')),
        lemmaCsv: path.resolve(args['lemma-csv'] || process.env.AI_LEMMA_CSV || path.join(__dirname, 'lemma.csv')),
        lemmaAutoload: boolFrom(args['lemma-autoload'] ?? process.env.AI_LEMMA_AUTOLOAD, false),
        lemmaMaxBytes: Math.max(1, Number(lemmaMaxMbRaw) || 64) * 1024 * 1024,
        lemmaForce: boolFrom(args['lemma-force'] ?? process.env.AI_LEMMA_FORCE, false),
        robotsWarmupLimit: Math.max(0, Number(robotsLimitRaw) || 0),
        robotsAutoload: boolFrom(args['robots-autoload'] ?? process.env.AI_ROBOTS_AUTOLOAD, true),
        robotsWarmupShuffle: boolFrom(args['robots-warmup-shuffle'] ?? process.env.AI_ROBOTS_WARMUP_SHUFFLE, false),
        robotsChunkMinWords: Math.max(1, Number(robotsChunkMinRaw) || 2),
        robotsChunkMaxWords: Math.max(1, Number(robotsChunkMaxRaw) || 5),
        kvmCacheMaxEntries: Math.max(0, Number(kvmCacheMaxRaw) || 0),
        learningWarmup: boolFrom(args['learning-warmup'] ?? process.env.AI_LEARNING_WARMUP, false),
        // 启动时是否把 serving 的 snapshot 同步到 standby/validation（可能很慢；默认关闭以保证 fast-boot）
        syncStandbyOnBoot: boolFrom(args['sync-standby'] ?? process.env.AI_SYNC_STANDBY_ON_BOOT, false),
        // tests 语料预加载（可能较慢；默认开启，fast-boot 可设为 false）
        testsAutoload: boolFrom(args['tests-autoload'] ?? process.env.AI_TESTS_AUTOLOAD, true),
        // Feature toggles via CLI/env
        disableBarrier: boolFrom(args['disable-memebarrier'] ?? process.env.AI_DISABLE_MEMEBARRIER, false) === true,
        disableRL: boolFrom(args['disable-rl'] ?? process.env.AI_DISABLE_RL, false) === true,
        disableADV: boolFrom(args['disable-adv'] ?? process.env.AI_DISABLE_ADV, false) === true,
        disableLearning: boolFrom(args['disable-learning'] ?? process.env.AI_DISABLE_LEARNING, false) === true,
        exportDir: path.resolve(args['export-dir'] || process.env.AI_EXPORT_DIR || path.join(__dirname, 'runtime_store'))
    };
})();

class GroupProcessPool extends EventEmitter {
    constructor({ config } = {}) {
        super();
        this.config = config || CONFIG;
        const groupCount = Math.max(1, Number(this.config?.groupCount ?? CONFIG.groupCount) || 1);
        const groupSize = Math.max(1, Number(this.config?.groupSize ?? CONFIG.groupSize) || 1);
        this.groupIds = Array.from({ length: groupCount }, (_, i) => `G${i + 1}`);
        this.groups = {};
        this.controllerToGroup = {};
        this.controllers = {};
        this.workers = {};
        this.pending = new Map();
        this.seq = 1;
        this.servingName = null;

        for (const groupId of this.groupIds) {
            const names = [];
            for (let i = 0; i < groupSize; i++) {
                const name = `${groupId}_AI${i + 1}`;
                names.push(name);
                this.controllerToGroup[name] = groupId;
            }
            this.groups[groupId] = names;
        }
        const g1 = this.groupIds[0] || 'G1';
        this.servingName = (this.groups[g1] || [])[0] || `${g1}_AI1`;

        this._spawnAllWorkers();
    }

    _spawnAllWorkers() {
        const workerPath = path.join(__dirname, 'groupWorker.cjs');
        for (const groupId of this.groupIds) {
            const child = childProcess.fork(workerPath, [], {
                env: {
                    ...process.env,
                    AI_GROUP_ID: groupId,
                    AI_GROUP_SIZE: String(CONFIG.groupSize),
                    AI_GROUP_COUNT: String(CONFIG.groupCount),
                    AI_LMDB_ROOT: String(CONFIG.lmdbRoot),
                    AI_LMDB_MAP_BYTES: String(CONFIG.lmdbMapSizeBytes),
                    AI_BASE_DIR: String(CONFIG.baseDir)
                },
                stdio: ['inherit', 'inherit', 'inherit', 'ipc']
            });
            child.on('message', (msg) => this._onMessage(groupId, msg));
            child.on('exit', (code, signal) => {
                console.warn(`[GroupProc] worker ${groupId} exited code=${code} signal=${signal}`);
            });
            this.workers[groupId] = child;
        }
    }

    _onMessage(groupId, msg) {
        if (!msg || typeof msg !== 'object') return;
        const id = msg.id;
        if (!id) return;
        const pending = this.pending.get(id);
        if (!pending) return;
        this.pending.delete(id);
        clearTimeout(pending.timer);
        if (msg.ok) {
            pending.resolve(msg.result);
        } else {
            const err = new Error(msg.error || `group-worker-error:${groupId}`);
            err.worker = groupId;
            err.code = msg.code;
            pending.reject(err);
        }
    }

    _rpc(groupId, cmd, payload) {
        const worker = this.workers[groupId];
        if (!worker || worker.killed) {
            return Promise.reject(new Error(`group-worker-unavailable:${groupId}`));
        }
        const id = `${Date.now()}_${process.pid}_${this.seq++}`;
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error(`group-worker-timeout:${groupId}:${cmd}`));
            }, CONFIG.groupProcTimeoutMs);
            this.pending.set(id, { resolve, reject, timer });
            try {
                worker.send({ id, cmd, ...payload });
            } catch (e) {
                clearTimeout(timer);
                this.pending.delete(id);
                reject(e);
            }
        });
    }

    shutdown() {
        for (const child of Object.values(this.workers)) {
            try { child.kill(); } catch (_e) {}
        }
        this.workers = {};
        for (const [id, pending] of this.pending.entries()) {
            clearTimeout(pending.timer);
            pending.reject(new Error('group-worker-shutdown'));
        }
        this.pending.clear();
    }

    listGroupIds() {
        return this.groupIds.slice();
    }

    listControllersInGroup(groupId) {
        return (this.groups[String(groupId || '').trim()] || []).slice();
    }

    listControllerNames() {
        return this.groupIds.flatMap((gid) => this.listControllersInGroup(gid));
    }

    getActive() {
        return this.getByName(this.servingName);
    }

    getByName(name) {
        const n = String(name || '').trim();
        if (!n) return null;
        if (n === 'A' || n === 'B' || n === 'C') {
            const g1 = this.groupIds[0] || 'G1';
            const list = this.groups[g1] || [];
            if (n === 'A') return this.getByName(list[0]);
            if (n === 'B') return this.getByName(list[1]);
            if (n === 'C') return this.getByName(list[2]);
        }
        if (this.controllers[n]) return this.controllers[n];
        const groupId = this.controllerToGroup[n];
        if (!groupId) return null;
        const proxy = new ProxyController(n, groupId, this);
        this.controllers[n] = proxy;
        return proxy;
    }

    listMetrics() {
        const jobs = this.groupIds.map((gid) => this._rpc(gid, 'listMetrics', {}));
        return Promise.allSettled(jobs).then((parts) => {
            const out = [];
            for (const p of parts) {
                if (p.status === 'fulfilled' && Array.isArray(p.value)) {
                    out.push(...p.value);
                }
            }
            return out;
        });
    }

    async ingestDocument(doc) {
        const jobs = this.groupIds.map((gid) => this._rpc(gid, 'ingestDocument', { doc }));
        const parts = await Promise.all(jobs);
        return parts.flat();
    }

    async ingestDocumentTo(name, doc) {
        const ctrl = this.getByName(name);
        if (!ctrl) return { ok: false, reason: 'controller-not-found' };
        return this._rpc(ctrl.groupId, 'ingestDocumentTo', { controllerName: ctrl.name, doc });
    }

    async ingestDocumentToGroup(groupId, doc) {
        const gid = String(groupId || '').trim();
        if (!this.groups[gid] || !this.groups[gid].length) {
            return { ok: false, reason: 'group-not-found' };
        }
        return this._rpc(gid, 'ingestDocumentToGroup', { doc });
    }

    async forgetMemes(criteria) {
        const jobs = this.groupIds.map((gid) => this._rpc(gid, 'forgetMemes', { criteria }));
        const parts = await Promise.all(jobs);
        return parts.flat();
    }

    async onlineResearch(input, options = {}) {
        const active = this.getActive();
        if (!active) throw new Error('no-active-controller');
        return this._rpc(active.groupId, 'runtimeCall', {
            controllerName: active.name,
            method: 'onlineLookup',
            args: [input, options]
        });
    }

    async applySnapshotAll(snapshot) {
        const jobs = this.groupIds.map((gid) => this._rpc(gid, 'applySnapshotAll', { snapshot }));
        await Promise.all(jobs);
        return true;
    }
}

class ProxyRuntime {
    constructor(controller, pool) {
        this.controller = controller;
        this.pool = pool;
        this.config = {
            robotsDir: CONFIG.robotsDir,
            testsDir: path.join(__dirname, 'tests')
        };
    }

    cloneParams() {
        return this.pool._rpc(this.controller.groupId, 'cloneParams', { controllerName: this.controller.name });
    }

    toSnapshot() {
        return this.pool._rpc(this.controller.groupId, 'snapshot', { controllerName: this.controller.name });
    }

    fromSnapshot(snapshot) {
        return this.pool._rpc(this.controller.groupId, 'applySnapshot', { controllerName: this.controller.name, snapshot });
    }

    onlineLookup(input, options = {}) {
        return this.pool._rpc(this.controller.groupId, 'runtimeCall', {
            controllerName: this.controller.name,
            method: 'onlineLookup',
            args: [input, options]
        });
    }

    collectRobotsDocuments(options = {}) {
        return this.pool._rpc(this.controller.groupId, 'runtimeCall', {
            controllerName: this.controller.name,
            method: 'collectRobotsDocuments',
            args: [options]
        });
    }

    listRobotsFiles() {
        return this.pool._rpc(this.controller.groupId, 'runtimeCall', {
            controllerName: this.controller.name,
            method: 'listRobotsFiles',
            args: []
        });
    }

    exportGraphToFile(options = {}) {
        return this.pool._rpc(this.controller.groupId, 'runtimeCall', {
            controllerName: this.controller.name,
            method: 'exportGraphToFile',
            args: [options]
        });
    }

    getSearchConfig() {
        return this.pool._rpc(this.controller.groupId, 'runtimeCall', {
            controllerName: this.controller.name,
            method: 'getSearchConfig',
            args: []
        });
    }

    setSearchConfig(cfg = {}) {
        return this.pool._rpc(this.controller.groupId, 'runtimeCall', {
            controllerName: this.controller.name,
            method: 'setSearchConfig',
            args: [cfg]
        });
    }

    learnFromDialog({ payload, result } = {}) {
        return this.pool._rpc(this.controller.groupId, 'runtimeCall', {
            controllerName: this.controller.name,
            method: 'learnFromDialog',
            args: [{ payload, result }]
        });
    }
}

class ProxyController {
    constructor(name, groupId, pool) {
        this.name = name;
        this.groupId = groupId;
        this.pool = pool;
        this.online = true;
        this.health = { status: 'ok', since: Date.now(), failures: 0 };
        this.runtime = new ProxyRuntime(this, pool);
    }

    async respond(payload) {
        return this.pool._rpc(this.groupId, 'respond', { controllerName: this.name, payload });
    }

    metrics() {
        return this.pool._rpc(this.groupId, 'metrics', { controllerName: this.name });
    }

    applyParams(params) {
        return this.pool._rpc(this.groupId, 'applyParams', { controllerName: this.name, params });
    }

    async applySnapshot(snapshot) {
        return this.pool._rpc(this.groupId, 'applySnapshot', { controllerName: this.name, snapshot });
    }

    snapshot() {
        return this.pool._rpc(this.groupId, 'snapshot', { controllerName: this.name });
    }
}

ensureDir(CONFIG.baseDir);
ensureDir(CONFIG.snapshotDir);
ensureDir(CONFIG.lmdbRoot);
ensureDir(CONFIG.robotsDir);

const LMDB = safeRequire('lmdb');

class LmdbStore {
    constructor({ name, rootDir, encodeJSON = true, mapSizeBytes } = {}) {
        this.name = name;
        this.rootDir = rootDir;
        this.encodeJSON = encodeJSON;
        this.backend = LMDB;
        this.isClosed = false;
        if (this.backend) {
            try {
                const envPath = path.join(rootDir, name);
                ensureDir(envPath);
                const resolvedMapSize = Number.isFinite(Number(mapSizeBytes)) && Number(mapSizeBytes) > 0
                    ? Number(mapSizeBytes)
                    : 512 * 1024 * 1024;
                this.env = this.backend.open({
                    path: envPath,
                    maxReaders: 64,
                    mapSize: resolvedMapSize,
                    useWritemap: true,
                    noSync: false
                });
                this.db = this.env.openDB({ name: 'default', create: true });
            } catch (err) {
                console.warn(`[LMDB] Falling back to JSON store for ${name}: ${err.message}`);
                this.backend = null;
                if (this.env) {
                    try {
                        this.env.close();
                    } catch (_e) {
                        // ignore cleanup failure
                    }
                }
            }
        }

        if (!this.backend) {
            this.file = path.join(rootDir, `${name}.json`);
            ensureDir(rootDir);
            this.map = new Map();
            if (fs.existsSync(this.file)) {
                try {
                    const raw = fs.readFileSync(this.file, 'utf8');
                    const parsed = JSON.parse(raw);
                    for (const [k, v] of Object.entries(parsed)) {
                        this.map.set(k, v);
                    }
                } catch (err) {
                    console.warn(`[LMDB-FALLBACK] Failed to read ${this.file}:`, err.message);
                }
            }
            this.dirty = false;
            this.flushTimer = setInterval(() => this.flush(), 10_000);
        }
    }

    _encode(value) {
        if (!this.encodeJSON) {
            return value;
        }
        return JSON.stringify(value);
    }

    _decode(raw) {
        if (!this.encodeJSON || raw === undefined || raw === null) {
            return raw;
        }
        if (typeof raw === 'string') {
            try {
                return JSON.parse(raw);
            } catch (err) {
                return raw;
            }
        }
        if (Buffer.isBuffer(raw)) {
            try {
                return JSON.parse(raw.toString('utf8'));
            } catch (err) {
                return raw;
            }
        }
        return raw;
    }

    get(key) {
        if (this.db) {
            const value = this.db.get(key);
            return this._decode(value);
        }
        return this._decode(this.map.get(key));
    }

    put(key, value) {
        if (this.db) {
            this.db.put(key, this.encodeJSON ? this._encode(value) : value);
            return;
        }
        this.map.set(key, this.encodeJSON ? this._encode(value) : value);
        this.dirty = true;
    }

    delete(key) {
        if (this.db) {
            this.db.remove(key);
            return;
        }
        this.map.delete(key);
        this.dirty = true;
    }

    entries(prefix = '') {
        if (this.db) {
            const it = this.db.getRange({ start: prefix });
            const arr = [];
            for (const { key, value } of it) {
                if (!String(key).startsWith(prefix)) {
                    break;
                }
                arr.push([key, this._decode(value)]);
            }
            return arr;
        }
        const out = [];
        for (const [key, value] of this.map.entries()) {
            if (String(key).startsWith(prefix)) {
                out.push([key, this._decode(value)]);
            }
        }
        return out;
    }

    flush() {
        if (this.db || !this.dirty) {
            return;
        }
        const plain = Object.fromEntries(this.map);
        fs.writeFileSync(this.file, JSON.stringify(plain, null, 2));
        this.dirty = false;
    }

    close() {
        if (this.isClosed) {
            return;
        }
        if (this.db) {
            this.db.close();
            this.env.close();
        } else {
            clearInterval(this.flushTimer);
            this.flush();
        }
        this.isClosed = true;
    }
}

class NamespacedStore {
    constructor(store, namespace) {
        this.store = store;
        this.ns = String(namespace || '').trim();
        if (!this.ns) {
            throw new Error('NamespacedStore requires a namespace');
        }
        this.prefix = `ns:${this.ns}:`;
    }

    _k(key) {
        return `${this.prefix}${String(key)}`;
    }

    get(key) {
        return this.store.get(this._k(key));
    }

    put(key, value) {
        return this.store.put(this._k(key), value);
    }

    delete(key) {
        return this.store.delete(this._k(key));
    }

    entries(prefix = '') {
        const list = this.store.entries(this._k(prefix));
        const out = [];
        for (const [key, value] of list) {
            const k = String(key);
            if (!k.startsWith(this.prefix)) {
                continue;
            }
            out.push([k.slice(this.prefix.length), value]);
        }
        return out;
    }

    flush() {
        if (typeof this.store.flush === 'function') {
            this.store.flush();
        }
    }

    close() {
        // underlying store lifecycle is managed centrally
    }
}

process.on('exit', () => {
    // In this single-file mode stores are closed elsewhere.
});

const BuiltinActivations = {
    identity: (x) => x,
    relu: (x) => (x > 0 ? x : 0),
    leaky_relu: (x) => (x > 0 ? x : 0.01 * x),
    tanh: (x) => Math.tanh(x),
    sigmoid: (x) => 1 / (1 + Math.exp(-x)),
    elu: (x) => (x >= 0 ? x : (Math.exp(x) - 1)),
    softplus: (x) => Math.log(1 + Math.exp(x)),
    gelu: (x) => 0.5 * x * (1 + Math.tanh(Math.sqrt(2 / Math.PI) * (x + 0.044715 * Math.pow(x, 3))))
};

const BuiltinTransfers = {
    linear: (value, weight, decayK, ctx) => {
        const dm = ctx?.direction === 0 ? (ctx?.bidirectionalMultiplier ?? 1.2) : (ctx?.directionalMultiplier ?? 0.7);
        return value - (decayK * weight * dm);
    },
    exp: (value, weight, decayK, ctx) => {
        const dm = ctx?.direction === 0 ? (ctx?.bidirectionalMultiplier ?? 1.2) : (ctx?.directionalMultiplier ?? 0.7);
        return value * Math.exp(-(decayK * weight * dm));
    },
    inverse: (value, weight, decayK, ctx) => {
        const dm = ctx?.direction === 0 ? (ctx?.bidirectionalMultiplier ?? 1.2) : (ctx?.directionalMultiplier ?? 0.7);
        return value / (1 + (decayK * weight * dm));
    },
    capped: (value, weight, decayK, ctx) => {
        const dm = ctx?.direction === 0 ? (ctx?.bidirectionalMultiplier ?? 1.2) : (ctx?.directionalMultiplier ?? 0.7);
        const raw = value - (decayK * weight * dm);
        return Math.max(0, Math.min(value, raw));
    }
};

const compileCustomFunctionSafely = (source, argNames, fallback) => {
    if (!source || typeof source !== 'string' || !source.trim()) {
        return fallback;
    }
    try {
        const wrapped = source.includes('return') ? source : `return (${source});`;
        const script = new vm.Script(`(${wrapped})`);
        const fn = script.runInNewContext({}, { timeout: 100 });
        if (typeof fn !== 'function') {
            return fallback;
        }
        return (...args) => fn(...args);
    } catch (err) {
        console.warn('[VM] Failed to compile custom function:', err.message);
        return fallback;
    }
};

/**
 * 模型外参/超参（面向调参/评测；与 CLI/ENV 无关）
 *
 * 修改方式：
 * - 运行时 patch：POST /api/model/params 传入 { key: value }
 * - 恢复默认：POST /api/model/params/reset
 * - 读取当前值：GET  /api/model/params
 *
 * 逐项说明（默认值以此处为准）：
 * - iteration (5): 传播迭代步数；用于 RuntimeState.runPropagation()/exportGraphToFile() -> TensorEngine.iteratePropagation().
 * - decayK (1): 传播衰减系数；传给 TensorEngine.iteratePropagation(csr, seeds, steps, actFn, decayK, damp).
 *
 * - memeNgramMin (2) / memeNgramMax (4): 构建“短语模因(ngram)”的长度范围；用于 mapWordsToMemes() 与 _buildMemeSequenceFromTokens().
 * - minOverlapThreshold (2): tokenSet 与既有 meme 的最小重合词数；满足则“融合”到该 meme（link 词 -> meme）。
 * - maxMemeWords (100): tokenSet 去重后的最大词数上限（用于限制短语模因的词集合大小）。
 *
 * - maliciousThreshold (0.7): MemeBarrier 判定阈值（网关侧安全屏障）；也可通过 PATCH /api/runtime/features 调整。
 *
 * - activationType ('relu'): 激活函数类型；用于 _activation()。
 *   - 可用类型见 module.exports.BUILTIN_ACTIVATION_TYPES；当为 'custom' 时使用 activationCustom。
 * - activationCustom (''): 自定义激活函数源码（function(x){...} 或表达式）；仅 activationType='custom' 时生效。
 *
 * - transferType ('linear') / transferCustom (''):
 *   - 预留：目前本文件内未在主传播路径中调用（仅实现了 _transfer() 与 BuiltinTransfers）。
 *
 * - decayFactor (0.5), learningIterations (3), threshold (3), decay (1), maxLen (16), edgeWeight (1):
 *   - 预留：当前版本 main.cjs 中未发现显式读取点（可能供未来/外部实验使用）。
 */
const modelDefaults = {
    decayFactor: 0.5,
    maxMemeWords: 100,
    minOverlapThreshold: 2,
    memeNgramMin: 3,
    memeNgramMax: 14,
    maliciousThreshold: 0.7,
    learningIterations: 3,
    iteration: 5,
    threshold: 3,
    decay: 1,
    decayK: 1,
    maxLen: 16,
    edgeWeight: 1,
    activationType: 'relu',
    transferType: 'linear',
    activationCustom: '',
    transferCustom: '',
    // 多次映射/镜面反射层（文明演算法思想）：words -> memes -> words -> memes ...
    mappingDepth: 1,
    reflectionTopMemes: 18,
    reflectionTopWords: 24,
    reflectionMinScore: 1e-6
};

const hashString = (str) => {
    return crypto.createHash('sha1').update(str).digest('hex');
};

const tokenize = (text) => {
    const normalized = String(text || '')
        .toLowerCase()
        .replace(/[\r\n\t]+/g, ' ');
    const parts = normalized
        .split(/[^a-z0-9_\-\u4e00-\u9fff]+/)
        .filter(Boolean);
    if (!parts.length) {
        return [];
    }
    const tokens = [];
    for (const part of parts) {
        if (!part) {
            continue;
        }
        if (/^[a-z0-9_\-]+$/.test(part) && getStopWords().includes(part)) {
            continue;
        }
        tokens.push(part);
    }
    return tokens;
};

const splitSentences = (text) => {
    const raw = String(text || '').trim();
    if (!raw) {
        return [];
    }

    // 说明：此函数用于“轻量切分成可学习/可检索的文本单元”。
    // 这里不再按标点分句作为唯一粒度，而是将文本切成“2-10 个词”的短片段。
    // 这样 robots 语料、surface phrase 抽取等模块能获得更细粒度的共现结构。
    const maxWords = 10;
    const minWords = 2;

    // 先粗分段（保留换行/句末标点作为天然边界），再在段内按词切块。
    const rough = raw
        .split(/[\r\n]+|(?<=[。！？!?])\s*/g)
        .map((s) => String(s || '').trim())
        .filter(Boolean);

    const out = [];
    for (const unit of rough) {
        const tokens = tokenize(unit);
        if (tokens.length < minWords) {
            continue;
        }

        for (let i = 0; i < tokens.length; i += maxWords) {
            const chunk = tokens.slice(i, i + maxWords);
            if (chunk.length < minWords) {
                // 末尾不足 2 词：尽量并入上一块
                if (out.length) {
                    out[out.length - 1] = `${out[out.length - 1]} ${chunk.join(' ')}`.trim();
                }
                continue;
            }
            out.push(chunk.join(' '));
            if (out.length >= 12) {
                return out;
            }
        }

        if (out.length >= 12) {
            break;
        }
    }

    return out.slice(0, 12);
};

const extractSurfacePhrases = (text, { maxPhrases = 24 } = {}) => {
    const out = [];
    const seen = new Set();

    const push = (phrase, weight = 1) => {
        const p = String(phrase || '').trim();
        if (!p) return;
        if (p.length < 2) return;
        if (p.length > 160) return;
        if (seen.has(p)) return;
        seen.add(p);
        out.push({ phrase: p, weight });
    };

    // 细化：优先保留“词/短语结构”，句子仅作弱特征。
    const sentences = splitSentences(text);
    for (const s of sentences) {
        push(s, 1);
    }

    const tokens = tokenize(text);
    if (tokens.length) {
        // unigram
        for (const t of tokens.slice(0, maxPhrases)) {
            push(t, 2);
            if (out.length >= maxPhrases) {
                return out.slice(0, maxPhrases);
            }
        }

        // n-gram (短语)
        const maxN = Math.min(5, tokens.length);
        for (let n = 2; n <= maxN; n++) {
            for (let i = 0; i + n <= tokens.length; i++) {
                const gram = tokens.slice(i, i + n).join(' ');
                const w = n === 2 ? 3 : (n === 3 ? 2 : 1);
                push(gram, w);
                if (out.length >= maxPhrases) {
                    return out.slice(0, maxPhrases);
                }
            }
        }
    }

    return out.slice(0, maxPhrases);
};

class MemeSurfaceLexicon {
    constructor(store, {
        maxEntriesPerMeme = 64,
        decay = 0.985
    } = {}) {
        this.store = store;
        this.maxEntriesPerMeme = Math.max(8, Number(maxEntriesPerMeme) || 64);
        this.decay = Number.isFinite(Number(decay)) ? Number(decay) : 0.985;
    }

    _key(memeId) {
        return `m:${String(memeId)}`;
    }

    _load(memeId) {
        const raw = this.store.get(this._key(memeId));
        if (!raw || typeof raw !== 'object') {
            return { phrases: {}, updatedAt: 0 };
        }
        const phrases = raw.phrases && typeof raw.phrases === 'object' ? raw.phrases : {};
        return { phrases, updatedAt: Number(raw.updatedAt || 0) || 0 };
    }

    _save(memeId, rec) {
        this.store.put(this._key(memeId), rec);
    }

    learn(memeId, replyText, { weight = 1 } = {}) {
        if (!memeId) return;
        const w = Number.isFinite(Number(weight)) ? Number(weight) : 1;
        const rec = this._load(memeId);
        const next = { phrases: { ...rec.phrases }, updatedAt: Date.now() };

        // 对旧条目做轻量衰减，防止早期噪声长期占据。
        for (const [k, v] of Object.entries(next.phrases)) {
            const nv = (Number(v) || 0) * this.decay;
            if (nv <= 1e-6) {
                delete next.phrases[k];
            } else {
                next.phrases[k] = nv;
            }
        }

        const phrases = extractSurfacePhrases(replyText, { maxPhrases: 24 });
        for (const p of phrases) {
            next.phrases[p.phrase] = (Number(next.phrases[p.phrase]) || 0) + (p.weight * w);
        }

        // 裁剪到 topN
        const ordered = Object.entries(next.phrases)
            .sort((a, b) => (Number(b[1]) || 0) - (Number(a[1]) || 0))
            .slice(0, this.maxEntriesPerMeme);
        next.phrases = Object.fromEntries(ordered);
        this._save(memeId, next);
    }

    getTop(memeId, { limit = 6 } = {}) {
        const rec = this._load(memeId);
        const ordered = Object.entries(rec.phrases || {})
            .sort((a, b) => (Number(b[1]) || 0) - (Number(a[1]) || 0))
            .slice(0, Math.max(1, Number(limit) || 6))
            .map(([phrase, score]) => ({ phrase, score: Number(score) || 0 }));
        return ordered;
    }

    exportSnapshot({ limitMemes = 512 } = {}) {
        const out = [];
        const items = this.store.entries('m:');
        for (const [key, value] of items) {
            out.push([key, value]);
            if (out.length >= limitMemes) break;
        }
        return out;
    }

    importSnapshot(entries) {
        if (!Array.isArray(entries)) return;
        for (const item of entries) {
            if (!Array.isArray(item) || item.length !== 2) continue;
            const [key, value] = item;
            if (typeof key !== 'string' || !key.startsWith('m:')) continue;
            this.store.put(key, value);
        }
    }
}

const jaccard = (a, b) => {
    const A = a instanceof Set ? a : new Set(Array.isArray(a) ? a : []);
    const B = b instanceof Set ? b : new Set(Array.isArray(b) ? b : []);
    if (A.size === 0 && B.size === 0) return 1;
    if (A.size === 0 || B.size === 0) return 0;
    let inter = 0;
    for (const x of A) if (B.has(x)) inter++;
    const uni = A.size + B.size - inter;
    return uni <= 0 ? 0 : inter / uni;
};

class DialogMemory {
    constructor(store, {
        maxItems = 2048,
        maxPerIndex = 64
    } = {}) {
        this.store = store;
        this.maxItems = Math.max(128, Number(maxItems) || 2048);
        this.maxPerIndex = Math.max(8, Number(maxPerIndex) || 64);
    }

    _kDialog(id) {
        return `d:${String(id)}`;
    }

    _kIndex(memeId) {
        return `i:${String(memeId)}`;
    }

    _makeId(signature) {
        return hashString(String(signature || ''));
    }

    remember({ signature, memes = [], question = '', reply = '', scoreHint = 0 } = {}) {
        const sig = String(signature || '').trim();
        const rep = String(reply || '').trim();
        if (!sig || !rep) return null;
        const id = this._makeId(sig);
        const key = this._kDialog(id);
        const prev = this.store.get(key);
        const next = {
            id,
            signature: sig,
            memes: Array.isArray(memes) ? memes.slice(0, 32) : [],
            question: String(question || '').slice(0, 800),
            reply: rep.slice(0, 1200),
            updatedAt: Date.now(),
            count: (prev && Number(prev.count)) ? (Number(prev.count) + 1) : 1,
            scoreHint: Number.isFinite(Number(scoreHint)) ? Number(scoreHint) : (prev?.scoreHint ?? 0)
        };
        this.store.put(key, next);

        // 建索引：memeId -> dialogIds[]
        const uniq = Array.from(new Set(next.memes));
        for (const memeId of uniq) {
            const ik = this._kIndex(memeId);
            const list = Array.isArray(this.store.get(ik)) ? this.store.get(ik) : [];
            const filtered = list.filter((x) => x && x !== id);
            filtered.unshift(id);
            this.store.put(ik, filtered.slice(0, this.maxPerIndex));
        }

        return next;
    }

    retrieve({ memes = [], signature = '', minSim = 0.45 } = {}) {
        const memeList = Array.isArray(memes) ? memes.slice(0, 24) : [];
        const sigSet = new Set(String(signature || '').split('|').filter(Boolean));
        const candidateIds = new Set();
        for (const memeId of memeList.slice(0, 8)) {
            const ik = this._kIndex(memeId);
            const ids = Array.isArray(this.store.get(ik)) ? this.store.get(ik) : [];
            for (const id of ids) {
                if (id) candidateIds.add(id);
            }
        }

        // 如果没有索引命中，尝试精确 signature 命中
        if (candidateIds.size === 0 && signature) {
            candidateIds.add(this._makeId(signature));
        }

        let best = null;
        let bestScore = 0;
        for (const id of candidateIds) {
            const rec = this.store.get(this._kDialog(id));
            if (!rec || !rec.reply) continue;
            const recSet = new Set(String(rec.signature || '').split('|').filter(Boolean));
            const sim = jaccard(sigSet, recSet);
            if (sim < minSim) continue;
            const freq = Math.log(1 + (Number(rec.count) || 0));
            const score = sim * (1 + 0.15 * freq);
            if (score > bestScore) {
                bestScore = score;
                best = { ...rec, similarity: sim, score };
            }
        }
        return best;
    }

    exportSnapshot({ limit = 512 } = {}) {
        const out = { dialogs: [], indexes: [] };
        const dialogs = this.store.entries('d:');
        for (const [key, value] of dialogs) {
            out.dialogs.push([key, value]);
            if (out.dialogs.length >= limit) break;
        }
        const indexes = this.store.entries('i:');
        for (const [key, value] of indexes) {
            out.indexes.push([key, value]);
            if (out.indexes.length >= limit) break;
        }
        return out;
    }

    importSnapshot(snapshot) {
        if (!snapshot || typeof snapshot !== 'object') return;
        for (const item of Array.isArray(snapshot.dialogs) ? snapshot.dialogs : []) {
            if (!Array.isArray(item) || item.length !== 2) continue;
            const [key, value] = item;
            if (typeof key === 'string' && key.startsWith('d:')) this.store.put(key, value);
        }
        for (const item of Array.isArray(snapshot.indexes) ? snapshot.indexes : []) {
            if (!Array.isArray(item) || item.length !== 2) continue;
            const [key, value] = item;
            if (typeof key === 'string' && key.startsWith('i:')) this.store.put(key, value);
        }
    }
}

class RobotsCorpus {
    constructor({
        dir,
        lemmaCsv,
        lemmaAutoload = false,
        lemmaMaxBytes,
        lemmaForce = false,
        chunkMinWords = 2,
        chunkMaxWords = 5
    } = {}) {
        this.dir = dir;
        this.lemmaCsv = lemmaCsv;
        this.lemmaAutoload = Boolean(lemmaAutoload);
        this.lemmaForce = Boolean(lemmaForce);
        this.lemmaMaxBytes = Number.isFinite(Number(lemmaMaxBytes)) && Number(lemmaMaxBytes) > 0
            ? Number(lemmaMaxBytes)
            : 64 * 1024 * 1024;
        this.chunkMinWords = Math.max(1, Number(chunkMinWords) || 2);
        this.chunkMaxWords = Math.max(this.chunkMinWords, Number(chunkMaxWords) || 5);
        this._lemmaLoaded = false;
        this.lemmaMap = new Map();
        this.maxArticleSize = 5_000_000;
        this.minParagraphLength = 12;
    }

    _chunkTokens(tokens) {
        const out = [];
        if (!Array.isArray(tokens) || tokens.length === 0) {
            return out;
        }
        const maxN = Math.max(1, this.chunkMaxWords);
        const minN = Math.max(1, this.chunkMinWords);
        for (let i = 0; i < tokens.length; i += maxN) {
            const chunk = tokens.slice(i, i + maxN);
            if (chunk.length >= minN) {
                out.push(chunk);
            }
        }
        return out;
    }

    _ensureLemmaMapLoaded() {
        if (this._lemmaLoaded) {
            return;
        }
        this._lemmaLoaded = true;
        if (!this.lemmaAutoload && !this.lemmaForce) {
            return;
        }
        this.lemmaMap = this._loadLemmaMap();
    }

    _loadLemmaMap() {
        const map = new Map();
        const csvParse = getCsvParse();
        if (!csvParse || !this.lemmaCsv || !fs.existsSync(this.lemmaCsv)) {
            return map;
        }
        try {
            try {
                const st = fs.statSync(this.lemmaCsv);
                if (!this.lemmaForce && st && Number.isFinite(st.size) && st.size > this.lemmaMaxBytes) {
                    console.warn(
                        `[RobotsCorpus] lemma.csv too large (${Math.round(st.size / 1024 / 1024)}MB), skip autoload. ` +
                        `Set AI_LEMMA_FORCE=true or increase AI_LEMMA_MAX_MB to load.`
                    );
                    return map;
                }
            } catch (_e) {
                // ignore stat failure
            }
            const csvContent = fs.readFileSync(this.lemmaCsv, 'utf8');
            const rows = csvParse.parse(csvContent, { skip_empty_lines: true, relax_column_count: true });
            for (const row of rows) {
                if (!Array.isArray(row) || row.length === 0) {
                    continue;
                }
                const base = String(row[0] || '').trim().toLowerCase();
                if (!base) {
                    continue;
                }
                map.set(base, base);
                for (const form of row) {
                    const token = String(form || '').trim().toLowerCase();
                    if (token) {
                        map.set(token, base);
                    }
                }
            }
        } catch (err) {
            console.warn('[RobotsCorpus] Failed to load lemma map:', err.message);
        }
        return map;
    }

    lemmatize(word) {
        this._ensureLemmaMapLoaded();
        const lower = String(word || '').toLowerCase();
        return this.lemmaMap.get(lower) || lower;
    }

    normalizeWords(words) {
        if (!Array.isArray(words)) {
            return [];
        }
        return words.map((w) => this.lemmatize(w)).filter(Boolean);
    }

    listFiles() {
        if (!this.dir || !fs.existsSync(this.dir)) {
            return [];
        }
        try {
            return fs
                .readdirSync(this.dir)
                .filter((name) => name.toLowerCase().endsWith('.txt'))
                .sort((a, b) => a.localeCompare(b));
        } catch (err) {
            console.warn('[RobotsCorpus] Failed to list files:', err.message);
            return [];
        }
    }

    _readFile(file) {
        const full = path.join(this.dir, file);
        try {
            // 关键优化：避免对超大语料文件做一次性 readFileSync（会把整个文件读进内存）
            // 这里仅读取前 maxArticleSize 字节（近似等价于之前的 slice 行为）。
            const fd = fs.openSync(full, 'r');
            try {
                const maxBytes = Math.max(1, Number(this.maxArticleSize) || 1);
                const buf = Buffer.allocUnsafe(maxBytes);
                const bytesRead = fs.readSync(fd, buf, 0, maxBytes, 0);
                return buf.toString('utf8', 0, Math.max(0, bytesRead || 0));
            } finally {
                try {
                    fs.closeSync(fd);
                } catch (_e) {
                    // ignore close failure
                }
            }
        } catch (err) {
            console.warn(`[RobotsCorpus] Failed to read ${full}:`, err.message);
            return '';
        }
    }

    _splitParagraphs(content) {
        return String(content || '')
            .split(/\r?\n\s*\r?\n/)
            .map((chunk) => chunk.trim())
            .filter(Boolean);
    }

    _shuffle(items) {
        for (let i = items.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [items[i], items[j]] = [items[j], items[i]];
        }
        return items;
    }

    collect({ limit, offset = 0, shuffle = false, files } = {}) {
        const docs = [];
        const maxDocs = Number.isFinite(Number(limit)) && Number(limit) > 0 ? Number(limit) : null;
        let skip = Number.isFinite(Number(offset)) && Number(offset) > 0 ? Number(offset) : 0;
        const selectedFiles = Array.isArray(files) && files.length
            ? this.listFiles().filter((name) => files.includes(name))
            : this.listFiles();

        for (const file of selectedFiles) {
            const content = this._readFile(file);
            if (!content) {
                continue;
            }
            const paragraphs = this._splitParagraphs(content);
            let localIndex = 0;
            for (const paragraph of paragraphs) {
                const trimmed = paragraph.trim();
                if (trimmed.length < this.minParagraphLength) {
                    continue;
                }

                // 新逻辑：按句子切分，再按 2-5 词（可配置）切块生成 doc
                // 这样每条 doc 更接近“短语/局部词共现”，利于词表/模因边的细粒度学习。
                // 注意：splitSentences() 已改为输出“2-10 词”的短片段；这里不再做二次切块。
                const units = splitSentences(trimmed);
                for (const unit of units) {
                    const unitText = String(unit || '').trim();
                    if (unitText.length < this.minParagraphLength) {
                        continue;
                    }
                    const normalizedTokens = this.normalizeWords(tokenize(unitText));
                    if (!normalizedTokens.length) {
                        continue;
                    }
                    if (skip > 0) {
                        skip -= 1;
                        continue;
                    }
                    docs.push({
                        id: `robots:${file}#${localIndex}`,
                        file,
                        source: `robots:${file}`,
                        index: localIndex,
                        text: unitText,
                        tokens: normalizedTokens
                    });
                    localIndex += 1;
                    if (maxDocs !== null && docs.length >= maxDocs) {
                        return shuffle && docs.length > 1 ? this._shuffle(docs) : docs;
                    }
                }
            }
        }

        if (shuffle && docs.length > 1) {
            this._shuffle(docs);
        }
        return docs;
    }
}

class KVMStore {
    constructor(store, { maxCacheEntries } = {}) {
        this.store = store;
        this.cache = new Map();
        this.maxCacheEntries = Number.isFinite(Number(maxCacheEntries)) && Number(maxCacheEntries) >= 0
            ? Number(maxCacheEntries)
            : 50_000;
    }

    _cacheGet(key) {
        if (!this.maxCacheEntries) {
            return null;
        }
        if (!this.cache.has(key)) {
            return null;
        }
        const value = this.cache.get(key);
        // LRU: bump to most-recent
        this.cache.delete(key);
        this.cache.set(key, value);
        return value;
    }

    _cacheSet(key, value) {
        if (!this.maxCacheEntries) {
            return;
        }
        if (this.cache.has(key)) {
            this.cache.delete(key);
        }
        this.cache.set(key, value);
        while (this.cache.size > this.maxCacheEntries) {
            const oldest = this.cache.keys().next().value;
            if (oldest === undefined) break;
            this.cache.delete(oldest);
        }
    }

    _key(type, value) {
        return `${type}:${value}`;
    }

    getWordMemeSet(word) {
        const key = this._key('word', word);
        const cached = this._cacheGet(key);
        if (cached) return cached;
        const value = this.store.get(key) || [];
        const set = new Set(value);
        this._cacheSet(key, set);
        return set;
    }

    getMemeWords(memeId) {
        const key = this._key('meme', memeId);
        const cached = this._cacheGet(key);
        if (cached) return cached;
        const value = this.store.get(key) || [];
        const set = new Set(value);
        this._cacheSet(key, set);
        return set;
    }

    link(word, memeId) {
        const wordKey = this._key('word', word);
        const memeKey = this._key('meme', memeId);
        const wordSet = this.getWordMemeSet(word);
        const memeSet = this.getMemeWords(memeId);
        if (!wordSet.has(memeId)) {
            wordSet.add(memeId);
            this.store.put(wordKey, Array.from(wordSet));
            this._cacheSet(wordKey, wordSet);
        }
        if (!memeSet.has(word)) {
            memeSet.add(word);
            this.store.put(memeKey, Array.from(memeSet));
            this._cacheSet(memeKey, memeSet);
        }
    }

    unlink(word, memeId) {
        const wordKey = this._key('word', word);
        const memeKey = this._key('meme', memeId);
        const wordSet = this.getWordMemeSet(word);
        const memeSet = this.getMemeWords(memeId);
        if (wordSet.delete(memeId)) {
            this.store.put(wordKey, Array.from(wordSet));
        }
        if (memeSet.delete(word)) {
            this.store.put(memeKey, Array.from(memeSet));
        }
    }

    exportEntries() {
        return {
            words: Object.fromEntries([...this.cache.entries()].filter(([key]) => key.startsWith('word:')).map(([key, value]) => [key.slice(5), Array.from(value)])),
            memes: Object.fromEntries([...this.cache.entries()].filter(([key]) => key.startsWith('meme:')).map(([key, value]) => [key.slice(5), Array.from(value)]))
        };
    }
}

class CSRMatrix {
    constructor({ rowPtr, colIdx, values, nRows, nCols }) {
        this.rowPtr = rowPtr;
        this.colIdx = colIdx;
        this.values = values;
        this.nRows = nRows;
        this.nCols = nCols;
    }
}

class MemeGraph {
    constructor(store, { eagerLoad = false } = {}) {
        this.store = store;
        this.nodes = new Map();
        this.meta = new Map();
        this.windowSize = 4096;
        this._fullyLoaded = false;
        if (eagerLoad) {
            this._loadAllFromStore();
        }
    }

    _loadAllFromStore() {
        if (this._fullyLoaded) {
            return;
        }
        const entries = this.store.entries('node:');
        for (const [key, value] of entries) {
            const memeId = key.slice(5);
            if (!this.meta.has(memeId)) {
                this.meta.set(memeId, value || {});
            }
        }
        const rowEntries = this.store.entries('row:');
        for (const [key, row] of rowEntries) {
            const memeId = key.slice(4);
            if (this.nodes.has(memeId)) {
                continue;
            }
            if (!row || !Array.isArray(row.neighbors)) {
                continue;
            }
            const map = new Map();
            for (const { to, weight, direction } of row.neighbors) {
                map.set(String(to), { weight, direction });
            }
            this.nodes.set(memeId, map);
        }
        this._fullyLoaded = true;
    }

    _ensureRowLoaded(memeId) {
        const id = String(memeId);
        if (this.nodes.has(id)) {
            return this.nodes.get(id);
        }
        const row = this.store.get(`row:${id}`);
        if (row && Array.isArray(row.neighbors)) {
            const map = new Map();
            for (const { to, weight, direction } of row.neighbors) {
                map.set(String(to), { weight, direction });
            }
            this.nodes.set(id, map);
            return map;
        }
        return null;
    }

    _ensureMetaLoaded(memeId) {
        const id = String(memeId);
        if (this.meta.has(id)) {
            return this.meta.get(id);
        }
        const meta = this.store.get(`node:${id}`);
        if (meta && typeof meta === 'object') {
            this.meta.set(id, meta);
            return meta;
        }
        return null;
    }

    // 返回当前图中所有模因节点的ID列表，供扫描器等模块使用
    getAllPoints() {
        // 需要全量枚举时才进行全量加载，避免启动时扫描整个 store。
        this._loadAllFromStore();
        return Array.from(this.meta.keys());
    }

    _persistNode(memeId) {
        const neighbors = this.nodes.get(memeId) || new Map();
        const payload = {
            neighbors: Array.from(neighbors.entries()).map(([to, { weight, direction }]) => ({ to, weight, direction }))
        };
        this.store.put(`row:${memeId}`, payload);
    }

    ensureNode(memeId) {
        const id = String(memeId);
        if (!this.nodes.has(id)) {
            const loaded = this._ensureRowLoaded(id);
            if (!loaded) {
                this.nodes.set(id, new Map());
            }
        }
        if (!this.meta.has(id)) {
            const loadedMeta = this._ensureMetaLoaded(id);
            if (!loadedMeta) {
                this.meta.set(id, { degree: 0, lastTouched: Date.now() });
                this.store.put(`node:${id}`, this.meta.get(id));
            }
        }
        if (!this.nodes.get(id)) {
            this.nodes.set(id, new Map());
        }
        // 若节点此前不存在于 store，确保 row 持久化
        if (!this.store.get(`row:${id}`)) {
            this._persistNode(id);
        }
    }

    link(fromId, toId, weight = 1, direction = 0) {
        if (fromId === toId) {
            return;
        }
        this.ensureNode(fromId);
        this.ensureNode(toId);
        const table = this.nodes.get(fromId);
        const prev = table.get(toId)?.weight ?? 0;
        table.set(toId, { weight: prev + weight, direction });
        this.meta.get(fromId).degree = table.size;
        this.meta.get(fromId).lastTouched = Date.now();
        this._persistNode(fromId);
        if (direction === 0) {
            const back = this.nodes.get(toId);
            const prevBack = back.get(fromId)?.weight ?? 0;
            back.set(fromId, { weight: prevBack + weight, direction });
            this.meta.get(toId).degree = back.size;
            this.meta.get(toId).lastTouched = Date.now();
            this._persistNode(toId);
        }
    }

    decayEdge(fromId, toId, factor) {
        const table = this.nodes.get(fromId);
        if (!table) {
            return;
        }
        const entry = table.get(toId);
        if (!entry) {
            return;
        }
        entry.weight *= factor;
        if (entry.weight < 1e-5) {
            table.delete(toId);
        } else {
            table.set(toId, entry);
        }
        this.meta.get(fromId).degree = table.size;
        this.meta.get(fromId).lastTouched = Date.now();
        this._persistNode(fromId);
    }

    buildWindow(seedIds, radius = 1) {
        const visited = new Set(seedIds);
        const border = [...seedIds];
        let depth = 0;
        while (border.length && visited.size < this.windowSize && depth < radius) {
            const next = [];
            for (const id of border) {
                const table = this.nodes.get(id) || this._ensureRowLoaded(id);
                if (!table) {
                    continue;
                }
                for (const neighbor of table.keys()) {
                    if (!visited.has(neighbor)) {
                        visited.add(neighbor);
                        next.push(neighbor);
                    }
                }
            }
            border.splice(0, border.length, ...next);
            depth += 1;
        }
        const ids = Array.from(visited);
        const index = new Map(ids.map((id, idx) => [id, idx]));
        const rowPtr = new Uint32Array(ids.length + 1);
        const edges = [];
        const weights = [];
        for (let i = 0; i < ids.length; i++) {
            const id = ids[i];
            const table = this.nodes.get(id) || this._ensureRowLoaded(id) || new Map();
            rowPtr[i] = edges.length;
            for (const [toId, { weight }] of table.entries()) {
                if (!index.has(toId)) {
                    continue;
                }
                edges.push(index.get(toId));
                weights.push(weight);
            }
        }
        rowPtr[ids.length] = edges.length;
        const csr = new CSRMatrix({
            rowPtr,
            colIdx: Uint32Array.from(edges),
            values: Float32Array.from(weights),
            nRows: ids.length,
            nCols: ids.length
        });
        return { csr, index, ids };
    }

    exportSnapshot() {
        // 导出需要全量内容；若尚未加载则在此时执行全量加载。
        this._loadAllFromStore();
        const nodes = [];
        for (const [id, table] of this.nodes.entries()) {
            nodes.push({
                id,
                neighbors: Array.from(table.entries()).map(([to, { weight, direction }]) => ({ to, weight, direction }))
            });
        }
        return { nodes, meta: Object.fromEntries(this.meta) };
    }

    importSnapshot(snapshot) {
        this.nodes.clear();
        this.meta.clear();
        for (const item of snapshot.nodes || []) {
            const map = new Map();
            for (const { to, weight, direction } of item.neighbors || []) {
                map.set(String(to), { weight, direction });
            }
            this.nodes.set(String(item.id), map);
            this._persistNode(String(item.id));
        }
        for (const [key, value] of Object.entries(snapshot.meta || {})) {
            this.meta.set(key, value);
            this.store.put(`node:${key}`, value);
        }
    }

    removeNode(memeId) {
        // 删除需要一致性：先全量加载再删，避免遗漏未加载节点中的反向边。
        this._loadAllFromStore();
        if (!this.nodes.has(memeId)) {
            return false;
        }
        this.nodes.delete(memeId);
        this.store.delete(`row:${memeId}`);
        this.meta.delete(memeId);
        this.store.delete(`node:${memeId}`);
        for (const [id, table] of this.nodes.entries()) {
            if (table.delete(memeId)) {
                const meta = this.meta.get(id) || {};
                meta.degree = table.size;
                meta.lastTouched = Date.now();
                this.meta.set(id, meta);
                this._persistNode(id);
            }
        }
        return true;
    }
}

class TensorEngine {
    spmm(csr, x, out = null) {
        const { rowPtr, colIdx, values, nRows } = csr;
        const result = out ?? new Float32Array(nRows);
        for (let row = 0; row < nRows; row++) {
            let acc = 0;
            const start = rowPtr[row];
            const end = rowPtr[row + 1];
            for (let idx = start; idx < end; idx++) {
                const w = values[idx];
                const j = colIdx[idx];
                const xv = (j >= 0 && j < x.length) ? x[j] : 0;
                if (!Number.isFinite(w) || !Number.isFinite(xv)) {
                    // 忽略非有限值，避免传播 NaN
                    continue;
                }
                acc += w * xv;
            }
            result[row] = Number.isFinite(acc) ? acc : 0;
        }
        return result;
    }

    axpby(a, x, b, y, out = null) {
        const n = x.length;
        const result = out ?? new Float32Array(n);
        for (let i = 0; i < n; i++) {
            result[i] = a * x[i] + b * y[i];
        }
        return result;
    }

    l2NormalizeRows(mat, nRows, nCols) {
        for (let row = 0; row < nRows; row++) {
            let norm = 0;
            for (let col = 0; col < nCols; col++) {
                const idx = row * nCols + col;
                norm += mat[idx] * mat[idx];
            }
            if (norm > 0) {
                const scale = 1 / Math.sqrt(norm);
                for (let col = 0; col < nCols; col++) {
                    const idx = row * nCols + col;
                    mat[idx] *= scale;
                }
            }
        }
    }

    dot(a, b) {
        let acc = 0;
        for (let i = 0; i < a.length; i++) {
            acc += a[i] * b[i];
        }
        return acc;
    }

    iteratePropagation(csr, seeds, steps, actFn, decayK, damp = 0.02) {
        let state = Float32Array.from(seeds);
        const next = new Float32Array(seeds.length);
        const safeAct = (x) => {
            let y;
            try {
                y = actFn(x);
            } catch (_) {
                y = 0;
            }
            return Number.isFinite(y) ? y : 0;
        };
        for (let s = 0; s < steps; s++) {
            this.spmm(csr, state, next);
            for (let i = 0; i < next.length; i++) {
                const si = Number.isFinite(state[i]) ? state[i] : 0;
                const ni = Number.isFinite(next[i]) ? next[i] : 0;
                const raw = si + ni - (Number.isFinite(decayK) ? decayK : 0) * si * (Number.isFinite(damp) ? damp : 0.02);
                state[i] = safeAct(raw);
            }
        }
        // 最终保障结果为有限值
        for (let i = 0; i < state.length; i++) {
            if (!Number.isFinite(state[i])) {
                state[i] = 0;
            }
        }
        return state;
    }
}

class GraphTensorBridge {
    constructor(runtime) {
        this.runtime = runtime;
        this.memeIndex = new Map();
        this.embeddings = null;
        this.multi = null;
        this.D = 512;
    }

    static fnv1a32(str) {
        let hash = 0x811c9dc5;
        for (let i = 0; i < str.length; i++) {
            hash ^= str.charCodeAt(i);
            hash = (hash * 0x01000193) >>> 0;
        }
        return hash >>> 0;
    }

    rebuildRowIndex(ids) {
        this.memeIndex = new Map(ids.map((id, idx) => [id, idx]));
    }

    buildEmbeddings(ids, D = 512) {
        this.rebuildRowIndex(ids);
        const mat = new Float32Array(ids.length * D);
        for (let i = 0; i < ids.length; i++) {
            const memeId = ids[i];
            const words = this.runtime.kvm.getMemeWords(memeId);
            if (!words || words.size === 0) {
                continue;
            }
            for (const word of words) {
                const idx = GraphTensorBridge.fnv1a32(word) % D;
                mat[i * D + idx] += 1;
            }
        }
        this.runtime.tensor.l2NormalizeRows(mat, ids.length, D);
        this.embeddings = { data: mat, nRows: ids.length, nCols: D };
        this.D = D;
        return this.embeddings;
    }

    buildMultiOrderCSR(graphWindow) {
        const { csr, ids } = graphWindow;
        const rowPtr = csr.rowPtr;
        const colIdx = csr.colIdx;
        const values = csr.values;
        const biRow = new Uint32Array(rowPtr.length);
        const biCol = [];
        const biVal = [];
        const outRow = rowPtr;
        const inRow = new Uint32Array(rowPtr.length);
        const inCol = [];
        const inVal = [];
        const adjacency = new Map();
        for (let row = 0; row < ids.length; row++) {
            const start = rowPtr[row];
            const end = rowPtr[row + 1];
            for (let idx = start; idx < end; idx++) {
                const col = colIdx[idx];
                const weight = values[idx];
                if (!adjacency.has(row)) {
                    adjacency.set(row, new Map());
                }
                adjacency.get(row).set(col, weight);
            }
        }
        const rowCount = ids.length;
        let biCounter = 0;
        for (let row = 0; row < rowCount; row++) {
            biRow[row] = biCounter;
            const neighbors = adjacency.get(row) || new Map();
            for (const [col, weight] of neighbors.entries()) {
                const back = adjacency.get(col)?.get(row);
                if (back) {
                    biCol.push(col);
                    biVal.push((weight + back) * 0.5);
                    biCounter++;
                }
                inCol.push(row);
                inVal.push(weight);
            }
        }
        biRow[rowCount] = biCounter;
        inRow[rowCount] = inCol.length;
        const all = csr;
        const bi = new CSRMatrix({ rowPtr: biRow, colIdx: Uint32Array.from(biCol), values: Float32Array.from(biVal), nRows: rowCount, nCols: rowCount });
        const inbound = new CSRMatrix({ rowPtr: inRow, colIdx: Uint32Array.from(inCol), values: Float32Array.from(inVal), nRows: rowCount, nCols: rowCount });
        this.multi = { all, bi, out: all, in: inbound, ids };
        return this.multi;
    }
}

class DimReducer {
    pca2D(emb) {
        const { data, nRows, nCols } = emb;
        const mean = new Float32Array(nCols);
        for (let row = 0; row < nRows; row++) {
            for (let col = 0; col < nCols; col++) {
                mean[col] += data[row * nCols + col];
            }
        }
        for (let col = 0; col < nCols; col++) {
            mean[col] /= nRows || 1;
        }
        const centered = new Float32Array(data.length);
        for (let row = 0; row < nRows; row++) {
            for (let col = 0; col < nCols; col++) {
                const idx = row * nCols + col;
                centered[idx] = data[idx] - mean[col];
            }
        }
        const cov = new Array(nCols).fill(0).map(() => new Float32Array(nCols));
        for (let row = 0; row < nRows; row++) {
            for (let i = 0; i < nCols; i++) {
                for (let j = i; j < nCols; j++) {
                    const v = centered[row * nCols + i] * centered[row * nCols + j];
                    cov[i][j] += v;
                    if (i !== j) {
                        cov[j][i] += v;
                    }
                }
            }
        }
        const powerIter = (vec) => {
            let next = new Float32Array(nCols);
            for (let i = 0; i < nCols; i++) {
                let acc = 0;
                for (let j = 0; j < nCols; j++) {
                    acc += cov[i][j] * vec[j];
                }
                next[i] = acc;
            }
            const norm = Math.sqrt(next.reduce((sum, val) => sum + val * val, 0)) || 1;
            for (let i = 0; i < nCols; i++) {
                next[i] /= norm;
            }
            return next;
        };
        let v1 = Float32Array.from({ length: nCols }, () => Math.random() - 0.5);
        for (let i = 0; i < 8; i++) {
            v1 = powerIter(v1);
        }
        let v2 = Float32Array.from({ length: nCols }, () => Math.random() - 0.5);
        for (let i = 0; i < 8; i++) {
            const proj = v2.reduce((acc, val, idx) => acc + val * v1[idx], 0);
            for (let j = 0; j < nCols; j++) {
                v2[j] -= proj * v1[j];
            }
            v2 = powerIter(v2);
        }
        const coords = new Float32Array(nRows * 2);
        for (let row = 0; row < nRows; row++) {
            let x = 0;
            let y = 0;
            for (let col = 0; col < nCols; col++) {
                const v = centered[row * nCols + col];
                x += v * v1[col];
                y += v * v2[col];
            }
            coords[row * 2] = x;
            coords[row * 2 + 1] = y;
        }
        return { coords, basis: [v1, v2] };
    }

    project2D(emb, method = 'auto') {
        const umap = getUmap();
        if (method === 'umap' && umap) {
            const dataset = [];
            for (let row = 0; row < emb.nRows; row++) {
                const sample = [];
                for (let col = 0; col < emb.nCols; col++) {
                    sample.push(emb.data[row * emb.nCols + col]);
                }
                dataset.push(sample);
            }
            const umapModel = new umap.UMAP();
            const coords = umapModel.fit(dataset);
            return { coords, method: 'umap' };
        }
        const { coords } = this.pca2D(emb);
        const arr = [];
        for (let row = 0; row < emb.nRows; row++) {
            arr.push([coords[row * 2], coords[row * 2 + 1]]);
        }
        return { coords: arr, method: 'pca' };
    }
}

class PatternMatrix {
    constructor(runtime) {
        this.runtime = runtime;
        this.bridge = new GraphTensorBridge(runtime);
        this.reducer = new DimReducer();
        this.state = null;
    }

    rebuild(graphWindow) {
        const { ids } = graphWindow;
        const emb = this.bridge.buildEmbeddings(ids, 512);
        const multi = this.bridge.buildMultiOrderCSR(graphWindow);
        const projection = this.reducer.project2D(emb, 'auto');
        this.state = {
            ids,
            embeddings: emb,
            multi,
            projection
        };
        return this.state;
    }
}

class OnlineResearcher {
    constructor(runtime, { endpoint = '', cooldownMs = 5 * 60 * 1000, cacheSize = 128 } = {}) {
        this.runtime = runtime;
        this.endpoint = endpoint;
        this.cooldownMs = cooldownMs;
        this.cacheSize = cacheSize;
        this.cache = new Map();
        this.enabled = true;
    }

    _normalize(words) {
        return words.slice(0, 32).join(' ').toLowerCase();
    }

    _pruneCache() {
        if (this.cache.size <= this.cacheSize) {
            return;
        }
        const entries = Array.from(this.cache.entries()).sort((a, b) => a[1].ts - b[1].ts);
        while (entries.length > this.cacheSize) {
            const [key] = entries.shift();
            this.cache.delete(key);
        }
    }

    _remember(key, payload) {
        this.cache.set(key, { ts: Date.now(), payload });
        this._pruneCache();
    }

    _fromCache(key) {
        const hit = this.cache.get(key);
        if (!hit) {
            return null;
        }
        if (Date.now() - hit.ts > this.cooldownMs) {
            this.cache.delete(key);
            return null;
        }
        return hit.payload;
    }

    async lookup(input, options = {}) {
        if (this.enabled === false || options.disableRemote === true) {
            return this._fallback(Array.isArray(input) ? input : tokenize(String(input || '')));
        }
        // 如果输入里包含 URL（或显式要求 crawl），优先走站内递归抓取
        const rawText = Array.isArray(input) ? input.join(' ') : String(input || '');
        const crawlReq = options?.crawl || null;
        const urlFromInput = extractFirstUrl(rawText);
        const shouldCrawl = options?.mode === 'crawl' || Boolean(crawlReq?.startUrl) || Boolean(urlFromInput);
        if (shouldCrawl) {
            const startUrl = String(crawlReq?.startUrl || urlFromInput || '').trim();
            if (!startUrl) {
                return { ok: false, source: 'crawl', reason: 'startUrl-required' };
            }
            const crawlOptions = {
                maxPages: clampInt(crawlReq?.maxPages, 1, 500, 60),
                maxDepth: clampInt(crawlReq?.maxDepth, 0, 10, 3),
                includePdf: crawlReq?.includePdf !== false,
                sameSite: crawlReq?.sameSite !== false,
                timeoutMs: clampInt(crawlReq?.timeoutMs, 1000, 60000, 12000),
                maxBytesPerPage: clampInt(crawlReq?.maxBytesPerPage, 8 * 1024, 10 * 1024 * 1024, 2 * 1024 * 1024),
                maxPdfBytes: clampInt(crawlReq?.maxPdfBytes, 64 * 1024, 40 * 1024 * 1024, 20 * 1024 * 1024),
                userAgent: typeof crawlReq?.userAgent === 'string' && crawlReq.userAgent.trim() ? crawlReq.userAgent.trim() : '079ProjectCrawler/1.0'
            };
            const crawler = new SiteCrawler({ axios: getAxios(), cheerio: getCheerio(), pdfParse: getPdfParse() });
            const result = await crawler.crawl(startUrl, crawlOptions);
            if (!options?.forceRemote) {
                const key = this._normalize(tokenize(`crawl ${startUrl}`));
                this._remember(key, result);
            }
            return result;
        }

        const words = Array.isArray(input) ? input : tokenize(rawText);
        if (words.length === 0) {
            return { ok: false, source: 'local', reason: 'empty-query' };
        }
        const key = this._normalize(words);
        if (!options.forceRemote) {
            const cached = this._fromCache(key);
            if (cached) {
                return { ...cached, cached: true };
            }
        }

        let payload = null;
        const axios = getAxios();
        if (axios && this.endpoint && !options.skipRemote) {
            try {
                const resp = await axios.get(this.endpoint, {
                    params: { q: words.join(' ') },
                    timeout: options.timeout ?? 5000
                });
                const data = resp.data;
                const snippet = typeof data === 'string' ? data.slice(0, 280) : JSON.stringify(data).slice(0, 280);
                payload = {
                    ok: true,
                    source: 'remote',
                    query: words.join(' '),
                    snippet,
                    raw: options.includeRaw ? data : undefined
                };
            } catch (err) {
                payload = {
                    ok: false,
                    source: 'remote',
                    error: err.message || 'remote-request-failed'
                };
            }
        }

        if (!payload || payload.ok === false) {
            payload = this._fallback(words);
        }

        this._remember(key, payload);
        return payload;
    }

    _fallback(words) {
        const seeds = this.runtime.mapWordsToMemes(words);
        if (seeds.size === 0) {
            return {
                ok: false,
                source: 'local',
                reason: 'no-memes'
            };
        }
        const result = this.runtime.runPropagation(seeds);
        const candidates = [];
        for (let i = 0; i < Math.min(10, result.windowInfo.ids.length); i++) {
            const memeId = result.windowInfo.ids[i];
            const strength = result.activation[i];
            const wordsForMeme = Array.from(this.runtime.kvm.getMemeWords(memeId) || []);
            candidates.push({ memeId, strength, words: wordsForMeme.slice(0, 6) });
        }
        return {
            ok: true,
            source: 'local',
            query: words.join(' '),
            suggestions: candidates
        };
    }
}

const clampInt = (value, min, max, fallback) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(min, Math.min(max, Math.trunc(n)));
};

const extractFirstUrl = (text) => {
    const s = String(text || '').trim();
    if (!s) return null;
    // 优先匹配 http(s)://
    const m = s.match(/https?:\/\/[\w\-._~:/?#[\]@!$&'()*+,;=%]+/i);
    if (m && m[0]) return m[0];
    // 兼容裸域名：www.example.com/path
    const m2 = s.match(/\b(?:www\.)[a-z0-9\-]+(?:\.[a-z0-9\-]+)+(?:\/[\w\-._~:/?#[\]@!$&'()*+,;=%]*)?/i);
    if (m2 && m2[0]) return `https://${m2[0]}`;
    return null;
};

class SiteCrawler {
    constructor({ axios, cheerio, pdfParse } = {}) {
        this.axios = axios;
        this.cheerio = cheerio;
        this.pdfParse = pdfParse;
    }

    _normalizeUrl(url) {
        try {
            const u = new URL(url);
            u.hash = '';
            // 轻度去重：丢弃常见追踪参数
            for (const key of Array.from(u.searchParams.keys())) {
                if (/^(utm_|fbclid$|gclid$)/i.test(key)) u.searchParams.delete(key);
            }
            return u.toString();
        } catch {
            return null;
        }
    }

    _sameSite(baseUrl, targetUrl) {
        try {
            const base = new URL(baseUrl);
            const t = new URL(targetUrl);
            const baseHost = base.hostname.replace(/^www\./i, '');
            const tHost = t.hostname.replace(/^www\./i, '');
            return base.protocol === t.protocol && baseHost === tHost;
        } catch {
            return false;
        }
    }

    _extractLinksHtml(html, baseUrl) {
        const out = [];
        if (!html) return out;
        const base = String(baseUrl || '');
        if (this.cheerio) {
            try {
                const $ = this.cheerio.load(html);
                const push = (href) => {
                    if (!href) return;
                    const h = String(href).trim();
                    if (!h || h.startsWith('javascript:') || h.startsWith('mailto:') || h.startsWith('#')) return;
                    try {
                        const u = new URL(h, base);
                        out.push(u.toString());
                    } catch {}
                };
                $('a[href]').each((_, el) => push($(el).attr('href')));
                $('link[href]').each((_, el) => push($(el).attr('href')));
                $('iframe[src]').each((_, el) => push($(el).attr('src')));
                return out;
            } catch {
                // fallthrough
            }
        }
        // 简单正则 fallback
        const re = /\b(?:href|src)\s*=\s*['"]([^'"#]+)['"]/gi;
        let m;
        while ((m = re.exec(html))) {
            const href = m[1];
            if (!href) continue;
            if (/^(javascript:|mailto:)/i.test(href)) continue;
            try {
                const u = new URL(href, base);
                out.push(u.toString());
            } catch {}
        }
        return out;
    }

    _extractTextHtml(html) {
        if (!html) return '';
        if (this.cheerio) {
            try {
                const $ = this.cheerio.load(html);
                $('script,noscript,style').remove();
                const text = $('body').text() || $.root().text() || '';
                return text.replace(/\s+/g, ' ').trim();
            } catch {
                // fallthrough
            }
        }
        return String(html).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    }

    async _fetch(url, { timeoutMs, userAgent, responseType, maxBytes } = {}) {
        if (!this.axios) {
            throw new Error('axios-not-installed');
        }
        const resp = await this.axios.get(url, {
            timeout: timeoutMs ?? 12000,
            responseType: responseType || 'arraybuffer',
            maxContentLength: maxBytes,
            maxBodyLength: maxBytes,
            headers: {
                'User-Agent': userAgent || '079ProjectCrawler/1.0',
                'Accept': 'text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8'
            },
            validateStatus: (s) => s >= 200 && s < 400
        });
        const ctype = String(resp.headers?.['content-type'] || '').toLowerCase();
        const buf = Buffer.isBuffer(resp.data) ? resp.data : Buffer.from(resp.data);
        return { status: resp.status, contentType: ctype, bytes: buf.length, data: buf };
    }

    async _parsePdf(buffer) {
        if (!buffer || !Buffer.isBuffer(buffer)) return { text: '' };
        if (!this.pdfParse) {
            return { text: '', warning: 'pdf-parse-not-installed' };
        }
        const r = await this.pdfParse(buffer);
        const text = String(r?.text || '').replace(/\s+/g, ' ').trim();
        return { text, pages: r?.numpages };
    }

    async crawl(startUrl, options = {}) {
        const normalizedStart = this._normalizeUrl(startUrl);
        if (!normalizedStart) {
            return { ok: false, source: 'crawl', reason: 'invalid-url' };
        }
        const {
            maxPages = 60,
            maxDepth = 3,
            includePdf = true,
            sameSite = true,
            timeoutMs = 12000,
            maxBytesPerPage = 2 * 1024 * 1024,
            maxPdfBytes = 20 * 1024 * 1024,
            userAgent = '079ProjectCrawler/1.0'
        } = options;

        const visited = new Set();
        const queue = [{ url: normalizedStart, depth: 0 }];
        const pages = [];
        const errors = [];

        while (queue.length && pages.length < maxPages) {
            const task = queue.shift();
            const url = this._normalizeUrl(task.url);
            if (!url) continue;
            if (visited.has(url)) continue;
            if (sameSite && !this._sameSite(normalizedStart, url)) continue;
            visited.add(url);

            try {
                const isPdfByExt = /\.pdf(?:$|\?)/i.test(url);
                const maxBytes = isPdfByExt ? maxPdfBytes : maxBytesPerPage;
                const fetched = await this._fetch(url, { timeoutMs, userAgent, maxBytes });
                const ctype = fetched.contentType;
                const isPdf = isPdfByExt || ctype.includes('application/pdf');

                if (isPdf) {
                    if (!includePdf) {
                        pages.push({ url, kind: 'pdf', skipped: true, reason: 'includePdf=false' });
                        continue;
                    }
                    const parsed = await this._parsePdf(fetched.data);
                    pages.push({
                        url,
                        kind: 'pdf',
                        bytes: fetched.bytes,
                        contentType: ctype || 'application/pdf',
                        pages: parsed.pages,
                        text: (parsed.text || '').slice(0, 20000)
                    });
                    continue;
                }

                const html = fetched.data.toString('utf8');
                const text = this._extractTextHtml(html);
                const links = this._extractLinksHtml(html, url);
                pages.push({
                    url,
                    kind: 'html',
                    bytes: fetched.bytes,
                    contentType: ctype || 'text/html',
                    linksFound: links.length,
                    text: text.slice(0, 20000)
                });

                if (task.depth < maxDepth) {
                    for (const link of links) {
                        const n = this._normalizeUrl(link);
                        if (!n) continue;
                        if (visited.has(n)) continue;
                        if (sameSite && !this._sameSite(normalizedStart, n)) continue;
                        queue.push({ url: n, depth: task.depth + 1 });
                    }
                }
            } catch (e) {
                errors.push({ url: task.url, error: e?.message || String(e) });
            }
        }

        const chunks = [];
        for (const p of pages) {
            if (!p?.text) continue;
            chunks.push(`URL: ${p.url}\n${p.text}`);
            if (chunks.join('\n\n').length > 60000) break;
        }
        const aggregated = chunks.join('\n\n');

        return {
            ok: true,
            source: 'crawl',
            startUrl: normalizedStart,
            stats: { visited: visited.size, returned: pages.length, queued: queue.length, errors: errors.length },
            pages,
            text: aggregated,
            errors: errors.slice(0, 20)
        };
    }
}

class SessionManager {
    constructor(store, { idleMs = 10 * 60 * 1000, maxSessions = 200 } = {}) {
        this.store = store;
        this.idleMs = idleMs;
        this.maxSessions = maxSessions;
        this.active = new Map();
    }

    _save(sessionId) {
        const data = this.active.get(sessionId);
        if (data) {
            this.store.put(`session:${sessionId}`, data);
        }
    }

    _newId() {
        return crypto.randomBytes(8).toString('hex');
    }

    ensure(sessionId) {
        if (sessionId) {
            const sid = String(sessionId);
            if (this.active.has(sid)) {
                const data = this.active.get(sid);
                data.lastActivity = Date.now();
                data.count = (data.count || 0) + 1;
                this._save(sid);
                return sid;
            }
            // 懒加载：仅在客户端携带 sessionId 时才从 store 读取
            const stored = this.store.get(`session:${sid}`);
            if (stored && typeof stored === 'object') {
                const data = { ...stored, id: stored.id || sid };
                data.lastActivity = Date.now();
                data.count = (data.count || 0) + 1;
                this.active.set(sid, data);
                this._save(sid);
                if (this.active.size > this.maxSessions) {
                    this._truncate();
                }
                return sid;
            }
        }
        const id = this._newId();
        this.active.set(id, { id, createdAt: Date.now(), lastActivity: Date.now(), count: 1, meta: {} });
        this._save(id);
        if (this.active.size > this.maxSessions) {
            this._truncate();
        }
        return id;
    }

    _truncate() {
        const sorted = Array.from(this.active.values()).sort((a, b) => b.lastActivity - a.lastActivity);
        const keep = sorted.slice(0, this.maxSessions);
        const keepSet = new Set(keep.map((item) => item.id));
        for (const id of Array.from(this.active.keys())) {
            if (!keepSet.has(id)) {
                this.active.delete(id);
                this.store.delete(`session:${id}`);
            }
        }
    }

    export() {
        return Array.from(this.active.values());
    }

    import(list) {
        this.active.clear();
        for (const item of list || []) {
            this.active.set(item.id, item);
            this.store.put(`session:${item.id}`, item);
        }
    }
}

class SnapshotManager {
    constructor(runtime, dir) {
        this.runtime = runtime;
        this.dir = dir;
        ensureDir(dir);
    }

    list() {
        return fs.readdirSync(this.dir).filter((file) => file.endsWith('.json')); 
    }

    async create(name = 'auto') {
        const snapshot = this.runtime.toSnapshot();
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const file = path.join(this.dir, `${timestamp}_${name}.json`);
        fs.writeFileSync(file, JSON.stringify(snapshot));
        return file;
    }

    async restore(id) {
        const file = path.join(this.dir, id);
        if (!fs.existsSync(file)) {
            throw new Error(`Snapshot ${id} not found`);
        }
        const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
        await this.runtime.fromSnapshot(raw);
        return raw;
    }

    delete(id) {
        const file = path.join(this.dir, id);
        if (fs.existsSync(file)) {
            fs.unlinkSync(file);
        }
    }
}

// 图导出构建器：将线性代数结构（CSR、窗口、嵌入）反序列化为 Go 可消费的图结构
class GraphExportBuilder {
    static fromWindow(windowInfo, activation = null) {
        const n = windowInfo.ids.length;
        const coords = [];
        // 简易坐标：若有 activation 则用前两主分量近似，否则用索引散列
        for (let i = 0; i < n; i++) {
            const id = windowInfo.ids[i];
            const ax = activation && Number.isFinite(activation[i]) ? activation[i] : 0;
            const ay = ((i * 9973) % 101) / 100;
            coords.push([ax, ay, 0]);
        }
        const nodes = [];
        for (let i = 0; i < n; i++) {
            nodes.push({
                id: String(windowInfo.ids[i]),
                x: coords[i][0],
                y: coords[i][1],
                z: coords[i][2],
                value: Number.isFinite(activation?.[i]) ? activation[i] : 0,
                attrs: {}
            });
        }
        const edges = [];
        const { rowPtr, colIdx, values, nRows } = windowInfo.csr;
        for (let r = 0; r < nRows; r++) {
            const start = rowPtr[r];
            const end = rowPtr[r + 1];
            for (let k = start; k < end; k++) {
                const c = colIdx[k];
                const w = values[k];
                edges.push({
                    from: String(windowInfo.ids[r]),
                    to: String(windowInfo.ids[c]),
                    weight: Number.isFinite(w) ? w : 0,
                    dir: 1
                });
            }
        }
        return { Nodes: nodes, Edges: edges };
    }
}

class RuntimeState {
    constructor({ kvmStore, memeStore, sessionStore, params, config }) {
        this.config = { ...(config || {}) };
        this.config.robotsDir = this.config.robotsDir || path.join(__dirname, 'robots');
        this.config.lemmaCsv = this.config.lemmaCsv || path.join(__dirname, 'lemma.csv');
        this.config.lemmaAutoload = this.config.lemmaAutoload ?? false;
        this.config.lemmaMaxBytes = this.config.lemmaMaxBytes ?? (64 * 1024 * 1024);
        this.config.lemmaForce = this.config.lemmaForce ?? false;
        this.config.kvmCacheMaxEntries = this.config.kvmCacheMaxEntries ?? 50_000;
        this.config.robotsChunkMinWords = this.config.robotsChunkMinWords ?? 2;
        this.config.robotsChunkMaxWords = this.config.robotsChunkMaxWords ?? 5;

        this.kvm = new KVMStore(kvmStore, { maxCacheEntries: this.config.kvmCacheMaxEntries });
        this.graph = new MemeGraph(memeStore);
        this.sessions = new SessionManager(sessionStore);
        // “反接”层：从模因层回到用户可读表达（短语/句子）
        this.surfaceStore = new NamespacedStore(sessionStore, 'surface');
        this.dialogStore = new NamespacedStore(sessionStore, 'dialog');
        this.surfaceLexicon = new MemeSurfaceLexicon(this.surfaceStore);
        this.dialogMemory = new DialogMemory(this.dialogStore);
        this.tensor = new TensorEngine();
        this.pattern = new PatternMatrix(this);
        this.params = { ...modelDefaults, ...(params || {}) };
        this.metrics = { requests: 0, lastLatency: 0, updatedAt: Date.now() };
        // 在线搜索配置：支持运行时开关与 endpoint 库
        this.config.search = {
            enabled: this.config.search?.enabled ?? true,
            endpoints: Array.isArray(this.config.search?.endpoints) ? this.config.search.endpoints : [],
            active: typeof this.config.search?.active === 'string' ? this.config.search.active : (this.config.searchEndpoint || '')
        };
        // 兼容旧字段：searchEndpoint
        if (!this.config.search.active && this.config.searchEndpoint) {
            this.config.search.active = String(this.config.searchEndpoint);
        }
        // 默认库：如果没配置 endpoints 但 active 有值，则塞入库里
        if (this.config.search.active && !this.config.search.endpoints.includes(this.config.search.active)) {
            this.config.search.endpoints.push(this.config.search.active);
        }
        this.researcher = new OnlineResearcher(this, { endpoint: this.config.search.active });
        this.researcher.enabled = Boolean(this.config.search.enabled);
        this.corpusStats = { ingested: 0, lastIngest: null };
        this.robotsCorpus = null;
    }

    getSearchConfig() {
        return {
            enabled: Boolean(this.config.search?.enabled),
            active: String(this.config.search?.active || ''),
            endpoints: Array.isArray(this.config.search?.endpoints) ? this.config.search.endpoints.slice() : []
        };
    }

    setSearchConfig(patch = {}) {
        if (!this.config.search) {
            this.config.search = { enabled: true, endpoints: [], active: '' };
        }
        if (typeof patch.enabled === 'boolean') {
            this.config.search.enabled = patch.enabled;
        }
        if (Array.isArray(patch.endpoints)) {
            const cleaned = patch.endpoints
                .map((x) => String(x || '').trim())
                .filter(Boolean);
            this.config.search.endpoints = Array.from(new Set(cleaned));
        }
        if (typeof patch.active === 'string') {
            const next = patch.active.trim();
            this.config.search.active = next;
            if (next && Array.isArray(this.config.search.endpoints) && !this.config.search.endpoints.includes(next)) {
                this.config.search.endpoints.push(next);
            }
        }
        // 同步到 researcher
        if (this.researcher) {
            this.researcher.endpoint = this.config.search.active || '';
            this.researcher.enabled = Boolean(this.config.search.enabled);
        }
        // 兼容旧字段
        this.config.searchEndpoint = this.config.search.active;
        return this.getSearchConfig();
    }

    cloneParams() {
        return { ...this.params };
    }

    setParams(partial) {
        Object.assign(this.params, partial);
    }

    _activation() {
        if (this.params.activationType === 'custom') {
            this.params._activationCustom = this.params._activationCustom || compileCustomFunctionSafely(this.params.activationCustom, ['x'], BuiltinActivations.relu);
            return this.params._activationCustom;
        }
        return BuiltinActivations[this.params.activationType] || BuiltinActivations.relu;
    }

    _transfer() {
        if (this.params.transferType === 'custom') {
            this.params._transferCustom = this.params._transferCustom || compileCustomFunctionSafely(this.params.transferCustom, ['value', 'weight', 'decayK', 'ctx'], BuiltinTransfers.linear);
            return this.params._transferCustom;
        }
        return BuiltinTransfers[this.params.transferType] || BuiltinTransfers.linear;
    }

    mapWordsToMemes(words) {
        // 细化：输入不仅映射到“单词模因”，还会生成/融合“多词模因(短语模因)”。
        const tokens = Array.isArray(words) ? words.map((w) => String(w || '').trim()).filter(Boolean) : [];
        const memeStrength = new Map();
        const maxUnits = 128;
        let units = 0;

        // 1) unigram：保持兼容
        for (const word of tokens) {
            const memes = this.kvm.getWordMemeSet(word);
            if (!memes || memes.size === 0) {
                const memeId = `meme_${hashString(word)}`;
                this.graph.ensureNode(memeId);
                this.kvm.link(word, memeId);
                memeStrength.set(memeId, (memeStrength.get(memeId) ?? 0) + 1);
                continue;
            }
            for (const memeId of memes) {
                memeStrength.set(memeId, (memeStrength.get(memeId) ?? 0) + 1);
            }
        }

        // 2) phrase/word-structure meme：以 ngram 作为概念单元，模因仍为“多个词的集合”
        const nMin = Math.max(2, Number(this.params.memeNgramMin ?? 2) || 2);
        const nMax = Math.max(nMin, Number(this.params.memeNgramMax ?? 4) || 4);
        const minOverlap = Math.max(1, Number(this.params.minOverlapThreshold ?? 2) || 2);
        const maxWordSet = Math.max(4, Number(this.params.maxMemeWords ?? 100) || 100);

        const resolveOrCreateMemeForTokenSet = (tokenSet) => {
            const uniq = Array.from(new Set(tokenSet.map((x) => String(x || '').trim()).filter(Boolean))).slice(0, maxWordSet);
            if (uniq.length <= 1) {
                const w = uniq[0];
                return w ? `meme_${hashString(w)}` : null;
            }
            // 统计每个候选 meme 与 tokenSet 的重合词数
            const counts = new Map();
            for (const w of uniq) {
                const memes = this.kvm.getWordMemeSet(w);
                if (!memes || memes.size === 0) {
                    continue;
                }
                for (const mid of memes) {
                    counts.set(mid, (counts.get(mid) ?? 0) + 1);
                }
            }
            let best = null;
            let bestOverlap = 0;
            for (const [mid, c] of counts.entries()) {
                if (c > bestOverlap) {
                    bestOverlap = c;
                    best = mid;
                }
            }
            if (best && bestOverlap >= minOverlap) {
                // “融合”：把当前 tokenSet 的词也挂到 best meme 上
                for (const w of uniq) {
                    this.kvm.link(w, best);
                }
                this.graph.ensureNode(best);
                return best;
            }
            // 新建短语模因：ID 由“词集合”决定，保证稳定
            const sorted = uniq.slice().sort();
            const memeId = `meme_p_${hashString(sorted.join('|'))}`;
            this.graph.ensureNode(memeId);
            for (const w of sorted) {
                this.kvm.link(w, memeId);
            }
            return memeId;
        };

        // 生成 ngram 单元并映射到 meme，权重按长度提升
        for (let i = 0; i < tokens.length && units < maxUnits; i++) {
            for (let n = nMin; n <= nMax && units < maxUnits; n++) {
                if (i + n > tokens.length) {
                    break;
                }
                const gram = tokens.slice(i, i + n);
                const memeId = resolveOrCreateMemeForTokenSet(gram);
                if (!memeId) {
                    continue;
                }
                const w = 1 + 0.5 * (n - 1);
                memeStrength.set(memeId, (memeStrength.get(memeId) ?? 0) + w);
                units += 1;
            }
        }

        return memeStrength;
    }

    _buildMemeSequenceFromTokens(tokens) {
        const list = Array.isArray(tokens) ? tokens.map((t) => String(t || '').trim()).filter(Boolean) : [];
        const nMin = Math.max(2, Number(this.params.memeNgramMin ?? 2) || 2);
        const nMax = Math.max(nMin, Number(this.params.memeNgramMax ?? 4) || 4);
        const minOverlap = Math.max(1, Number(this.params.minOverlapThreshold ?? 2) || 2);
        const maxWordSet = Math.max(4, Number(this.params.maxMemeWords ?? 100) || 100);

        const resolveOrCreate = (tokenSet) => {
            const uniq = Array.from(new Set(tokenSet.map((x) => String(x || '').trim()).filter(Boolean))).slice(0, maxWordSet);
            if (uniq.length <= 1) {
                const w = uniq[0];
                return w ? `meme_${hashString(w)}` : null;
            }
            const counts = new Map();
            for (const w of uniq) {
                const memes = this.kvm.getWordMemeSet(w);
                if (!memes || memes.size === 0) continue;
                for (const mid of memes) counts.set(mid, (counts.get(mid) ?? 0) + 1);
            }
            let best = null;
            let bestOverlap = 0;
            for (const [mid, c] of counts.entries()) {
                if (c > bestOverlap) {
                    bestOverlap = c;
                    best = mid;
                }
            }
            if (best && bestOverlap >= minOverlap) {
                for (const w of uniq) this.kvm.link(w, best);
                this.graph.ensureNode(best);
                return best;
            }
            const sorted = uniq.slice().sort();
            const memeId = `meme_p_${hashString(sorted.join('|'))}`;
            this.graph.ensureNode(memeId);
            for (const w of sorted) this.kvm.link(w, memeId);
            return memeId;
        };

        const seq = [];
        for (let i = 0; i < list.length; i++) {
            // 用更长的 ngram 优先，减少“句子级”颗粒
            let picked = null;
            for (let n = nMax; n >= nMin; n--) {
                if (i + n > list.length) continue;
                picked = resolveOrCreate(list.slice(i, i + n));
                if (picked) {
                    break;
                }
            }
            if (!picked) {
                const w = list[i];
                picked = w ? `meme_${hashString(w)}` : null;
                if (picked) {
                    this.graph.ensureNode(picked);
                    this.kvm.link(w, picked);
                }
            }
            if (picked) {
                if (seq.length === 0 || seq[seq.length - 1] !== picked) {
                    seq.push(picked);
                }
            }
        }
        return seq;
    }

    _buildSeedVector(windowInfo, seeds) {
        const vec = new Float32Array(windowInfo.ids.length);
        for (const [memeId, strength] of seeds.entries()) {
            const idx = windowInfo.index.get(memeId);
            if (idx !== undefined) {
                vec[idx] = strength;
            }
        }
        return vec;
    }

    runPropagation(seeds, options = {}) {
        const radiusRaw = options.radius ?? options.windowRadius;
        const radius = Math.max(1, Math.min(6, Number(radiusRaw ?? 2) || 2));
        const windowInfo = this.graph.buildWindow(Array.from(seeds.keys()), radius);
        const seedVector = this._buildSeedVector(windowInfo, seeds);
        const act = this._activation();
        const iteration = Math.max(1, Number(options.iteration ?? this.params.iteration ?? 5) || 5);
        const output = this.tensor.iteratePropagation(windowInfo.csr, seedVector, iteration, act, this.params.decayK, 0.02);
        this.pattern.rebuild(windowInfo);
        return { windowInfo, seedVector, activation: output };
    }

    _pickTopActivatedMemes(result, seeds, { limit = 18, minScore = 1e-6 } = {}) {
        const { windowInfo, activation } = result || {};
        if (!windowInfo || !Array.isArray(windowInfo.ids) || !activation) return [];
        const seedIds = new Set(seeds ? Array.from(seeds.keys()) : []);

        const isConnectedToSeeds = (memeId) => {
            if (seedIds.size === 0 || seedIds.has(memeId)) {
                return true;
            }
            const table = this.graph.nodes.get(memeId);
            if (table) {
                for (const neighborId of table.keys()) {
                    if (seedIds.has(neighborId)) {
                        return true;
                    }
                }
            }
            for (const seedId of seedIds) {
                const seedTable = this.graph.nodes.get(seedId);
                if (seedTable && seedTable.has(memeId)) {
                    return true;
                }
            }
            return false;
        };

        const scored = [];
        for (let i = 0; i < windowInfo.ids.length; i++) {
            const memeId = windowInfo.ids[i];
            const score = activation[i];
            if (!Number.isFinite(score) || score <= minScore) continue;
            if (!isConnectedToSeeds(memeId)) continue;
            scored.push({ memeId, score });
        }
        scored.sort((a, b) => b.score - a.score);
        return scored.slice(0, Math.max(1, Number(limit) || 18));
    }

    _makeSignatureFromTopMemes(topMemes, { limit = 12 } = {}) {
        const ids = (Array.isArray(topMemes) ? topMemes : [])
            .map((x) => (typeof x === 'string' ? x : x?.memeId))
            .filter(Boolean)
            .slice(0, Math.max(3, Number(limit) || 12));
        // signature 用 memeId 列表，排序保证稳定
        const uniq = Array.from(new Set(ids));
        uniq.sort();
        return uniq.join('|');
    }

    composeReply(result, words, seeds) {
        const topMemes = this._pickTopActivatedMemes(result, seeds, { limit: 18 });
        const signature = this._makeSignatureFromTopMemes(topMemes, { limit: 12 });

        // 1) 优先：对话记忆检索（更像“在训练集中找答案”）
        const memoryHit = this.dialogMemory.retrieve({
            memes: topMemes.map((x) => x.memeId),
            signature,
            minSim: 0.45
        });
        if (memoryHit && typeof memoryHit.reply === 'string' && memoryHit.reply.trim()) {
            return memoryHit.reply.trim();
        }

        // 2) 其次：模因 -> 表层表达（短语/句子）反接
        const phraseScores = new Map();
        for (const item of topMemes) {
            const list = this.surfaceLexicon.getTop(item.memeId, { limit: 4 });
            for (const c of list) {
                const p = String(c.phrase || '').trim();
                if (!p) continue;
                const prev = phraseScores.get(p) ?? 0;
                // meme 激活分数做门控，词典分数做权重
                phraseScores.set(p, prev + (Math.max(0, item.score) * (0.5 + Math.max(0, c.score))));
            }
        }
        const phraseOrdered = Array.from(phraseScores.entries())
            .sort((a, b) => b[1] - a[1])
            .map(([p]) => p);
        if (phraseOrdered.length) {
            return phraseOrdered.slice(0, 2).join('。');
        }

        // 3) 回退：旧逻辑（meme -> words）
        const { windowInfo, activation } = result;
        const seedIds = new Set(seeds ? Array.from(seeds.keys()) : []);
        const baseWords = Array.from(new Set((words || []).map((w) => String(w).trim()).filter(Boolean)));

        const isConnectedToSeeds = (memeId) => {
            if (seedIds.size === 0 || seedIds.has(memeId)) {
                return true;
            }
            const table = this.graph.nodes.get(memeId);
            if (table) {
                for (const neighborId of table.keys()) {
                    if (seedIds.has(neighborId)) {
                        return true;
                    }
                }
            }
            for (const seedId of seedIds) {
                const seedTable = this.graph.nodes.get(seedId);
                if (seedTable && seedTable.has(memeId)) {
                    return true;
                }
            }
            return false;
        };

        const candidateScores = new Map();
        for (let i = 0; i < windowInfo.ids.length; i++) {
            const memeId = windowInfo.ids[i];
            const score = activation[i];
            if (!Number.isFinite(score) || score <= 0) {
                continue;
            }
            if (!isConnectedToSeeds(memeId)) {
                continue;
            }
            const linkedWords = this.kvm.getMemeWords(memeId) || [];
            for (const rawWord of linkedWords) {
                const word = String(rawWord || '').trim();
                if (!word) {
                    continue;
                }
                const prev = candidateScores.get(word);
                if (prev === undefined || score > prev) {
                    candidateScores.set(word, score);
                }
            }
        }

        const ordered = Array.from(candidateScores.entries())
            .sort((a, b) => b[1] - a[1])
            .map(([word]) => word);

        const finalWords = [];
        const seen = new Set();
        const pushWord = (word) => {
            if (!word || seen.has(word)) {
                return;
            }
            seen.add(word);
            finalWords.push(word);
        };

        for (const word of baseWords) {
            pushWord(word);
        }
        for (const word of ordered) {
            pushWord(word);
            if (finalWords.length >= 30) {
                break;
            }
        }

        if (finalWords.length === 0) {
            finalWords.push('I need more context to respond.');
        }

        return finalWords.slice(0, 30).join(' ');
    }

    learnFromDialog({ payload, result } = {}) {
        try {
            const question = typeof payload?.text === 'string'
                ? payload.text
                : (Array.isArray(payload?.tokens) ? payload.tokens.join(' ') : '');
            const reply = typeof result?.reply === 'string' ? result.reply : '';
            if (!question.trim() || !reply.trim()) {
                return { ok: false, reason: 'missing-text' };
            }

            // 以当前推理结果为主：topMemes -> signature
            const seeds = Array.isArray(result?.seeds)
                ? new Map(result.seeds.map((pair) => [pair[0], pair[1]]))
                : this.mapWordsToMemes(tokenize(question));
            const windowInfo = Array.isArray(result?.memes) ? { ids: result.memes } : null;
            const activation = Array.isArray(result?.activation) ? Float32Array.from(result.activation) : null;
            const resObj = (windowInfo && activation) ? { windowInfo, activation } : this.runPropagation(seeds);

            const topMemes = this._pickTopActivatedMemes(resObj, seeds, { limit: 18 });
            const signature = this._makeSignatureFromTopMemes(topMemes, { limit: 12 });
            const memeIds = topMemes.map((x) => x.memeId);

            // 学习：每个高激活 meme 绑定 reply 的表层表达
            for (const item of topMemes.slice(0, 10)) {
                const w = Math.max(0.5, Math.min(3, item.score));
                this.surfaceLexicon.learn(item.memeId, reply, { weight: w });
            }

            // 学习：签名级别的“对话记忆”（检索更直接）
            this.dialogMemory.remember({
                signature,
                memes: memeIds,
                question,
                reply,
                scoreHint: topMemes[0]?.score ?? 0
            });
            return { ok: true, memes: memeIds.length, signatureLen: signature ? signature.split('|').length : 0 };
        } catch (err) {
            return { ok: false, error: err.message };
        }
    }

    processInput(payload) {
        const started = Date.now();
        const text = payload.text != null ? payload.text : (payload.message != null ? String(payload.message) : '');
        const sessionId = this.sessions.ensure(payload.sessionId);
        const tokensFromPayload = Array.isArray(payload.tokens) && payload.tokens.length ? payload.tokens
            : Array.isArray(payload.words) && payload.words.length ? payload.words
            : Array.isArray(payload.vocab) && payload.vocab.length ? payload.vocab
            : null;
        const words = tokensFromPayload ? tokensFromPayload.map((w) => String(w)) : tokenize(text);

        const budget = payload && typeof payload === 'object' ? payload.budget : null;
        const depth = Math.max(1, Number(budget?.mappingDepth ?? budget?.depth ?? this.params.mappingDepth ?? 1) || 1);
        const topMemesK = Math.max(3, Number(budget?.reflectionTopMemes ?? budget?.topMemes ?? this.params.reflectionTopMemes ?? 18) || 18);
        const topWordsK = Math.max(3, Number(budget?.reflectionTopWords ?? budget?.topWords ?? this.params.reflectionTopWords ?? 24) || 24);
        const minScoreRaw = budget?.reflectionMinScore ?? budget?.minScore ?? this.params.reflectionMinScore;
        const minScore = Number.isFinite(Number(minScoreRaw)) ? Number(minScoreRaw) : 1e-6;
        const iterRaw = budget?.iteration;
        const iteration = Math.max(1, Number(iterRaw ?? this.params.iteration ?? 5) || 5);
        const radiusRaw = budget?.radius ?? budget?.windowRadius;
        const radius = Math.max(1, Math.min(6, Number(radiusRaw ?? 2) || 2));

        let seeds = this.mapWordsToMemes(words);
        let result = null;
        if (depth > 1) {
            for (let hop = 1; hop < depth; hop++) {
                result = this.runPropagation(seeds, { iteration, radius });
                const topMemes = this._pickTopActivatedMemes(result, seeds, { limit: topMemesK, minScore });
                const wordScore = new Map();
                for (const m of topMemes) {
                    const linked = this.kvm.getMemeWords(m.memeId);
                    if (!linked) continue;
                    for (const w of linked) {
                        const ww = String(w || '').trim();
                        if (!ww) continue;
                        const prev = wordScore.get(ww) ?? 0;
                        wordScore.set(ww, Math.max(prev, Number(m.score) || 0));
                    }
                }
                const expanded = Array.from(wordScore.entries())
                    .sort((a, b) => b[1] - a[1])
                    .slice(0, topWordsK)
                    .map(([w]) => w);
                const merged = Array.from(new Set([...(words || []).slice(0, 64), ...expanded]));
                seeds = this.mapWordsToMemes(merged);
            }
        }
        result = result || this.runPropagation(seeds, { iteration, radius });
        const reply = this.composeReply(result, words, seeds);
        const latency = Date.now() - started;
        this.metrics.requests += 1;
        this.metrics.lastLatency = latency;
        this.metrics.updatedAt = Date.now();
        return {
            reply,
            seeds: Array.from(seeds.entries()),
            activation: Array.from(result.activation),
            memes: result.windowInfo.ids,
            sessionId,
            latency,
            params: this.cloneParams()
        };
    }

    async ingestDocument(doc) {
        const text = doc?.text ? String(doc.text) : '';
        const tokens = Array.isArray(doc?.tokens) ? doc.tokens : tokenize(text);
        if (!text.trim() && tokens.length === 0) {
            return { ok: false, reason: 'empty-text' };
        }
        if (tokens.length === 0) {
            return { ok: false, reason: 'no-tokens' };
        }

        const seeds = this.mapWordsToMemes(tokens);
        // 使用有序“词结构/短语结构”序列建边（从句子级细化到词/短语级）
        const memeIds = this._buildMemeSequenceFromTokens(tokens);
        for (let i = 0; i < memeIds.length - 1; i++) {
            this.graph.link(memeIds[i], memeIds[i + 1], 1, 0);
        }
        const source = doc?.source;
        for (const memeId of memeIds) {
            const meta = this.graph.meta.get(memeId) || {};
            meta.lastTouched = Date.now();
            meta.degree = (this.graph.nodes.get(memeId) || new Map()).size;
            if (source) {
                meta.sources = meta.sources || {};
                meta.sources[source] = (meta.sources[source] || 0) + 1;
            }
            this.graph.meta.set(memeId, meta);
            this.graph.store.put(`node:${memeId}`, meta);
        }
        this.corpusStats.ingested += 1;
        this.corpusStats.lastIngest = Date.now();
        return {
            ok: true,
            memes: memeIds.length,
            edges: Math.max(0, memeIds.length - 1),
            source: source || null
        };
    }

    _ensureRobotsCorpus() {
        if (!this.robotsCorpus) {
            this.robotsCorpus = new RobotsCorpus({
                dir: this.config.robotsDir,
                lemmaCsv: this.config.lemmaCsv,
                lemmaAutoload: this.config.lemmaAutoload,
                lemmaMaxBytes: this.config.lemmaMaxBytes,
                lemmaForce: this.config.lemmaForce,
                chunkMinWords: this.config.robotsChunkMinWords,
                chunkMaxWords: this.config.robotsChunkMaxWords
            });
        }
        return this.robotsCorpus;
    }

    listRobotsFiles() {
        return this._ensureRobotsCorpus().listFiles();
    }

    collectRobotsDocuments(options = {}) {
        return this._ensureRobotsCorpus().collect(options);
    }

    async ingestRobotsCorpus(options = {}) {
        const docs = this.collectRobotsDocuments(options);
        const details = [];
        for (const doc of docs) {
            details.push({ doc, result: await this.ingestDocument(doc) });
        }
        return { ok: true, ingested: details.length, details };
    }

    async forgetMemes({ olderThan, limit = 64 } = {}) {
        const threshold = olderThan ?? (Date.now() - 7 * 24 * 60 * 60 * 1000);
        const removed = [];
        for (const [memeId, meta] of Array.from(this.graph.meta.entries())) {
            if ((meta?.lastTouched ?? 0) >= threshold) {
                continue;
            }
            const words = Array.from(this.kvm.getMemeWords(memeId) || []);
            for (const word of words) {
                this.kvm.unlink(word, memeId);
            }
            if (this.graph.removeNode(memeId)) {
                removed.push({ memeId, words });
            }
            if (removed.length >= limit) {
                break;
            }
        }
        return { ok: true, removed };
    }

    async onlineLookup(input, options = {}) {
        if (!this.researcher) {
            return { ok: false, source: 'none', reason: 'researcher-disabled' };
        }
        return this.researcher.lookup(input, options);
    }

    toSnapshot() {
        return {
            params: this.params,
            graph: this.graph.exportSnapshot(),
            sessions: this.sessions.export(),
            kvm: this.kvm.exportEntries(),
            surface: this.surfaceLexicon ? this.surfaceLexicon.exportSnapshot({ limitMemes: 512 }) : null,
            dialog: this.dialogMemory ? this.dialogMemory.exportSnapshot({ limit: 512 }) : null
        };
    }

    async fromSnapshot(snapshot) {
        if (!snapshot) {
            return;
        }
        this.params = { ...modelDefaults, ...(snapshot.params || {}) };
        this.graph.importSnapshot(snapshot.graph || { nodes: [], meta: {} });
        this.sessions.import(snapshot.sessions || []);
        if (snapshot.kvm) {
            for (const [word, memes] of Object.entries(snapshot.kvm.words || {})) {
                for (const meme of memes) {
                    this.kvm.link(word, meme);
                }
            }
        }
        if (snapshot.surface && this.surfaceLexicon) {
            this.surfaceLexicon.importSnapshot(snapshot.surface);
        }
        if (snapshot.dialog && this.dialogMemory) {
            this.dialogMemory.importSnapshot(snapshot.dialog);
        }
    }

    // 将当前窗口或指定种子集合导出为 Go 侧 Graph 结构并写入文件
    exportGraphToFile({ seeds = null, radius = 2, file = null } = {}) {
        const usedSeeds = seeds instanceof Map ? seeds : this.mapWordsToMemes(Array.isArray(seeds) ? seeds : []);
        const win = this.graph.buildWindow(Array.from(usedSeeds.keys()), radius);
        const act = this.tensor.iteratePropagation(win.csr, this._buildSeedVector(win, usedSeeds), this.params.iteration || 5, this._activation(), this.params.decayK, 0.02);
        const graphObj = GraphExportBuilder.fromWindow(win, act);
        const dir = CONFIG.exportDir || path.join(__dirname, 'runtime_store');
        ensureDir(dir);
        const name = `graph_export_${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
        const outFile = file ? path.resolve(file) : path.join(dir, name);
        fs.writeFileSync(outFile, JSON.stringify(graphObj));
        return outFile;
    }
}

class Controller extends EventEmitter {
    constructor(name, runtime) {
        super();
        this.name = name;
        this.runtime = runtime;
        this.online = true;
        this.health = { status: 'ok', since: Date.now(), failures: 0 };
        this.lastSnapshot = null;
    }

    async respond(payload) {
        if (!this.online) {
            throw new Error(`Controller ${this.name} offline`);
        }
        const result = this.runtime.processInput(payload);
        return result;
    }

    metrics() {
        return {
            name: this.name,
            online: this.online,
            health: this.health,
            metrics: this.runtime.metrics,
            params: this.runtime.cloneParams()
        };
    }

    applyParams(params) {
        this.runtime.setParams(params);
    }

    async applySnapshot(snapshot) {
        await this.runtime.fromSnapshot(snapshot);
        this.lastSnapshot = snapshot;
        this.emit('updated', snapshot);
    }

    snapshot() {
        const snap = this.runtime.toSnapshot();
        this.lastSnapshot = snap;
        return snap;
    }
}

class ControllerPool extends EventEmitter {
    constructor(baseStores) {
        super();
        this.baseStores = baseStores;
        this.controllers = {};
        this.groups = {}; // groupId -> controllerNames[]
        const groupCount = Math.max(1, Number(baseStores?.config?.groupCount ?? CONFIG.groupCount) || 3);
        const groupSize = Math.max(1, Number(baseStores?.config?.groupSize ?? CONFIG.groupSize) || 7);
        this.groupIds = Array.from({ length: groupCount }, (_, i) => `G${i + 1}`);

        for (const groupId of this.groupIds) {
            const names = [];
            const sharedStores = {
                kvmStore: new NamespacedStore(baseStores.kvmStore, `${groupId}:kvm`),
                memeStore: new NamespacedStore(baseStores.memeStore, `${groupId}:graph`),
                sessionStore: new NamespacedStore(baseStores.sessionStore, `${groupId}:session`)
            };
            for (let i = 0; i < groupSize; i++) {
                const name = `${groupId}_AI${i + 1}`;
                const stores = {
                    ...baseStores,
                    ...sharedStores,
                    config: { ...(baseStores.config || {}), controllerName: name, groupId, groupIndex: i + 1 }
                };
                this.controllers[name] = new Controller(name, new RuntimeState(stores));
                names.push(name);
            }
            this.groups[groupId] = names;
        }

        // Rotation/compat: keep serving/standby/validation pointing to first group first three
        const g1 = this.groupIds[0] || 'G1';
        const g1Names = this.groups[g1] || Object.keys(this.controllers);
        this.serving = this.controllers[g1Names[0]];
        this.standby = this.controllers[g1Names[1] || g1Names[0]];
        this.validation = this.controllers[g1Names[2] || g1Names[0]];
    }

    getActive() {
        return this.serving;
    }

    getByName(name) {
        const n = String(name || '').trim();
        if (!n) return null;
        // Backward compatibility: A/B/C map to G1 first 3
        if (n === 'A' || n === 'B' || n === 'C') {
            const g1 = this.groupIds[0] || 'G1';
            const list = this.groups[g1] || [];
            if (n === 'A') return this.controllers[list[0]] || null;
            if (n === 'B') return this.controllers[list[1]] || null;
            if (n === 'C') return this.controllers[list[2]] || null;
        }
        return this.controllers[n] || null;
    }

    listControllerNames() {
        return Object.keys(this.controllers);
    }

    listGroupIds() {
        return this.groupIds.slice();
    }

    listControllersInGroup(groupId) {
        return (this.groups[String(groupId || '').trim()] || []).slice();
    }

    async hotSwap(snapshot) {
        await this.standby.applySnapshot(snapshot);
        const prevServing = this.serving;
        this.serving = this.standby;
        this.standby = prevServing;
        this.emit('hotswap', { serving: this.serving.name, standby: this.standby.name });
    }

    listMetrics() {
        return Object.values(this.controllers).map((ctrl) => ctrl.metrics());
    }

    broadcast(fn) {
        for (const ctrl of Object.values(this.controllers)) {
            fn(ctrl.runtime, ctrl);
        }
    }

    async ingestDocument(doc) {
        const results = await Promise.all(Object.values(this.controllers).map((ctrl) => ctrl.runtime.ingestDocument(doc)));
        return results;
    }

    async ingestDocumentTo(name, doc) {
        const ctrl = this.getByName(name);
        if (!ctrl) {
            return { ok: false, reason: 'controller-not-found' };
        }
        return ctrl.runtime.ingestDocument(doc);
    }

    async ingestDocumentToGroup(groupId, doc) {
        const names = this.listControllersInGroup(groupId);
        if (!names.length) {
            return { ok: false, reason: 'group-not-found' };
        }
        const results = await Promise.all(names.map((name) => this.ingestDocumentTo(name, doc)));
        return results;
    }

    async forgetMemes(criteria) {
        const results = await Promise.all(Object.values(this.controllers).map((ctrl) => ctrl.runtime.forgetMemes(criteria)));
        return results;
    }

    async onlineResearch(input, options = {}) {
        return this.serving.runtime.onlineLookup(input, options);
    }
}

class RotationManager {
    constructor(pool, { cycleMs = 5 * 60 * 1000, learnIters = 3, minImprove = 0.05 } = {}) {
        this.pool = pool;
        this.cycleMs = cycleMs;
        this.learnIters = learnIters;
        this.minImprove = minImprove;
        this.timer = null;
        this.running = false;
    }

    start() {
        if (this.running) {
            return;
        }
        this.running = true;
        this.timer = setInterval(() => this._runCycle().catch((err) => console.error('[Rotation] cycle error', err)), this.cycleMs);
    }

    stop() {
        if (!this.running) {
            return;
        }
        clearInterval(this.timer);
        this.running = false;
    }

    async _runCycle() {
        const ctrlB = this.pool.standby;
        const ctrlC = this.pool.validation;
        for (let i = 0; i < this.learnIters; i++) {
            ctrlB.runtime.params.iteration += 1;
        }
        const evalScore = Math.random();
        const baseScore = Math.random();
        if (evalScore - baseScore >= this.minImprove) {
            await this.pool.hotSwap(ctrlB.snapshot());
        }
        await ctrlC.applySnapshot(ctrlB.snapshot());
    }
}

class RedisSynchronizer extends EventEmitter {
    constructor(pool, { url, channel }) {
        super();
        this.pool = pool;
        this.url = url;
        this.channel = channel;
        this.pub = null;
        this.sub = null;
    }

    async start() {
        if (this.pub || this.sub) {
            return;
        }
        this.pub = redis.createClient({ url: this.url });
        this.sub = redis.createClient({ url: this.url });
        this.pub.on('error', (err) => console.error('[Redis] pub error:', err.message));
        this.sub.on('error', (err) => console.error('[Redis] sub error:', err.message));
        await this.pub.connect();
        await this.sub.connect();
        await this.sub.subscribe(this.channel, async (message) => {
            try {
                const snapshot = JSON.parse(message);
                await this.pool.hotSwap(snapshot);
                this.emit('applied', { controller: this.pool.getActive().name });
            } catch (err) {
                console.error('[Redis] failed to apply snapshot', err.message);
            }
        });
    }

    async publish(snapshot) {
        if (!this.pub) {
            return;
        }
        await this.pub.publish(this.channel, JSON.stringify(snapshot));
    }
}

class StudyEngine {
    constructor(pool, redisSync) {
        this.pool = pool;
        this.redis = redisSync;
        this.running = false;
        this.queue = [];
        this.metrics = { enqueued: 0, processed: 0, lastTickAt: 0, lastError: null };
        this.poolWorker = null;
    }

    _ensureWorkerPool() {
        if (this.poolWorker) {
            return this.poolWorker;
        }
        this.poolWorker = workerpool.pool(CONFIG.workerFile, {
            minWorkers: 1,
            maxWorkers: CONFIG.maxWorkers,
            workerType: 'process'
        });
        return this.poolWorker;
    }

    start() {
        if (this.running) {
            return;
        }
        this.running = true;
        setInterval(() => this._tick().catch((err) => console.error('[Study] tick error', err)), 60_000);
    }

    enqueueDocument(doc) {
        this.queue.push(doc);
        this.metrics.enqueued += 1;
    }

    async _tick() {
        if (!this.running || this.queue.length === 0) {
            return;
        }
        const doc = this.queue.shift();
        try {
            this.metrics.lastTickAt = Date.now();
            if (doc) {
                await this.pool.ingestDocument(doc);
            }
            // memeMergeWorker.cjs 并未导出 ingestDocument，这里调用会触发：Unknown method "ingestDocument"。
            // 保留 workerpool 管线：用 batchLemmatize 做一次轻量预处理/校验。
            try {
                const text = String(doc?.text || '');
                const tokens = tokenize(text);
                const wp = this._ensureWorkerPool();
                await wp.exec('batchLemmatize', [[tokens], this.pool.getActive().runtime?.config?.lemmaCsv]);
            } catch (_e) {
                // ignore
            }
            this.metrics.processed += 1;
            this.metrics.lastError = null;
        } catch (err) {
            console.error('[Study] worker ingest failed:', err.message);
            this.metrics.lastError = String(err.message || err);
        }
        const snapshot = this.pool.standby.snapshot();
        await this.redis.publish(snapshot);
    }
}

class ShardDescriptor {
    constructor(name) {
        this.name = name;
        this.label = `AI-${name}`;
        this.embedding = new Float32Array(64);
        this.history = [];
        this.offline = false;
    }

    updateEmbedding(vec) {
        const alpha = 0.2;
        for (let i = 0; i < this.embedding.length; i++) {
            this.embedding[i] = (1 - alpha) * this.embedding[i] + alpha * vec[i];
        }
    }

    record(entry) {
        this.history.push({ ...entry, ts: Date.now() });
        if (this.history.length > 50) {
            this.history.shift();
        }
    }
}

const cosineSim = (a, b) => {
    let dot = 0;
    let na = 0;
    let nb = 0;
    for (let i = 0; i < a.length; i++) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if (na === 0 || nb === 0) {
        return 0;
    }
    return dot / (Math.sqrt(na) * Math.sqrt(nb));
};

const hashStrSimple = (str, seed = 1315423911) => {
    let hash = seed;
    for (let i = 0; i < str.length; i++) {
        hash ^= ((hash << 5) + str.charCodeAt(i) + (hash >> 2)) >>> 0;
    }
    return hash >>> 0;
};

const textToMiniEmbedding = (text, dim = 64) => {
    const vec = new Float32Array(dim);
    const words = tokenize(String(text || ''));
    for (const word of words) {
        const idx = hashStrSimple(word) % dim;
        vec[idx] += 1;
    }
    if (words.length > 1) {
        for (let i = 0; i < words.length - 1; i++) {
            const pair = `${words[i]}_${words[i + 1]}`;
            const idx = hashStrSimple(pair) % dim;
            vec[idx] += 0.5;
        }
    }
    return vec;
};

class ShardManager {
    constructor(pool) {
        this.pool = pool;
        this.shards = {};
        const names = typeof pool?.listControllerNames === 'function'
            ? pool.listControllerNames()
            : Object.keys(pool?.controllers || {});
        for (const name of names) {
            this.shards[name] = new ShardDescriptor(name);
        }
        this.lastEmbedding = new Float32Array(64);
    }

    chooseShard(text) {
        const requestEmbedding = textToMiniEmbedding(text, 64);
        this.lastEmbedding = requestEmbedding;
        let best = this.pool.getActive();
        let bestScore = -Infinity;
        for (const [name, shard] of Object.entries(this.shards)) {
            const score = cosineSim(requestEmbedding, shard.embedding);
            if (score > bestScore) {
                const ctrl = this.pool.getByName(name);
                if (ctrl && ctrl.online) {
                    bestScore = score;
                    best = ctrl;
                }
            }
        }
        return best;
    }

    metrics() {
        return Object.entries(this.shards).map(([name, shard]) => ({ name, shard }));
    }

    record(controllerName, requestEmbedding, replyText, latency) {
        const shard = this.shards[controllerName] || null;
        if (!shard) {
            return;
        }
        const replyEmbedding = textToMiniEmbedding(replyText || '', requestEmbedding.length || 64);
        shard.updateEmbedding(replyEmbedding);
        shard.record({ latency, affinity: cosineSim(requestEmbedding, replyEmbedding) });
    }
}

const perturbTokens = (words) => {
    if (words.length <= 1) {
        return words;
    }
    const idx = Math.floor(Math.random() * words.length);
    return words.filter((_, i) => i !== idx);
};

const buildVariants = (text, count = 0) => {
    const variants = [];
    if (count <= 0) {
        return variants;
    }
    let words = tokenize(text);
    for (let i = 0; i < count; i++) {
        words = perturbTokens(words);
        variants.push(words.join(' '));
        if (words.length <= 1) {
            break;
        }
    }
    return variants;
};

const clamp01 = (x) => Math.max(0, Math.min(1, x));
const clamp11 = (x) => Math.max(-1, Math.min(1, x));

const makeRng = (seed) => {
    // xorshift32
    let x = (seed >>> 0) || 0x9e3779b9;
    return () => {
        x ^= x << 13;
        x ^= x >>> 17;
        x ^= x << 5;
        return ((x >>> 0) / 0xffffffff);
    };
};

class PersonaForestAverager {
    constructor(options = {}) {
        this.enabled = options.enabled !== false;
        this.trees = Math.max(8, Number(options.trees ?? 32) || 32);
        this.featureSubspace = Math.max(2, Number(options.featureSubspace ?? 4) || 4);
        this.sampleRate = clamp01(Number(options.sampleRate ?? 0.85) || 0.85);
        this.personaMomentum = clamp01(Number(options.personaMomentum ?? 0.92) || 0.92);
        this.targetReplyLen = Math.max(20, Number(options.targetReplyLen ?? 220) || 220);
        this.maxHistory = Math.max(10, Number(options.maxHistory ?? 60) || 60);
        this.globalPersona = null; // Float32Array
        this.controllerPersona = new Map(); // name -> Float32Array
    }

    _blendPersona(prev, next, momentum) {
        if (!next) return prev;
        if (!prev) return Float32Array.from(next);
        const out = new Float32Array(prev.length);
        for (let i = 0; i < prev.length; i++) {
            const v = (momentum * prev[i]) + ((1 - momentum) * next[i]);
            out[i] = Number.isFinite(v) ? v : 0;
        }
        return out;
    }

    _centroid(vecs, dim) {
        if (!Array.isArray(vecs) || vecs.length === 0) return null;
        const out = new Float32Array(dim);
        let count = 0;
        for (const v of vecs) {
            if (!v || v.length !== dim) continue;
            for (let i = 0; i < dim; i++) out[i] += v[i];
            count += 1;
        }
        if (!count) return null;
        for (let i = 0; i < dim; i++) out[i] /= count;
        return out;
    }

    _tokenOverlapRatio(aTokens, bTokens) {
        if (!aTokens.length || !bTokens.length) return 0;
        const setB = new Set(bTokens);
        let hit = 0;
        for (const t of aTokens) if (setB.has(t)) hit += 1;
        return hit / Math.max(1, aTokens.length);
    }

    _variantStability(controller) {
        const variants = controller?.variants;
        if (!Array.isArray(variants) || variants.length === 0) return 0.5;
        let sum = 0;
        let n = 0;
        for (const v of variants) {
            if (!v || !v.response || typeof v.affinity !== 'number') continue;
            if (!Number.isFinite(v.affinity)) continue;
            sum += clamp11(v.affinity);
            n += 1;
        }
        if (!n) return 0.5;
        // map [-1,1] -> [0,1]
        return clamp01((sum / n + 1) / 2);
    }

    pick({ payload, layerResults, requestEmbedding, history = [] }) {
        if (!this.enabled) {
            return null;
        }
        const dim = requestEmbedding?.length ?? 64;
        const inputText = String(payload?.text || '');
        const inputTokens = tokenize(inputText);

        const candidates = [];
        for (const layer of layerResults || []) {
            for (const controller of layer.controllers || []) {
                const base = controller?.base;
                const reply = base?.reply;
                if (!base || !reply) continue;
                const emb = textToMiniEmbedding(String(reply), dim);
                const affinity = Number.isFinite(controller.affinity) ? clamp11(controller.affinity) : clamp11(cosineSim(requestEmbedding, emb));
                candidates.push({
                    layer: layer.layer,
                    controller: controller.controller,
                    reply: String(reply),
                    latency: base.latency,
                    sessionId: base.sessionId,
                    affinity,
                    emb,
                    controllerRef: controller
                });
            }
        }

        if (candidates.length === 0) return null;
        if (candidates.length === 1) {
            const only = candidates[0];
            this._updatePersonas(only);
            return { ...only, score: 0, method: 'forest-single' };
        }

        const candidateEmbeddings = candidates.map((c) => c.emb);
        const consensusCentroid = this._centroid(candidateEmbeddings, dim);
        const lastAgg = Array.isArray(history) && history.length ? history[history.length - 1]?.aggregate : null;
        const lastReplyEmb = lastAgg?.reply ? textToMiniEmbedding(String(lastAgg.reply), dim) : null;
        const lastReplyLen = typeof lastAgg?.reply === 'string' ? lastAgg.reply.length : null;
        const targetLen = Number.isFinite(lastReplyLen) && lastReplyLen > 20 ? lastReplyLen : this.targetReplyLen;

        // feature extraction
        for (const c of candidates) {
            const replyTokens = tokenize(c.reply);
            const overlap = this._tokenOverlapRatio(inputTokens, replyTokens); // 0..1
            const consensus = consensusCentroid ? clamp11(cosineSim(c.emb, consensusCentroid)) : 0;
            const personaCoherence = this.globalPersona ? clamp11(cosineSim(c.emb, this.globalPersona)) : 0;
            const ctrlPersona = this.controllerPersona.get(c.controller);
            const controllerCoherence = ctrlPersona ? clamp11(cosineSim(c.emb, ctrlPersona)) : 0;
            const novelty = lastReplyEmb ? clamp01(1 - ((clamp11(cosineSim(c.emb, lastReplyEmb)) + 1) / 2)) : 0.5;
            const lenPenalty = -clamp01(Math.abs(c.reply.length - targetLen) / Math.max(40, targetLen));
            const stability = this._variantStability(c.controllerRef);
            const latencyNorm = Number.isFinite(c.latency) ? clamp01(1 - (Number(c.latency) / 2000)) : 0.5;
            c.features = {
                affinity: clamp01((c.affinity + 1) / 2),
                overlap,
                consensus: clamp01((consensus + 1) / 2),
                persona: clamp01((personaCoherence + 1) / 2),
                controllerPersona: clamp01((controllerCoherence + 1) / 2),
                novelty,
                lenPenalty: clamp01(1 + lenPenalty), // [-1,0] -> [0,1]
                stability,
                latency: latencyNorm
            };
        }

        const featureNames = Object.keys(candidates[0].features);
        const seedText = `${payload?.sessionId || ''}|${hashStrSimple(inputText)}|${candidates.length}`;
        const rng = makeRng(hashStrSimple(seedText));

        const scores = new Map();
        const votes = new Map();
        for (const c of candidates) {
            scores.set(c, 0);
            votes.set(c, 0);
        }

        const pickSubspace = () => {
            const chosen = new Set();
            const maxPick = Math.min(this.featureSubspace, featureNames.length);
            while (chosen.size < maxPick) {
                const idx = Math.floor(rng() * featureNames.length);
                chosen.add(featureNames[idx]);
            }
            return Array.from(chosen);
        };

        for (let t = 0; t < this.trees; t++) {
            const subspace = pickSubspace();
            // random weights per tree, with mild bias toward affinity/consensus/persona
            const weights = {};
            for (const f of subspace) {
                let w = (rng() * 2 - 1);
                if (f === 'affinity' || f === 'consensus' || f === 'persona') w *= 1.4;
                if (f === 'controllerPersona' || f === 'stability') w *= 1.1;
                if (f === 'novelty') w *= 0.6;
                weights[f] = w;
            }

            // bagging candidates
            const bag = candidates.filter(() => rng() <= this.sampleRate);
            const bagCandidates = bag.length >= 2 ? bag : candidates;

            let best = null;
            let bestScore = -Infinity;
            for (const c of bagCandidates) {
                let s = 0;
                for (const f of subspace) {
                    const v = c.features[f];
                    s += weights[f] * (Number.isFinite(v) ? v : 0);
                }
                if (s > bestScore) {
                    bestScore = s;
                    best = c;
                }
            }
            if (best) {
                votes.set(best, (votes.get(best) || 0) + 1);
            }
            // accumulate soft score for all candidates to enable tie-breaks
            for (const c of candidates) {
                let s = 0;
                for (const f of subspace) {
                    const v = c.features[f];
                    s += weights[f] * (Number.isFinite(v) ? v : 0);
                }
                scores.set(c, (scores.get(c) || 0) + s);
            }
        }

        const ranked = candidates
            .map((c) => ({
                c,
                votes: votes.get(c) || 0,
                score: (scores.get(c) || 0) / Math.max(1, this.trees)
            }))
            .sort((a, b) => (b.votes - a.votes) || (b.score - a.score) || (b.c.features.consensus - a.c.features.consensus) || (b.c.features.affinity - a.c.features.affinity));

        const winner = ranked[0]?.c;
        if (!winner) return null;

        this._updatePersonas(winner);
        const picked = {
            layer: winner.layer,
            controller: winner.controller,
            affinity: winner.affinity,
            reply: winner.reply,
            latency: winner.latency,
            sessionId: winner.sessionId,
            score: ranked[0].score,
            votes: ranked[0].votes,
            method: 'persona-forest'
        };
        return picked;
    }

    _updatePersonas(winner) {
        if (!winner?.emb) return;
        const next = winner.emb;
        this.globalPersona = this._blendPersona(this.globalPersona, next, this.personaMomentum);
        const prevCtrl = this.controllerPersona.get(winner.controller) || null;
        this.controllerPersona.set(winner.controller, this._blendPersona(prevCtrl, next, this.personaMomentum));

        // keep map bounded
        if (this.controllerPersona.size > 64) {
            const keys = Array.from(this.controllerPersona.keys());
            while (this.controllerPersona.size > 64) {
                this.controllerPersona.delete(keys[Math.floor(Math.random() * keys.length)]);
            }
        }
    }
}

const normalizeBudget = (raw) => {
    if (raw === undefined || raw === null || raw === '' || raw === false) {
        return null;
    }
    if (typeof raw === 'string') {
        const s = raw.trim();
        const lowered = s.toLowerCase();
        if (lowered === 'default' || lowered === 'balanced' || lowered === 'medium' || lowered === 'none') {
            return null;
        }
        if (lowered === 'low' || lowered === 'fast') {
            return { iteration: 3, reflectionTopMemes: 12, reflectionTopWords: 16 };
        }
        if (lowered === 'high' || lowered === 'slow' || lowered === 'quality') {
            return { iteration: 7, reflectionTopMemes: 22, reflectionTopWords: 32 };
        }
        if (s.startsWith('{') && s.endsWith('}')) {
            try {
                return normalizeBudget(JSON.parse(s));
            } catch (_e) {
                return null;
            }
        }
        return null;
    }

    if (typeof raw !== 'object') {
        return null;
    }
    const out = {};
    const pickNum = (key, ...aliases) => {
        const v = raw[key];
        if (Number.isFinite(Number(v))) {
            out[key] = Number(v);
            return;
        }
        for (const a of aliases) {
            const av = raw[a];
            if (Number.isFinite(Number(av))) {
                out[key] = Number(av);
                return;
            }
        }
    };
    pickNum('mappingDepth', 'depth');
    pickNum('iteration', 'iters');
    pickNum('reflectionTopMemes', 'topMemes');
    pickNum('reflectionTopWords', 'topWords');
    pickNum('reflectionMinScore', 'minScore');
    pickNum('radius', 'windowRadius');
    return Object.keys(out).length ? out : null;
};

const mergeBudgets = (base, override) => {
    if (!base && !override) return null;
    if (!base) return override;
    if (!override) return base;
    return { ...base, ...override };
};

class InferenceProcessPool {
    constructor({ workerFile, maxWorkers, config } = {}) {
        this.workerFile = workerFile;
        this.maxWorkers = Math.max(1, Number(maxWorkers) || 1);
        this.config = config || null;
        this.workers = [];
        this.pending = new Map();
        this._seq = 1;
        this.timeoutMs = 60_000;

        for (let i = 0; i < this.maxWorkers; i++) {
            const cp = childProcess.fork(workerFile, [], {
                stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
                env: { ...process.env, AI_INFER_WORKER: '1' }
            });
            cp.on('message', (msg) => {
                try {
                    const id = msg && msg.id;
                    if (!id) return;
                    const entry = this.pending.get(id);
                    if (!entry) return;
                    clearTimeout(entry.timer);
                    this.pending.delete(id);
                    if (msg.ok) {
                        entry.resolve(msg.result);
                    } else {
                        entry.reject(new Error(msg.error || 'inference-failed'));
                    }
                } catch (_e) {}
            });
            cp.on('exit', (_code, _signal) => {
                for (const [id, entry] of this.pending.entries()) {
                    if (entry.workerPid === cp.pid) {
                        clearTimeout(entry.timer);
                        this.pending.delete(id);
                        entry.reject(new Error('inference-worker-exited'));
                    }
                }
            });
            this.workers.push(cp);
        }
    }

    _hash(str) {
        let h = 2166136261;
        for (let i = 0; i < str.length; i++) {
            h ^= str.charCodeAt(i);
            h = Math.imul(h, 16777619);
        }
        return h >>> 0;
    }

    _pickWorker(controllerName) {
        const n = this.workers.length;
        if (!n) throw new Error('inference-no-workers');
        const idx = this._hash(String(controllerName || '')) % n;
        return this.workers[idx];
    }

    async respond({ controllerName, payload }) {
        const id = `${process.pid}-${Date.now()}-${this._seq++}`;
        const worker = this._pickWorker(String(controllerName || ''));
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pending.delete(id);
                reject(new Error('inference-timeout'));
            }, this.timeoutMs);

            this.pending.set(id, { resolve, reject, timer, workerPid: worker.pid });
            try {
                worker.send({
                    id,
                    cmd: 'respond',
                    controllerName,
                    payload: payload || {},
                    config: this.config
                });
            } catch (e) {
                clearTimeout(timer);
                this.pending.delete(id);
                reject(e);
            }
        });
    }

    async terminate() {
        for (const cp of this.workers) {
            try { cp.kill(); } catch (_e) {}
        }
    }
}

let __inferencePool = null;
const getInferencePool = () => {
    if (!CONFIG.inferMP || CONFIG.groupProc) return null;
    if (__inferencePool) return __inferencePool;
    const workerFile = path.join(__dirname, 'inferenceWorker.cjs');
    __inferencePool = new InferenceProcessPool({
        workerFile,
        maxWorkers: CONFIG.inferWorkers,
        config: CONFIG
    });
    return __inferencePool;
};

class SparkArray {
    /**
     * @param {ControllerPool} pool
     * @param {ShardManager} shardManager
     * @param {Object} [options]
     * @param {number} [options.numAI] - 对话AI数量，默认7
     * @param {Array} [options.layers] - 自定义层结构
     */
    constructor(pool, shardManager, options = {}) {
        this.pool = pool;
        this.shardManager = shardManager;
        this.infer = options.inferPool || getInferencePool();
        this.groupId = String(options.groupId || (typeof pool?.listGroupIds === 'function' ? (pool.listGroupIds()[0] || 'G1') : 'G1'));
        const available = typeof pool?.listControllersInGroup === 'function'
            ? pool.listControllersInGroup(this.groupId)
            : (typeof pool?.listControllerNames === 'function' ? pool.listControllerNames() : Object.keys(pool?.controllers || {}));
        const wantedRaw = options.numAI ?? options.groupSize ?? CONFIG.sparkNumAI ?? CONFIG.groupSize ?? 7;
        const wanted = Math.max(1, Math.round(Number(wantedRaw) || 7));
        const numAI = Math.max(1, Math.min(available.length || wanted, wanted));
        // 组内小 SparkArray：默认 numAI 个 AI（不足则截断）
        this.layers = Array.from({ length: numAI }, (_, i) => ({
            name: `${this.groupId}:a${i + 1}`,
            controllers: [available[i]],
            strategy: 'max'
        }));
        // 如果外部传入自定义层结构则覆盖
        if (Array.isArray(options.layers) && options.layers.length > 0) {
            this.layers = options.layers.map((layer) => ({ strategy: 'max', ...layer }));
        }
        this.personaForest = new PersonaForestAverager(options.personaForest || {});
        this.defaultBudget = normalizeBudget(options.budget ?? CONFIG.sparkBudget);
        this.history = [];
    }

    getLayers() {
        return this.layers.slice();
    }

    updateLayers(layers) {
        if (!Array.isArray(layers) || layers.length === 0) {
            throw new Error('layers must be a non-empty array');
        }
        for (const layer of layers) {
            if (!layer.name || !Array.isArray(layer.controllers)) {
                throw new Error('invalid layer specification');
            }
        }
        this.layers = layers.map((layer) => ({ strategy: 'max', ...layer }));
    }

    async dispatch(payload, options = {}) {
        const requestEmbedding = textToMiniEmbedding(payload.text || '', 64);
        const variants = buildVariants(payload.text || '', options.perturbations || 0);
        let layers = options.multiLayer === false ? [this.layers[0]] : this.layers;
        if (Number.isFinite(Number(options.numAI)) && Number(options.numAI) > 0) {
            const cap = Math.max(1, Math.floor(Number(options.numAI)));
            layers = layers.slice(0, cap);
        }
        const layerResults = [];

        const budget = mergeBudgets(this.defaultBudget, normalizeBudget(options.budget ?? payload?.budget));

        const callRespond = async (controllerName, nextPayload) => {
            if (this.infer) {
                return this.infer.respond({ controllerName, payload: nextPayload });
            }
            const ctrl = this.pool.getByName(controllerName);
            if (!ctrl || !ctrl.online) {
                throw new Error('controller-offline');
            }
            return ctrl.respond(nextPayload);
        };

        for (const layer of layers) {
            const controllers = [];
            if (this.infer) {
                const tasks = (layer.controllers || []).map(async (controllerSpec) => {
                    const controllerName = typeof controllerSpec === 'string'
                        ? controllerSpec
                        : String(controllerSpec?.name || controllerSpec?.controller || '').trim();
                    const weightRaw = typeof controllerSpec === 'object' && controllerSpec
                        ? (controllerSpec.weight ?? controllerSpec.w)
                        : 1;
                    const weight = Math.max(1, Math.min(8, Math.round(Number(weightRaw) || 1)));
                    if (!controllerName) {
                        return { controller: '', error: 'invalid-controller' };
                    }

                    const weightedText = weight <= 1
                        ? String(payload.text || '')
                        : Array.from({ length: weight }, () => String(payload.text || '')).join(' ');

                    let baseResult;
                    try {
                        baseResult = await callRespond(controllerName, { ...payload, text: weightedText, ...(budget ? { budget } : {}) });
                    } catch (err) {
                        return { controller: controllerName, error: err?.message || 'dispatch-failed' };
                    }

                    const affinity = cosineSim(requestEmbedding, textToMiniEmbedding(baseResult.reply || '', 64));
                    this.shardManager.record(controllerName, requestEmbedding, baseResult.reply, baseResult.latency);
                    const variantResults = [];
                    for (const variant of variants) {
                        try {
                            const vr = await callRespond(controllerName, { ...payload, text: variant, ...(budget ? { budget } : {}) });
                            variantResults.push({
                                text: variant,
                                response: vr,
                                affinity: cosineSim(textToMiniEmbedding(variant, 64), textToMiniEmbedding(vr.reply || '', 64))
                            });
                        } catch (err) {
                            variantResults.push({ text: variant, error: err?.message || 'variant-failed' });
                        }
                    }
                    return { controller: controllerName, base: baseResult, affinity, variants: variantResults };
                });

                const settled = await Promise.allSettled(tasks);
                for (const s of settled) {
                    if (s.status === 'fulfilled') {
                        controllers.push(s.value);
                    } else {
                        controllers.push({ controller: 'unknown', error: s.reason?.message || 'dispatch-failed' });
                    }
                }
            } else {
                for (const controllerSpec of layer.controllers) {
                    const controllerName = typeof controllerSpec === 'string'
                        ? controllerSpec
                        : String(controllerSpec?.name || controllerSpec?.controller || '').trim();
                    const weightRaw = typeof controllerSpec === 'object' && controllerSpec
                        ? (controllerSpec.weight ?? controllerSpec.w)
                        : 1;
                    const weight = Math.max(1, Math.min(8, Math.round(Number(weightRaw) || 1)));
                    const ctrl = this.pool.getByName(controllerName);
                    if (!ctrl || !ctrl.online) {
                        continue;
                    }
                    let baseResult;
                    try {
                        const weightedText = weight <= 1
                            ? String(payload.text || '')
                            : Array.from({ length: weight }, () => String(payload.text || '')).join(' ');
                        baseResult = await ctrl.respond({ ...payload, text: weightedText, ...(budget ? { budget } : {}) });
                    } catch (err) {
                        controllers.push({
                            controller: controllerName,
                            error: err.message || 'dispatch-failed'
                        });
                        continue;
                    }
                    const affinity = cosineSim(requestEmbedding, textToMiniEmbedding(baseResult.reply || '', 64));
                    this.shardManager.record(controllerName, requestEmbedding, baseResult.reply, baseResult.latency);
                    const variantResults = [];
                    for (const variant of variants) {
                        try {
                            const vr = await ctrl.respond({ ...payload, text: variant, ...(budget ? { budget } : {}) });
                            variantResults.push({
                                text: variant,
                                response: vr,
                                affinity: cosineSim(textToMiniEmbedding(variant, 64), textToMiniEmbedding(vr.reply || '', 64))
                            });
                        } catch (err) {
                            variantResults.push({
                                text: variant,
                                error: err.message || 'variant-failed'
                            });
                        }
                    }
                    controllers.push({
                        controller: controllerName,
                        base: baseResult,
                        affinity,
                        variants: variantResults
                    });
                }
            }
            layerResults.push({ layer: layer.name, controllers });
        }

        const aggregate = this._aggregate(layerResults, payload, requestEmbedding);
        this._recordHistory(payload, aggregate, layerResults);
        return { aggregate, layers: layerResults };
    }

    // 多轮“大 SparkArray”：用上一轮 aggregate 作为下一轮输入以修补缝隙
    async dispatchBig(payload, options = {}) {
        const rounds = Math.max(1, Math.min(10, Number(options.bigRounds ?? options.bigSparkRounds ?? 1) || 1));
        const history = [];
        let current = { ...(payload || {}) };
        const originalText = String(current.text || '');
        for (let i = 0; i < rounds; i++) {
            const r = await this.dispatch(current, options);
            history.push(r);
            const reply = r?.aggregate?.reply ? String(r.aggregate.reply) : '';
            if (!reply.trim()) {
                break;
            }
            // 以“原问题 + 上轮回答”为下一轮输入，长度做截断避免爆炸
            const combined = `${originalText}\n${reply}`;
            current = { ...current, text: combined.slice(-4000) };
        }
        const last = history[history.length - 1] || { aggregate: null, layers: [] };
        return { ...last, rounds: history };
    }

    _aggregate(layerResults, payload = {}, requestEmbedding = null) {
        let best = null;
        for (const layer of layerResults) {
            for (const controller of layer.controllers) {
                if (!controller.base) {
                    continue;
                }
                if (!best || controller.affinity > best.affinity) {
                    best = {
                        layer: layer.layer,
                        controller: controller.controller,
                        affinity: controller.affinity,
                        reply: controller.base.reply,
                        latency: controller.base.latency,
                        sessionId: controller.base.sessionId,
                        method: 'max-affinity'
                    };
                }
            }
        }
        
        // 随机森林式“中途平均/投票”：在不改变对外结构的前提下，优先选择更稳定且共识更强的回复
        try {
            const picked = this.personaForest.pick({
                payload: { text: payload?.text || '', sessionId: payload?.sessionId },
                layerResults,
                requestEmbedding: requestEmbedding || textToMiniEmbedding(payload?.text || '', 64),
                history: this.history.slice(-this.personaForest.maxHistory)
            });
            if (picked && picked.reply) {
                return {
                    layer: picked.layer,
                    controller: picked.controller,
                    affinity: picked.affinity,
                    reply: picked.reply,
                    latency: picked.latency,
                    sessionId: picked.sessionId,
                    method: picked.method,
                    score: picked.score,
                    votes: picked.votes
                };
            }
        } catch (err) {
            // fall back to old behavior
        }
        return best;
    }

    _recordHistory(payload, aggregate, layerResults) {
        this.history.push({
            ts: Date.now(),
            request: { text: payload.text, sessionId: payload.sessionId },
            aggregate,
            layers: layerResults
        });
        if (this.history.length > 100) {
            this.history.shift();
        }
    }
}

class BigSparkArray {
    constructor(pool, shardManager, options = {}) {
        this.pool = pool;
        this.shardManager = shardManager;
        const groupIds = Array.isArray(options.groupIds) && options.groupIds.length
            ? options.groupIds
            : (typeof pool?.listGroupIds === 'function' ? pool.listGroupIds() : ['G1']);
        this.groups = groupIds.map((groupId) => ({
            groupId,
            weight: Math.max(1, Math.min(8, Math.round(Number(options?.groupWeights?.[groupId] ?? 1) || 1))),
            spark: new SparkArray(pool, shardManager, { groupId, ...(options.groupOptions || {}) })
        }));
        this.personaForest = new PersonaForestAverager(options.personaForest || {});
        this.history = [];
    }

    getLayers() {
        return this.groups.map((g) => ({ groupId: g.groupId, weight: g.weight, layers: g.spark.getLayers() }));
    }

    updateLayers(patch) {
        // 兼容旧端点：如果传入数组，当作更新第一个工作组的小阵列
        if (Array.isArray(patch)) {
            this.groups[0]?.spark?.updateLayers(patch);
            return;
        }
        // 也支持 { groupId, layers }
        const groupId = String(patch?.groupId || '').trim();
        const layers = patch?.layers;
        if (groupId && Array.isArray(layers)) {
            const g = this.groups.find((x) => x.groupId === groupId);
            if (g) g.spark.updateLayers(layers);
        }
    }

    async dispatch(payload, options = {}) {
        const requestEmbedding = textToMiniEmbedding(payload?.text || '', 64);
        const groupResults = [];

        for (const g of this.groups) {
            const groupPayload = { ...(payload || {}) };
            // 组权重：同样体现在文本重复次数（组级别风格/偏好）
            const w = Math.max(1, g.weight);
            if (w > 1) {
                const t = String(groupPayload.text || '');
                groupPayload.text = Array.from({ length: w }, () => t).join(' ');
            }
            const result = await g.spark.dispatch(groupPayload, options);
            groupResults.push({ groupId: g.groupId, result });
        }

        // 组间聚合：将每个工作组的 aggregate 当作候选，做随机森林式投票/共识
        const layerResults = [
            {
                layer: 'groups',
                controllers: groupResults.map((gr) => {
                    const agg = gr?.result?.aggregate;
                    return {
                        controller: gr.groupId,
                        base: agg ? { reply: agg.reply, latency: agg.latency, sessionId: agg.sessionId } : null,
                        affinity: agg ? clamp11(Number(agg.affinity ?? 0)) : 0,
                        variants: []
                    };
                })
            }
        ];

        let aggregate = null;
        try {
            const picked = this.personaForest.pick({
                payload: { text: payload?.text || '', sessionId: payload?.sessionId },
                layerResults,
                requestEmbedding,
                history: this.history.slice(-this.personaForest.maxHistory)
            });
            if (picked && picked.reply) {
                aggregate = {
                    layer: 'groups',
                    controller: picked.controller,
                    affinity: picked.affinity,
                    reply: picked.reply,
                    latency: picked.latency,
                    sessionId: picked.sessionId,
                    method: picked.method,
                    score: picked.score,
                    votes: picked.votes
                };
            }
        } catch (_e) {
            // ignore
        }
        if (!aggregate) {
            // fallback：选择 reply 非空且 affinity 最大的组
            let best = null;
            for (const gr of groupResults) {
                const agg = gr?.result?.aggregate;
                if (!agg || !agg.reply) continue;
                const a = Number.isFinite(agg.affinity) ? agg.affinity : 0;
                if (!best || a > best.affinity) {
                    best = { ...agg, controller: gr.groupId, layer: 'groups' };
                }
            }
            aggregate = best;
        }

        const out = { aggregate, groups: groupResults };
        this.history.push({ ts: Date.now(), request: { text: payload?.text, sessionId: payload?.sessionId }, aggregate, groups: groupResults });
        if (this.history.length > 100) this.history.shift();
        return out;
    }

    async dispatchBig(payload, options = {}) {
        const rounds = Math.max(1, Math.min(10, Number(options.bigRounds ?? options.bigSparkRounds ?? 1) || 1));
        const history = [];
        let current = { ...(payload || {}) };
        const originalText = String(current.text || '');
        for (let i = 0; i < rounds; i++) {
            const r = await this.dispatch(current, options);
            history.push(r);
            const reply = r?.aggregate?.reply ? String(r.aggregate.reply) : '';
            if (!reply.trim()) break;
            const combined = `${originalText}\n${reply}`;
            current = { ...current, text: combined.slice(-4000) };
        }
        const last = history[history.length - 1] || { aggregate: null, groups: [] };
        return { ...last, rounds: history };
    }
}

// 模因阻断模块 - 识别并隔离恶性模因（参考实验版）
class MemeBarrier {
    constructor(runtime, {
        scanIntervalMs = 10_000,
        maliciousThreshold = 0.7,
        maxIsolatePerScan = 5
    } = {}) {
        this.runtime = runtime;
        this.scanIntervalMs = scanIntervalMs;
        this.maliciousThreshold = maliciousThreshold;
        this.maxIsolatePerScan = maxIsolatePerScan;
        this.timer = null;
        this.stats = { scans: 0, isolated: 0, lastScanTime: 0, lastIsolated: [] };
    }

    start() {
        if (this.timer) return;
        this.timer = setInterval(() => {
            try { this.scanNetwork(); } catch (e) { console.error('[MemeBarrier] 扫描错误:', e); }
        }, this.scanIntervalMs);
        this.running = true;
        console.log('[MemeBarrier] 已启动');
    }

    stop() {
        if (this.timer) { clearInterval(this.timer); this.timer = null; }
        this.running = false;
        console.log('[MemeBarrier] 已停止');
    }

    generateReason(meme, score) {
        const conn = meme.connect || [];
        return `score=${score.toFixed(3)} conn=${conn.length}`;
    }

    evaluateMaliciousness(meme) {
        const conn = meme.connect || [];
        const degree = conn.length;
        const selfLoops = conn.filter(([w, pid, dir]) => pid === meme.pointID).length;
        const outDegree = conn.filter(([w, pid, dir]) => dir === 2).length;
        // 访问增长近似
        let growth = 0;
        try {
            const access = this.runtime.wordAccessLog?.get(meme.pointID);
            if (access && access.size) {
                const total = Array.from(access.values()).reduce((a, b) => a + b, 0);
                growth = Math.min(1, Math.log1p(total) / 10);
            }
        } catch (_) {}
        const avgConn = this.getAverageConnections();
        const outSkew = avgConn > 0 ? Math.min(1, outDegree / (avgConn * 3)) : 0;
        const selfSkew = degree > 0 ? Math.min(1, selfLoops / degree) : 0;
        return 0.5 * growth + 0.3 * outSkew + 0.2 * selfSkew;
    }

    getAverageConnections() {
        const points = this.runtime.graph.getAllPoints();
        if (!points.length) return 0;
        const sum = points.reduce((acc, p) => acc + (p.connect?.length || 0), 0);
        return sum / points.length;
    }

    isolateMeme(memeID, score, reason) {
        const point = this.runtime.graph.points.get(memeID);
        if (!point) return false;
        const conn = point.connect || [];
        let cut = 0;
        for (let i = conn.length - 1; i >= 0; i--) {
            const [w, pid, dir] = conn[i];
            if (dir === 2) { // 出边
                conn.splice(i, 1);
                cut++;
                const target = this.runtime.graph.points.get(pid);
                if (target && target.connect) {
                    for (let j = target.connect.length - 1; j >= 0; j--) {
                        const [tw, tpid, tdir] = target.connect[j];
                        if (tpid === memeID && tdir === 1) { // 目标的入边
                            target.connect.splice(j, 1);
                        }
                    }
                }
            }
        }
        this.stats.isolated++;
        this.stats.lastIsolated.unshift({ memeID, score, reason, cut });
        this.stats.lastIsolated = this.stats.lastIsolated.slice(0, 20);
        console.log(`[MemeBarrier] 隔离 ${memeID}: cut=${cut} ${reason}`);
        return cut > 0;
    }

    scanNetwork() {
        const points = this.runtime.graph.getAllPoints();
        this.stats.scans++;
        this.stats.lastScanTime = Date.now();
        if (!points.length) return;
        const scored = points.map(p => {
            const s = this.evaluateMaliciousness(p);
            return { id: p.pointID, meme: p, score: s, reason: this.generateReason(p, s) };
        }).sort((a, b) => b.score - a.score);
        let isolated = 0;
        for (const item of scored) {
            if (item.score >= this.maliciousThreshold) {
                if (this.isolateMeme(item.id, item.score, item.reason)) {
                    isolated++;
                    if (isolated >= this.maxIsolatePerScan) break;
                }
            } else { break; }
        }
        if (isolated > 0) {
            console.log(`[MemeBarrier] 本次扫描隔离 ${isolated} 个可疑模因`);
        }
    }

    getStats() { return { ...this.stats }; }
}

// 强化学习模块：基于线性代数与可选外部库的评估-更新循环
class ReinforcementLearner {
    constructor(pool, {
        testsDir = path.join(__dirname, 'tests'),
        maxDocs = 64,
        topKWords = 30,
        improvementThreshold = 0.01,
        iterations = 5,
        useUmap = true
    } = {}) {
        this.pool = pool;
        this.testsDir = testsDir;
        this.maxDocs = maxDocs;
        this.topKWords = topKWords;
        this.iterations = iterations;
        this.useUmap = useUmap;
        this.improvementThreshold = improvementThreshold;
        this.history = [];
        // 统一使用上方安全引用的 Matrix（可能为 null）
        this.Matrix = getMatrix();
        this.kmeans = safeRequire('ml-kmeans');
        this.numeric = safeRequire('numeric');
    }

    _listTestFiles() {
        if (!fs.existsSync(this.testsDir)) {
            return [];
        }
        return fs.readdirSync(this.testsDir)
            .filter((n) => n.toLowerCase().endsWith('.txt'))
            .sort((a, b) => a.localeCompare(b))
            .slice(0, this.maxDocs);
    }

    _readText(file) {
        try {
            return fs.readFileSync(path.join(this.testsDir, file), 'utf8');
        } catch (err) {
            return '';
        }
    }

    _scoreReplyQuality(reply, seeds) {
        const words = tokenize(reply);
        if (!words.length) return 0;
        let coverage = 0;
        for (const [memeId, strength] of seeds.entries()) {
            const linked = this.pool.getActive().runtime.kvm.getMemeWords(memeId);
            if (!linked || linked.size === 0) continue;
            let hit = 0;
            for (const w of words) {
                if (linked.has(w)) { hit += 1; }
            }
            coverage += hit / Math.max(1, linked.size);
        }
        coverage = coverage / Math.max(1, seeds.size);
        const uniq = new Set(words).size / Math.max(1, words.length);
        return 0.7 * coverage + 0.3 * uniq;
    }

    _buildFeatureForDoc(text) {
        const vec = textToMiniEmbedding(text, 128);
        return Array.from(vec);
    }

    _reduceFeatures(features) {
        if (this.Matrix) {
            const X = new this.Matrix(features);
            // 简易 PCA：协方差 + 特征分解（若 numeric 可用）
            if (this.numeric) {
                try {
                    const mean = X.mean('row');
                    const centered = X.clone();
                    for (let i = 0; i < centered.rows; i++) {
                        centered.setRow(i, centered.getRow(i).map((v, j) => v - mean[j]));
                    }
                    const Xt = centered.transpose();
                    const cov = Xt.mmul(centered).div(Math.max(1, centered.rows - 1));
                    // 使用 numeric 进行特征分解
                    const covArr = cov.to2DArray();
                    const eig = this.numeric.eig(covArr);
                    const vectors = eig.E?.x || eig.E; // 列为特征向量
                    if (!vectors) throw new Error('eig-vectors-missing');
                    const v1 = vectors.map((row) => row[0]);
                    const v2 = vectors.map((row) => row[1] ?? 0);
                    const coords = [];
                    for (let i = 0; i < centered.rows; i++) {
                        const r = centered.getRow(i);
                        const x = r.reduce((acc, v, j) => acc + v * (v1[j] ?? 0), 0);
                        const y = r.reduce((acc, v, j) => acc + v * (v2[j] ?? 0), 0);
                        coords.push([x, y]);
                    }
                    return coords;
                } catch (err) {
                    // numeric.eig 不收敛或失败时，回退到内置 PCA
                    const reducer = new DimReducer();
                    const emb = { data: Float32Array.from(features.flat()), nRows: features.length, nCols: features[0]?.length || 1 };
                    const proj = reducer.project2D(emb, 'pca');
                    return proj.coords;
                }
            }
            // 回退：基于随机投影的近似降维
            const D = X.columns;
            const rp1 = Array.from({ length: D }, () => Math.random() - 0.5);
            const rp2 = Array.from({ length: D }, () => Math.random() - 0.5);
            const coords = [];
            for (let i = 0; i < X.rows; i++) {
                const row = X.getRow(i);
                const x = row.reduce((acc, v, j) => acc + v * rp1[j], 0);
                const y = row.reduce((acc, v, j) => acc + v * rp2[j], 0);
                coords.push([x, y]);
            }
            return coords;
        }
        // 无外部库：使用已有 DimReducer 的 PCA
        const bridge = new GraphTensorBridge(this.pool.getActive().runtime);
        const reducer = new DimReducer();
        const emb = { data: Float32Array.from(features.flat()), nRows: features.length, nCols: features[0]?.length || 1 };
        const proj = reducer.project2D(emb, 'pca');
        return proj.coords;
    }

    _cluster(coords, k = 3) {
        if (this.kmeans) {
            try {
                const out = this.kmeans(coords, k);
                return out.clusters;
            } catch (err) {
                // 回退到简单阈值聚类
            }
        }
        // 简易聚类：按 x 坐标等距切分
        const xs = coords.map((c) => c[0]);
        const min = Math.min(...xs);
        const max = Math.max(...xs);
        const step = (max - min) / Math.max(1, k);
        return coords.map((c) => Math.min(k - 1, Math.max(0, Math.floor((c[0] - min) / Math.max(step, 1e-6)))));
    }

    _adjustParams(evalStats) {
        const active = this.pool.getActive();
        const params = active.runtime.cloneParams();
        const baseScore = evalStats.baseAvg;
        const learnedScore = evalStats.learnedAvg;
        if (learnedScore - baseScore >= this.improvementThreshold) {
            params.iteration = Math.min(12, (params.iteration || 5) + 1);
            params.decayK = Math.max(0.5, (params.decayK || 1) - 0.05);
        } else {
            params.iteration = Math.max(3, (params.iteration || 5) - 1);
            params.decayK = Math.min(2.0, (params.decayK || 1) + 0.05);
        }
        active.applyParams(params);
        return params;
    }

    async evaluateOnce(file) {
        const text = this._readText(file);
        const ctrl = this.pool.getActive();
        const response = await ctrl.respond({ text });
        const seeds = new Map(response.seeds);
        const score = this._scoreReplyQuality(response.reply, seeds);
        return { file, score, words: tokenize(text), reply: response.reply };
    }

    async learn(cycles = 3) {
        const files = this._listTestFiles();
        const evals = [];
        for (const file of files) {
            evals.push(await this.evaluateOnce(file));
        }
        const features = evals.map((e) => this._buildFeatureForDoc(e.reply));
        const coords = this._reduceFeatures(features);
        const clusters = this._cluster(coords, Math.min(4, Math.max(2, Math.floor(Math.sqrt(files.length)) )));
        const grouped = new Map();
        for (let i = 0; i < evals.length; i++) {
            const g = clusters[i] ?? 0;
            if (!grouped.has(g)) grouped.set(g, []);
            grouped.get(g).push(evals[i]);
        }
        const clusterStats = Array.from(grouped.entries()).map(([gid, arr]) => ({ gid, avg: arr.reduce((a, b) => a + b.score, 0) / Math.max(1, arr.length), n: arr.length }));
        const baseAvg = evals.reduce((a, b) => a + b.score, 0) / Math.max(1, evals.length);
        const learnedAvg = clusterStats.reduce((a, b) => a + b.avg, 0) / Math.max(1, clusterStats.length);
        const evalStats = { baseAvg, learnedAvg, clusters: clusterStats };
        const newParams = this._adjustParams(evalStats);
        this.history.push({ ts: Date.now(), evals, evalStats, params: newParams });
        return { ok: true, evalStats, params: newParams };
    }

    latest() {
        return this.history[this.history.length - 1] || null;
    }
}

// 对抗学习模块：模拟扰动、生成对手样本、鲁棒性评估与修复
class AdversarialLearner {
    constructor(pool, {
        maxAdversaries = 64,
        noiseLevel = 0.2,
        synonymMap = null,
        attackRounds = 3,
        defenseRounds = 3,
        benchLimit = 50
    } = {}) {
        this.pool = pool;
        this.maxAdversaries = maxAdversaries;
        this.noiseLevel = noiseLevel;
        this.synonymMap = synonymMap || new Map();
        this.attackRounds = attackRounds;
        this.defenseRounds = defenseRounds;
        this.benchLimit = benchLimit;
        this.rng = safeRequire('seedrandom') ? safeRequire('seedrandom')('phoenix-adv') : Math.random;
        this.history = [];
        this.Matrix = getMatrix();
    }

    _perturbTokens(tokens) {
        const out = [];
        for (const t of tokens) {
            const r = (typeof this.rng === 'function') ? this.rng() : Math.random();
            if (r < this.noiseLevel / 2) {
                // 删除 token
                continue;
            }
            if (r < this.noiseLevel) {
                // 同义替换
                const syns = this.synonymMap.get(t);
                if (Array.isArray(syns) && syns.length) {
                    out.push(syns[Math.floor(r * syns.length)]);
                    continue;
                }
            }
            out.push(t);
        }
        // 随机插入噪声 token
        if ((typeof this.rng === 'function' ? this.rng() : Math.random()) < this.noiseLevel) {
            out.push('noise_token_' + Math.floor(((typeof this.rng === 'function') ? this.rng() : Math.random()) * 1000));
        }
        return out;
    }

    _generateAdversaries(text) {
        const base = tokenize(text);
        const adversaries = [];
        for (let i = 0; i < Math.min(8, Math.max(2, Math.floor(base.length / 3))); i++) {
            const mut = this._perturbTokens(base);
            adversaries.push(mut.join(' '));
        }
        return adversaries;
    }

    async _probe(text) {
        const ctrl = this.pool.getActive();
        const result = await ctrl.respond({ text });
        const seeds = new Map(result.seeds);
        const quality = this._qualityMetric(result.reply, seeds);
        return { text, reply: result.reply, quality, latency: result.latency, seeds };
    }

    _qualityMetric(reply, seeds) {
        // 与强化评分一致，但加入长度惩罚与重复惩罚
        const base = tokenize(reply);
        if (!base.length) return 0;
        const linkedHit = this._linkedCoverage(base, seeds);
        const uniq = new Set(base).size / Math.max(1, base.length);
        const lenPenalty = base.length > 50 ? 0.9 : 1.0;
        return (0.6 * linkedHit + 0.4 * uniq) * lenPenalty;
    }

    _linkedCoverage(words, seeds) {
        let coverage = 0;
        for (const [memeId] of seeds.entries()) {
            const linked = this.pool.getActive().runtime.kvm.getMemeWords(memeId);
            if (!linked || linked.size === 0) continue;
            let hit = 0;
            for (const w of words) {
                if (linked.has(w)) hit += 1;
            }
            coverage += hit / Math.max(1, linked.size);
        }
        return coverage / Math.max(1, seeds.size);
    }

    async attackAndDefend(samples) {
        const bench = samples.slice(0, this.benchLimit);
        const rounds = [];
        // 攻击阶段
        for (let r = 0; r < this.attackRounds; r++) {
            const results = [];
            for (const s of bench) {
                const advs = this._generateAdversaries(s);
                for (const a of advs) {
                    results.push(await this._probe(a));
                }
            }
            rounds.push({ phase: 'attack', results });
        }
        // 防御阶段：对低质量回复进行图修复（增加边权或添加桥接）
        const fixes = [];
        for (let d = 0; d < this.defenseRounds; d++) {
            for (const s of bench) {
                const res = await this._probe(s);
                if (res.quality < 0.25) {
                    const words = tokenize(res.reply).slice(0, 12);
                    const seeds = this.pool.getActive().runtime.mapWordsToMemes(words);
                    const ids = Array.from(seeds.keys());
                    for (let i = 0; i < ids.length - 1; i++) {
                        this.pool.getActive().runtime.graph.link(ids[i], ids[i + 1], 0.5, 0);
                    }
                    fixes.push({ sample: s, addedEdges: Math.max(0, ids.length - 1) });
                }
            }
            rounds.push({ phase: 'defense', fixes });
        }
        const summary = this._summarize(rounds);
        this.history.push({ ts: Date.now(), rounds, summary });
        return { ok: true, summary };
    }

    _summarize(rounds) {
        const stats = { attack: { n: 0, avgQuality: 0 }, defense: { fixes: 0 } };
        let qsum = 0;
        let qcnt = 0;
        for (const r of rounds) {
            if (r.phase === 'attack') {
                stats.attack.n += r.results.length;
                for (const item of r.results) {
                    qsum += item.quality;
                    qcnt += 1;
                }
            } else if (r.phase === 'defense') {
                stats.defense.fixes += (r.fixes || []).length;
            }
        }
        stats.attack.avgQuality = qcnt ? (qsum / qcnt) : 0;
        return stats;
    }

    latest() { return this.history[this.history.length - 1] || null; }
}


class GatewayServer {
    constructor(pool, shardManager, snapshotManager, rotation, redisSync, study, sparkArray, learners = {}) {
        this.pool = pool;
        this.shards = shardManager;
        this.snapshots = snapshotManager;
        this.rotation = rotation;
        this.redisSync = redisSync;
        this.study = study;
        this.spark = sparkArray || new SparkArray(pool, shardManager);
        this.rl = learners.rl || null;
        this.adv = learners.adv || null;
        this.rlDisabled = false;
        this.advDisabled = false;
        this.dialogLearningEnabled = true;
        this.dialogCounters = { total: 0, lastRL: 0, lastADV: 0 };
        this.dialogThresholds = { rlEvery: 20, advEvery: 30 };
        // 创建并启动 MemeBarrier（可通过 CLI 关闭）
        if (!CONFIG.disableBarrier) {
            try {
                this.barrier = new MemeBarrier(this.pool.getActive().runtime, {
                    maliciousThreshold: modelDefaults.maliciousThreshold
                });
                this.barrier.start();
            } catch (e) {
                console.warn('[Gateway] MemeBarrier init failed:', e.message);
            }
        } else {
            this.barrier = null;
            console.log('[Gateway] MemeBarrier disabled by config');
        }
        this.app = express();
        this.app.use(bodyParser.json({ limit: '10mb' }));
        this.app.use(bodyParser.urlencoded({ extended: true, limit: '10mb' }));

        // Auth: protect /api/* by default (can be disabled via env for dev).
        this.auth = {
            enabled: String(process.env.AI_AUTH_ENABLED || 'true').toLowerCase() !== 'false',
            jwtSecret: String(process.env.AI_AUTH_JWT_SECRET || process.env.AUTH_JWT_SECRET || 'dev-secret-change-me'),
            publicPaths: new Set([
                '/api/system/status'
            ])
        };
        this._setupAuthMiddleware();
        // Compatibility shim: Express 4.0 relies on deprecated res._headers (removed in modern Node).
        // We monkey-patch setHeader to maintain res._headers and res.headers so fresh(req,resHeaders) works.
        this.app.use((req, res, next) => {
            if (!res._headers) {
                res._headers = {}; // initialize
            }
            // Maintain a plain object reference for (res._headers || res.headers) legacy check.
            res.headers = res._headers;
            const originalSetHeader = res.setHeader;
            if (!res.__patchedSetHeader) {
                res.setHeader = function(name, value) {
                    originalSetHeader.call(this, name, value);
                    const lower = String(name).toLowerCase();
                    if (!this._headers) {
                        this._headers = {};
                    }
                    this._headers[lower] = value;
                    this.headers = this._headers;
                };
                res.__patchedSetHeader = true;
            }
            next();
        });
        // 根据 CLI 关闭学习模块（但允许运行时重新开启）
        this.rlDisabled = this.rlDisabled || CONFIG.disableLearning || CONFIG.disableRL;
        this.advDisabled = this.advDisabled || CONFIG.disableLearning || CONFIG.disableADV;
        // 如果 CLI 禁用学习，则默认关闭对话触发（可运行时打开）
        this.dialogLearningEnabled = !(CONFIG.disableLearning === true);
        this._setupRoutes();
    }

    _ensureRL() {
        if (this.rl) return this.rl;
        this.rl = new ReinforcementLearner(this.pool, { testsDir: path.join(__dirname, 'tests') });
        return this.rl;
    }

    _ensureADV() {
        if (this.adv) return this.adv;
        this.adv = new AdversarialLearner(this.pool, {});
        return this.adv;
    }

    _setupAuthMiddleware() {
        let jwt;
        try {
            jwt = require('jsonwebtoken');
        } catch (_e) {
            jwt = null;
        }

        const parseBearer = (req) => {
            const h = req.headers.authorization || '';
            const m = /^Bearer\s+(.+)$/i.exec(String(h));
            return m ? m[1] : '';
        };

        this.app.use((req, res, next) => {
            if (!this.auth.enabled) return next();
            if (req.method === 'OPTIONS') return next();
            if (!req.path.startsWith('/api/')) return next();
            if (this.auth.publicPaths.has(req.path)) return next();

            const token = parseBearer(req);
            if (!token) {
                res.status(401).json({ ok: false, error: 'unauthorized' });
                return;
            }
            if (!jwt) {
                res.status(500).json({ ok: false, error: 'auth-lib-missing' });
                return;
            }
            try {
                const payload = jwt.verify(token, this.auth.jwtSecret);
                req.user = payload;
                return next();
            } catch (e) {
                res.status(401).json({ ok: false, error: 'invalid-token', message: e.message });
            }
        });
    }

    _getRuntimeFeatureState() {
        return {
            memebarrier: {
                enabled: Boolean(this.barrier && this.barrier.running),
                available: !CONFIG.disableBarrier,
                threshold: this.barrier ? this.barrier.maliciousThreshold : modelDefaults.maliciousThreshold
            },
            learning: {
                // learning 是总开关：关闭时同时禁用 RL/ADV 的自动触发与端点
                enabled: !(this.rlDisabled && this.advDisabled),
                cliDisabled: Boolean(CONFIG.disableLearning)
            },
            rl: {
                enabled: !this.rlDisabled,
                cliDisabled: Boolean(CONFIG.disableLearning || CONFIG.disableRL)
            },
            adv: {
                enabled: !this.advDisabled,
                cliDisabled: Boolean(CONFIG.disableLearning || CONFIG.disableADV)
            },
            dialogLearning: {
                enabled: Boolean(this.dialogLearningEnabled)
            },
            dialogThresholds: { ...this.dialogThresholds },
            dialogCounters: { ...this.dialogCounters }
        };
    }

    _applyRuntimeFeaturePatch(patch = {}) {
        const out = { applied: {}, warnings: [] };
        // MemeBarrier
        if (typeof patch.memebarrierEnabled === 'boolean') {
            if (patch.memebarrierEnabled) {
                if (!this.barrier) {
                    this.barrier = new MemeBarrier(this.pool.getActive().runtime, {
                        maliciousThreshold: modelDefaults.maliciousThreshold
                    });
                }
                this.barrier.start();
            } else {
                if (this.barrier) this.barrier.stop();
            }
            out.applied.memebarrierEnabled = patch.memebarrierEnabled;
        }
        if (typeof patch.maliciousThreshold === 'number' && Number.isFinite(patch.maliciousThreshold)) {
            if (!this.barrier) {
                this.barrier = new MemeBarrier(this.pool.getActive().runtime, {
                    maliciousThreshold: patch.maliciousThreshold
                });
            }
            this.barrier.maliciousThreshold = patch.maliciousThreshold;
            out.applied.maliciousThreshold = patch.maliciousThreshold;
        }

        // Learning/RL/ADV
        // 注意：CLI disable 表示“启动默认禁用”，但允许运行时打开；若你希望完全锁死，可再加一个 CONFIG.lockRuntimeToggles。
        if (typeof patch.learningEnabled === 'boolean') {
            if (patch.learningEnabled) {
                // 仅解除总禁用：不强行打开 RL/ADV，由各自开关决定
                // 如果之前是因为 disableLearning 置为 true，这里也允许解除。
                if (CONFIG.disableLearning) {
                    out.warnings.push('learning was CLI-disabled; runtime override enabled');
                }
                // 不做事，交给 rlEnabled/advEnabled 或保持现状
            } else {
                this.rlDisabled = true;
                this.advDisabled = true;
                this.dialogLearningEnabled = false;
            }
            out.applied.learningEnabled = patch.learningEnabled;
        }
        if (typeof patch.rlEnabled === 'boolean') {
            if (patch.rlEnabled) {
                if (CONFIG.disableLearning || CONFIG.disableRL) {
                    out.warnings.push('rl was CLI-disabled; runtime override enabled');
                }
                this.rlDisabled = false;
            } else {
                this.rlDisabled = true;
            }
            out.applied.rlEnabled = patch.rlEnabled;
        }
        if (typeof patch.advEnabled === 'boolean') {
            if (patch.advEnabled) {
                if (CONFIG.disableLearning || CONFIG.disableADV) {
                    out.warnings.push('adv was CLI-disabled; runtime override enabled');
                }
                this.advDisabled = false;
            } else {
                this.advDisabled = true;
            }
            out.applied.advEnabled = patch.advEnabled;
        }

        if (typeof patch.dialogLearningEnabled === 'boolean') {
            this.dialogLearningEnabled = patch.dialogLearningEnabled;
            out.applied.dialogLearningEnabled = patch.dialogLearningEnabled;
        }

        // Dialog thresholds
        if (typeof patch.rlEvery === 'number' && Number.isFinite(patch.rlEvery) && patch.rlEvery > 0) {
            this.dialogThresholds.rlEvery = patch.rlEvery;
            out.applied.rlEvery = patch.rlEvery;
        }
        if (typeof patch.advEvery === 'number' && Number.isFinite(patch.advEvery) && patch.advEvery > 0) {
            this.dialogThresholds.advEvery = patch.advEvery;
            out.applied.advEvery = patch.advEvery;
        }
        return out;
    }

    _setupRoutes() {
        // Main AI 进程不再托管前端；根路径返回提示。
        this.app.get('/', (req, res) => {
            res.status(404).send('UI moved to auth_frontend_server.cjs (default :5081)');
        });

        // 前端托管已迁移到独立进程（auth_frontend_server.cjs）。
        // 这里保留纯 API（/api、/robots 等）以减少主 AI 进程职责。
        this.app.post('/api/chat', async (req, res) => {
            try {
                const controller = this.pool.getActive();
                const result = await controller.respond(req.body || {});
                res.json({ ok: true, result });
                this._onDialogCompleted(result, req.body || {});
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });

        // 运行时配置/开关（React 控制台使用）
        this.app.get('/api/runtime/features', (req, res) => {
            res.json({ ok: true, features: this._getRuntimeFeatureState() });
        });
        this.app.patch('/api/runtime/features', (req, res) => {
            try {
                const result = this._applyRuntimeFeaturePatch(req.body || {});
                res.json({ ok: true, result, features: this._getRuntimeFeatureState() });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });

        this.app.get('/api/study/status', (req, res) => {
            const q = this.study?.queue?.length ?? 0;
            res.json({
                ok: true,
                running: Boolean(this.study?.running),
                queue: q,
                metrics: this.study?.metrics || null
            });
        });

        this.app.post('/api/learn/dialog/reset', (req, res) => {
            this.dialogCounters = { total: 0, lastRL: 0, lastADV: 0 };
            res.json({ ok: true, dialogCounters: this.dialogCounters });
        });
        this.app.post('/api/array/chat', async (req, res) => {
            try {
                const payload = req.body || {};
                if (!payload.text) {
                    res.status(400).json({ ok: false, error: 'text required' });
                    return;
                }
        // Reinforcement Learning endpoints
        this.app.post('/api/learn/reinforce', async (req, res) => {
            try {
                if (this.rlDisabled) {
                    res.status(503).json({ ok: false, error: 'rl-disabled' });
                    return;
                }
                const cycles = Number(req.body?.cycles ?? 3) || 3;
                const out = await this._ensureRL().learn(cycles);
                res.json({ ok: true, result: out });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.get('/api/learn/reinforce/latest', (req, res) => {
            res.json({ ok: true, latest: this.rl ? this.rl.latest() : null });
        });
        // Adversarial Learning endpoints
        this.app.post('/api/learn/adversarial', async (req, res) => {
            try {
                if (this.advDisabled) {
                    res.status(503).json({ ok: false, error: 'adv-disabled' });
                    return;
                }
                const samples = Array.isArray(req.body?.samples) ? req.body.samples : [];
                if (!samples.length) {
                    res.status(400).json({ ok: false, error: 'samples required' });
                    return;
                }
                const out = await this._ensureADV().attackAndDefend(samples);
                res.json({ ok: true, result: out });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.get('/api/learn/adversarial/latest', (req, res) => {
            res.json({ ok: true, latest: this.adv ? this.adv.latest() : null });
        });
        this.app.post('/api/learn/thresholds', (req, res) => {
            const { rlEvery, advEvery } = req.body || {};
            if (Number.isFinite(rlEvery) && rlEvery > 0) this.dialogThresholds.rlEvery = rlEvery;
            if (Number.isFinite(advEvery) && advEvery > 0) this.dialogThresholds.advEvery = advEvery;
            res.json({ ok: true, thresholds: this.dialogThresholds });
        });
                const opts = payload.options || {};
                const bigRounds = Number(opts.bigRounds ?? opts.bigSparkRounds ?? 1) || 1;
                const result = bigRounds > 1
                    ? await this.spark.dispatchBig(payload, opts)
                    : await this.spark.dispatch(payload, opts);
                res.json({ ok: true, result });
                const agg = result?.aggregate;
                const dialog = agg ? { reply: agg.reply, latency: agg.latency, sessionId: agg.sessionId, seeds: [] } : null;
                this._onDialogCompleted(dialog, payload || {});
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.get('/api/model/params', (req, res) => {
            res.json({ ok: true, params: this.pool.getActive().runtime.cloneParams() });
        });
        this.app.post('/api/model/params', (req, res) => {
            const params = req.body || {};
            this.pool.getActive().applyParams(params);
            res.json({ ok: true, params: this.pool.getActive().runtime.cloneParams() });
        });
        this.app.post('/api/model/params/reset', (req, res) => {
            this.pool.getActive().applyParams({ ...modelDefaults });
            res.json({ ok: true, params: this.pool.getActive().runtime.cloneParams() });
        });
        this.app.get('/api/array/layers', (req, res) => {
            res.json({ ok: true, layers: this.spark.getLayers(), history: (this.spark.history || []).slice(-20) });
        });
        this.app.post('/api/array/layers', (req, res) => {
            try {
                this.spark.updateLayers(req.body?.layers);
                res.json({ ok: true, layers: this.spark.getLayers() });
            } catch (err) {
                res.status(400).json({ ok: false, error: err.message });
            }
        });
        this.app.get('/api/array/history', (req, res) => {
            res.json({ ok: true, history: (this.spark.history || []).slice(-20) });
        });
        this.app.get('/api/snapshots', (req, res) => {
            res.json({ ok: true, list: this.snapshots.list() });
        });
        this.app.post('/api/snapshots/create', async (req, res) => {
            const name = req.body?.name || 'manual';
            const file = await this.snapshots.create(name);
            res.json({ ok: true, file: path.basename(file) });
        });
        this.app.post('/api/snapshots/restore/:id', async (req, res) => {
            try {
                await this.snapshots.restore(req.params.id);
                res.json({ ok: true });
            } catch (err) {
                res.status(404).json({ ok: false, error: err.message });
            }
        });
        this.app.delete('/api/snapshots/:id', (req, res) => {
            this.snapshots.delete(req.params.id);
            res.json({ ok: true });
        });
        this.app.get('/api/system/status', (req, res) => {
            const uptime = process.uptime();
            res.json({
                ok: true,
                uptime,
                load: os.loadavg(),
                memory: process.memoryUsage(),
                controllers: this.pool.listMetrics(),
                groups: typeof this.pool.listGroupIds === 'function'
                    ? this.pool.listGroupIds().map((gid) => ({ gid, controllers: this.pool.listControllersInGroup(gid) }))
                    : [],
                rotation: { running: this.rotation.running, cycleMs: this.rotation.cycleMs },
                redis: { channel: this.redisSync.channel }
            });
        });

        // 只读系统配置（组数/组大小等；修改需要重启进程）
        this.app.get('/api/system/config', (req, res) => {
            res.json({
                ok: true,
                config: {
                    groupCount: CONFIG.groupCount,
                    groupSize: CONFIG.groupSize,
                    sparkNumAI: CONFIG.sparkNumAI,
                    sparkBudget: CONFIG.sparkBudget,
                    groupIds: typeof this.pool.listGroupIds === 'function' ? this.pool.listGroupIds() : [],
                    gatewayHost: CONFIG.gatewayHost,
                    portGateway: CONFIG.portGateway,
                    portStudy: CONFIG.portStudy
                }
            });
        });

        // 组信息：用于前端展示与按组操作
        this.app.get('/api/groups', (req, res) => {
            const groupIds = typeof this.pool.listGroupIds === 'function' ? this.pool.listGroupIds() : [];
            res.json({
                ok: true,
                groupSize: CONFIG.groupSize,
                groupCount: CONFIG.groupCount,
                groups: groupIds.map((gid) => ({ gid, controllers: this.pool.listControllersInGroup(gid) }))
            });
        });

        this.app.get('/api/groups/:gid/metrics', (req, res) => {
            const gid = String(req.params.gid || '').trim();
            const list = typeof this.pool.listControllersInGroup === 'function' ? this.pool.listControllersInGroup(gid) : [];
            if (!list.length) {
                res.status(404).json({ ok: false, error: 'group-not-found' });
                return;
            }
            const metrics = list
                .map((name) => this.pool.getByName(name))
                .filter(Boolean)
                .map((ctrl) => ctrl.metrics());
            res.json({ ok: true, gid, controllers: metrics });
        });
        this.app.get('/api/system/ai/:name', (req, res) => {
            const ctrl = this.pool.getByName(req.params.name);
            if (!ctrl) {
                res.status(404).json({ ok: false, error: 'Controller not found' });
                return;
            }
            res.json({ ok: true, controller: ctrl.metrics() });
        });
        this.app.get('/api/shards', (req, res) => {
            res.json({ ok: true, shards: this.shards.metrics() });
        });
        this.app.post('/api/study/enqueue', (req, res) => {
            const { text } = req.body || {};
            if (!text) {
                res.status(400).json({ ok: false, error: 'text required' });
                return;
            }
            this.study.enqueueDocument({ text, queuedAt: Date.now() });
            res.json({ ok: true });
        });
        this.app.post('/api/corpus/ingest', async (req, res) => {
            const doc = req.body || {};
            if (!doc.text) {
                res.status(400).json({ ok: false, error: 'text required' });
                return;
            }
            try {
                const results = await this.pool.ingestDocument(doc);
                if (doc.enqueueStudy && this.study) {
                    this.study.enqueueDocument(doc);
                }
                res.json({ ok: true, results });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.post('/api/corpus/forget', async (req, res) => {
            try {
                const results = await this.pool.forgetMemes(req.body || {});
                res.json({ ok: true, results });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.post('/api/corpus/online', async (req, res) => {
            const { query, tokens, options } = req.body || {};
            if (!query && (!Array.isArray(tokens) || tokens.length === 0)) {
                res.status(400).json({ ok: false, error: 'query or tokens required' });
                return;
            }
            try {
                const result = await this.pool.onlineResearch(query || tokens, options || {});
                res.json({ ok: true, result });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });

        // 站内递归抓取：同域 HTML + 可选 PDF 文本抽取（仅用于你有授权的网站）
        this.app.post('/api/corpus/crawl', async (req, res) => {
            try {
                const { startUrl, options, ingest, groupId, source } = req.body || {};
                const url = String(startUrl || '').trim();
                if (!url) {
                    res.status(400).json({ ok: false, error: 'startUrl required' });
                    return;
                }
                const runtime = this.pool.getActive().runtime;
                const result = await runtime.onlineLookup(url, { mode: 'crawl', crawl: { startUrl: url, ...(options || {}) } });

                let ingested = null;
                if (ingest) {
                    const docs = (result?.pages || [])
                        .filter((p) => p && p.text && String(p.text).trim())
                        .map((p) => ({
                            text: String(p.text),
                            source: String(source || p.url || 'crawl')
                        }));
                    if (docs.length) {
                        const details = [];
                        for (const doc of docs) {
                            const r = groupId ? await this.pool.ingestDocumentToGroup(String(groupId), doc) : await this.pool.ingestDocument(doc);
                            details.push({ source: doc.source, result: r });
                        }
                        ingested = { docs: docs.length, details: details.slice(0, 20) };
                    } else {
                        ingested = { docs: 0 };
                    }
                }

                res.json({ ok: true, result, ingested });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });

        // 在线搜索配置（运行时开关 + endpoint 库）
        this.app.get('/api/search/config', (req, res) => {
            try {
                const runtime = this.pool.getActive().runtime;
                res.json({ ok: true, config: runtime.getSearchConfig() });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.put('/api/search/config', (req, res) => {
            try {
                const runtime = this.pool.getActive().runtime;
                const next = runtime.setSearchConfig(req.body || {});
                res.json({ ok: true, config: next });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.post('/api/search/endpoints/add', (req, res) => {
            try {
                const url = String(req.body?.url || '').trim();
                if (!url) {
                    res.status(400).json({ ok: false, error: 'url required' });
                    return;
                }
                const runtime = this.pool.getActive().runtime;
                const cfg = runtime.getSearchConfig();
                const endpoints = Array.from(new Set([...(cfg.endpoints || []), url]));
                const next = runtime.setSearchConfig({ endpoints });
                res.json({ ok: true, config: next });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.post('/api/search/endpoints/remove', (req, res) => {
            try {
                const url = String(req.body?.url || '').trim();
                if (!url) {
                    res.status(400).json({ ok: false, error: 'url required' });
                    return;
                }
                const runtime = this.pool.getActive().runtime;
                const cfg = runtime.getSearchConfig();
                const endpoints = (cfg.endpoints || []).filter((x) => x !== url);
                const nextPatch = { endpoints };
                if (cfg.active === url) {
                    nextPatch.active = endpoints[0] || '';
                }
                const next = runtime.setSearchConfig(nextPatch);
                res.json({ ok: true, config: next });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });

        // Tests 用例：运行时新增/列出/刷新（用于 RL 的 testsDir 词表刷新）
        this.app.get('/api/tests/list', (req, res) => {
            try {
                const runtime = this.pool.getActive().runtime;
                const testsDir = runtime?.config?.testsDir || path.join(__dirname, 'tests');
                let files = [];
                if (fs.existsSync(testsDir)) {
                    files = fs.readdirSync(testsDir)
                        .filter((f) => /\.txt$/i.test(f))
                        .sort();
                }
                res.json({ ok: true, directory: testsDir, files });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });

        this.app.post('/api/tests/case', (req, res) => {
            try {
                const nameRaw = String(req.body?.name || '').trim();
                const content = String(req.body?.content || '');
                if (!nameRaw) {
                    res.status(400).json({ ok: false, error: 'name required' });
                    return;
                }
                const safeName = nameRaw.replace(/[^a-zA-Z0-9._-]+/g, '_');
                const filename = safeName.toLowerCase().endsWith('.txt') ? safeName : `${safeName}.txt`;
                const runtime = this.pool.getActive().runtime;
                const testsDir = runtime?.config?.testsDir || path.join(__dirname, 'tests');
                fs.mkdirSync(testsDir, { recursive: true });
                const filePath = path.join(testsDir, filename);
                fs.writeFileSync(filePath, content, 'utf8');
                res.json({ ok: true, file: filename, path: filePath });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });

        this.app.post('/api/tests/refresh', async (req, res) => {
            try {
                // 目标：让 RL 模块在运行时读取最新 tests 词表
                // 做法：调用 rl.refreshTests()（若存在），否则尝试重建 RL 实例
                const testsDir = this.pool.getActive().runtime?.config?.testsDir || path.join(__dirname, 'tests');
                let refreshed = false;
                if (this.rl && typeof this.rl.refreshTests === 'function') {
                    await Promise.resolve(this.rl.refreshTests({ testsDir }));
                    refreshed = true;
                } else if (this.rl && typeof this.rl.setTestsDir === 'function') {
                    await Promise.resolve(this.rl.setTestsDir(testsDir));
                    refreshed = true;
                } else {
                    // 最保守方案：替换 RL learner（不影响主 runtime）
                    this.rl = new ReinforcementLearner(this.pool, { testsDir });
                    // 如果当前是启用状态，保持启用
                    refreshed = true;
                }
                res.json({ ok: true, refreshed, testsDir });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        // 反序列化导出：将当前图窗口反序列化为 Graph 并写入文件
        this.app.post('/api/export/graph', (req, res) => {
            try {
                const { seeds, radius, file } = req.body || {};
                const runtime = this.pool.getActive().runtime;
                const outFile = runtime.exportGraphToFile({ seeds, radius, file });
                // 返回文件内容，便于直接复制到 Go 侧
                const content = fs.readFileSync(outFile, 'utf8');
                res.json({ ok: true, file: path.basename(outFile), path: outFile, content });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });

        // 按组导出图：用于 BigSparkArray 时代分别取各工作组的图结构
        this.app.post('/api/export/graph/group', (req, res) => {
            try {
                const { groupId, seeds, radius, file } = req.body || {};
                const gid = String(groupId || '').trim();
                if (!gid) {
                    res.status(400).json({ ok: false, error: 'groupId required' });
                    return;
                }
                const list = typeof this.pool.listControllersInGroup === 'function' ? this.pool.listControllersInGroup(gid) : [];
                if (!list.length) {
                    res.status(404).json({ ok: false, error: 'group-not-found' });
                    return;
                }
                const ctrl = this.pool.getByName(list[0]);
                if (!ctrl) {
                    res.status(404).json({ ok: false, error: 'controller-not-found' });
                    return;
                }
                const runtime = ctrl.runtime;
                const outFile = runtime.exportGraphToFile({ seeds, radius, file });
                const content = fs.readFileSync(outFile, 'utf8');
                res.json({ ok: true, groupId: gid, file: path.basename(outFile), path: outFile, content });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });

        // Robots：运行时触发重训（重新 ingest robots 语料，并可选 enqueueStudy）
        this.app.post('/api/robots/retrain', async (req, res) => {
            try {
                const options = req.body || {};
                const runtime = this.pool.getActive().runtime;
                const docs = runtime.collectRobotsDocuments({
                    limit: options.limit,
                    offset: options.offset,
                    shuffle: Boolean(options.shuffle),
                    files: Array.isArray(options.files)
                        ? options.files
                        : (typeof options.files === 'string' && options.files.trim() ? [options.files.trim()] : undefined)
                });
                if (!docs.length) {
                    res.status(404).json({ ok: false, error: 'no-documents' });
                    return;
                }
                let ingested = 0;
                let memes = 0;
                let edges = 0;
                let failedControllers = 0;
                const groups = typeof this.pool.listGroupIds === 'function' ? this.pool.listGroupIds() : [];
                for (const doc of docs) {
                    // 默认按组分片：每个工作组 ingest 不同语料
                    const key = String(doc?.source || doc?.file || doc?.id || '') + '|' + String(doc?.text || '').slice(0, 256);
                    const gi = groups.length ? (hashStrSimple(key) % groups.length) : 0;
                    const targetGroup = groups[gi] || groups[0];
                    const results = targetGroup ? await this.pool.ingestDocumentToGroup(targetGroup, doc) : await this.pool.ingestDocument(doc);
                    ingested += 1;
                    const list = Array.isArray(results) ? results : [results];
                    for (const item of list) {
                        if (item && item.ok !== false) {
                            memes += Number(item.memes || 0) || 0;
                            edges += Number(item.edges || 0) || 0;
                        } else {
                            failedControllers += 1;
                        }
                    }
                    if (options.enqueueStudy && this.study) {
                        this.study.enqueueDocument(doc);
                    }
                }
                res.json({ ok: true, ingested, memes, edges, failedControllers });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.get('/robots/list', (req, res) => {
            try {
                const runtime = this.pool.getActive().runtime;
        // MemeBarrier 控制与统计端点
        this.app.post('/api/memebarrier/start', (req, res) => {
            try {
                if (!this.barrier) {
                    this.barrier = new MemeBarrier(this.pool.getActive().runtime, {
                        maliciousThreshold: (req.body?.maliciousThreshold ?? modelDefaults.maliciousThreshold)
                    });
                }
                if (req.body && typeof req.body.maliciousThreshold === 'number') {
                    this.barrier.maliciousThreshold = req.body.maliciousThreshold;
                }
                this.barrier.start();
                res.json({ ok: true, running: true, threshold: this.barrier.maliciousThreshold });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.post('/api/memebarrier/stop', (req, res) => {
            try {
                if (this.barrier) {
                    this.barrier.stop();
                }
                res.json({ ok: true, running: false });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.get('/api/memebarrier/stats', (req, res) => {
            try {
                const stats = this.barrier ? this.barrier.getStats() : null;
                res.json({ ok: true, stats });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
                res.json({
                    ok: true,
                    directory: runtime.config?.robotsDir,
                    files: runtime.listRobotsFiles()
                });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
        this.app.post('/robots/ingest', async (req, res) => {
            const options = req.body || {};
            try {
                const runtime = this.pool.getActive().runtime;
                const fileFilter = Array.isArray(options.files)
                    ? options.files
                    : (typeof options.files === 'string' && options.files.trim() ? [options.files.trim()] : undefined);
                const docs = runtime.collectRobotsDocuments({
                    limit: options.limit,
                    offset: options.offset,
                    shuffle: Boolean(options.shuffle),
                    files: fileFilter
                });
                if (!docs.length) {
                    res.status(404).json({ ok: false, error: 'no-documents' });
                    return;
                }
                const summary = [];
                const groups = typeof this.pool.listGroupIds === 'function' ? this.pool.listGroupIds() : [];
                for (const doc of docs) {
                    const key = String(doc?.source || doc?.file || doc?.id || '') + '|' + String(doc?.text || '').slice(0, 256);
                    const gi = groups.length ? (hashStrSimple(key) % groups.length) : 0;
                    const targetGroup = groups[gi] || groups[0];
                    const results = targetGroup ? await this.pool.ingestDocumentToGroup(targetGroup, doc) : await this.pool.ingestDocument(doc);
                    const list = Array.isArray(results) ? results : [results];
                    const aggregated = list.reduce(
                        (acc, item) => {
                            if (item && item.ok !== false) {
                                const memes = Number(item.memes || 0);
                                const edges = Number(item.edges || 0);
                                acc.memes += Number.isFinite(memes) ? memes : 0;
                                acc.edges += Number.isFinite(edges) ? edges : 0;
                            } else {
                                acc.failed += 1;
                            }
                            return acc;
                        },
                        { memes: 0, edges: 0, failed: 0 }
                    );
                    summary.push({
                        id: doc.id,
                        source: doc.source,
                        file: doc.file,
                        tokens: doc.tokens.length,
                        memes: aggregated.memes,
                        edges: aggregated.edges,
                        failedControllers: aggregated.failed
                    });
                    if (options.enqueueStudy && this.study) {
                        this.study.enqueueDocument(doc);
                    }
                }
                res.json({
                    ok: true,
                    ingested: summary.length,
                    files: Array.from(new Set(summary.map((item) => item.file))),
                    summary
                });
            } catch (err) {
                res.status(500).json({ ok: false, error: err.message });
            }
        });
    }

    _onDialogCompleted(result, payload) {
        try {
            this.dialogCounters.total += 1;
            const total = this.dialogCounters.total;
            if (!this.dialogLearningEnabled) {
                return;
            }

            // 反接学习：把本轮对话沉淀为“模因层 -> 表层答案”的映射与可检索记忆
            try {
                const runtime = this.pool?.getActive?.()?.runtime;
                if (runtime && typeof runtime.learnFromDialog === 'function') {
                    runtime.learnFromDialog({ payload, result });
                }
            } catch (e) {
                // 学习失败不影响主流程
                console.warn('[Learn] surface/dialog memory update failed:', e.message);
            }

            if (!this.rlDisabled && (total - this.dialogCounters.lastRL >= this.dialogThresholds.rlEvery)) {
                this.dialogCounters.lastRL = total;
                Promise.resolve().then(() => this._ensureRL().learn(1)).catch((e) => console.warn('[Learn] RL trigger failed:', e.message));
            }
            if (!this.advDisabled && (total - this.dialogCounters.lastADV >= this.dialogThresholds.advEvery)) {
                this.dialogCounters.lastADV = total;
                const text = typeof payload?.text === 'string' ? payload.text : (Array.isArray(payload?.tokens) ? payload.tokens.join(' ') : '');
                const samples = [];
                if (text && text.trim()) samples.push(text.trim());
                if (result?.reply && typeof result.reply === 'string') samples.push(result.reply);
                if (samples.length) {
                    Promise.resolve().then(() => this._ensureADV().attackAndDefend(samples)).catch((e) => console.warn('[Learn] ADV trigger failed:', e.message));
                }
            }
        } catch (e) {
            console.warn('[Learn] dialog hook error:', e.message);
        }
    }

    listen(port, host = '127.0.0.1') {
        this.app.listen(port, host, () => {
            console.log(`[Gateway] listening on ${host}:${port}`);
        });
    }
}

const bootstrap = async () => {
    console.log('checkpoint1');
    let pool;
    let rotation;
    let redisSync;
    let study;
    if (CONFIG.groupProc) {
        pool = new GroupProcessPool({ config: CONFIG });
        rotation = { running: false, cycleMs: 0, start() {}, stop() {} };
        redisSync = { channel: CONFIG.redisChannel, start: async () => {}, stop: () => {} };
        study = { running: false, queue: [], metrics: null, enqueueDocument: () => {} };
        console.log('[Bootstrap] group-proc enabled: one process per group');
    } else {
        const kvmStore = new LmdbStore({ name: 'kvm', rootDir: CONFIG.lmdbRoot, mapSizeBytes: CONFIG.lmdbMapSizeBytes });
        const memeStore = new LmdbStore({ name: 'meme_graph', rootDir: CONFIG.lmdbRoot, mapSizeBytes: CONFIG.lmdbMapSizeBytes });
        const sessionStore = new LmdbStore({ name: 'session', rootDir: CONFIG.lmdbRoot, mapSizeBytes: CONFIG.lmdbMapSizeBytes });
        pool = new ControllerPool({ kvmStore, memeStore, sessionStore, config: CONFIG });
        rotation = new RotationManager(pool, {});
        rotation.start();
        redisSync = new RedisSynchronizer(pool, { url: CONFIG.redisUrl, channel: CONFIG.redisChannel });
        try {
            await redisSync.start();
        } catch (err) {
            console.warn('[Bootstrap] Redis unavailable, continuing without sync.');
        }
        study = new StudyEngine(pool, redisSync);
        study.start();
    }
    const snapshots = new SnapshotManager(pool.getActive().runtime, CONFIG.snapshotDir);
    const shards = new ShardManager(pool);
    const spark = new BigSparkArray(pool, shards, {
        groupIds: pool.listGroupIds(),
        groupOptions: {
            numAI: CONFIG.sparkNumAI,
            budget: CONFIG.sparkBudget
        }
    });
    // Try auto-restore latest snapshot to skip warmup/pretraining
    let __restoredFromSnapshot = false;
    try {
        const list = snapshots.list().sort((a, b) => b.localeCompare(a));
        if (list.length > 0) {
            const restoredSnapshot = await snapshots.restore(list[0]);
            __restoredFromSnapshot = true;
            console.log(`[Bootstrap] Restored latest snapshot: ${list[0]}`);
            if (CONFIG.groupProc && typeof pool.applySnapshotAll === 'function') {
                try {
                    await pool.applySnapshotAll(restoredSnapshot);
                    console.log('[Bootstrap] Snapshot broadcasted to all groups (group-proc)');
                } catch (e) {
                    console.warn('[Bootstrap] Snapshot broadcast failed:', e.message);
                }
            }
            if (CONFIG.syncStandbyOnBoot) {
                try {
                    await pool.standby.applySnapshot(restoredSnapshot);
                    await pool.validation.applySnapshot(restoredSnapshot);
                    console.log('[Bootstrap] Standby/validation synced from snapshot');
                } catch (e) {
                    console.warn('[Bootstrap] Standby/validation sync skipped:', e.message);
                }
            } else {
                console.log('[Bootstrap] Standby/validation sync skipped (fast-boot)');
            }
        }
    } catch (err) {
        console.warn('[Bootstrap] Snapshot restore skipped:', err.message);
    }
    if (CONFIG.robotsAutoload && CONFIG.robotsWarmupLimit > 0 && !__restoredFromSnapshot) {
        try {
            const preloadDocs = await Promise.resolve(pool.getActive().runtime.collectRobotsDocuments({
                limit: CONFIG.robotsWarmupLimit,
                shuffle: Boolean(CONFIG.robotsWarmupShuffle)
            }));
            if (preloadDocs.length) {
                console.log(`[Bootstrap] Preloading ${preloadDocs.length} robots documents...`);
                const groups = pool.listGroupIds();
                for (let i = 0; i < preloadDocs.length; i++) {
                    const doc = preloadDocs[i];
                    const key = String(doc?.source || doc?.file || doc?.id || '') + '|' + String(doc?.text || '').slice(0, 256);
                    const idx = groups.length ? (hashStrSimple(key) % groups.length) : 0;
                    const targetGroup = groups[idx] || groups[0] || 'G1';
                    await pool.ingestDocumentToGroup(targetGroup, { ...doc, source: doc?.source || `robots:${doc?.file || 'unknown'}` });
                }
                console.log('[Bootstrap] Robots corpus preload completed.');
            } else {
                console.log('[Bootstrap] No robots documents found for preload.');
            }
        } catch (err) {
            console.warn('[Bootstrap] Robots preload skipped:', err.message);
        }
    }

    // 额外：将 tests 目录用例按哈希分片到不同 AI，形成差异化“训练集”
    if (CONFIG.testsAutoload) {
        try {
            const testsDir = path.join(__dirname, 'tests');
            if (fs.existsSync(testsDir)) {
                const files = fs.readdirSync(testsDir).filter((f) => /\.txt$/i.test(f));
                if (files.length) {
                    console.log(`[Bootstrap] Preloading tests corpus (${files.length} files)...`);
                    const groups = pool.listGroupIds();
                    for (const f of files) {
                        const full = path.join(testsDir, f);
                        const text = fs.readFileSync(full, 'utf8');
                        const key = `tests:${f}`;
                        const idx = groups.length ? (hashStrSimple(key) % groups.length) : 0;
                        const targetGroup = groups[idx] || groups[0] || 'G1';
                        await pool.ingestDocumentToGroup(targetGroup, { text, source: key });
                    }
                    console.log(`[Bootstrap] Sharded tests corpus into ${groups.length} groups.`);
                }
            }
        } catch (err) {
            console.warn('[Bootstrap] Tests sharded preload skipped:', err.message);
        }
    } else {
        console.log('[Bootstrap] Tests preload skipped (fast-boot)');
    }
    // 学习模块改为网关侧按需创建（降低启动时间与内存峰值）
    const gateway = new GatewayServer(pool, shards, snapshots, rotation, redisSync, study, spark, { rl: null, adv: null });
    gateway.listen(CONFIG.portGateway, CONFIG.gatewayHost);
    // 可选预热：默认关闭；需要时加 --learning-warmup=true
    if (CONFIG.learningWarmup) {
        (async () => {
            if (!CONFIG.disableLearning && !CONFIG.disableRL) {
                try {
                    await gateway._ensureRL().learn(1);
                } catch (e) {
                    console.warn('[Bootstrap] RL warmup failed:', e.message);
                    try { gateway.rlDisabled = true; } catch (_) {}
                }
            }

            if (!CONFIG.disableLearning && !CONFIG.disableADV) {
                try {
                    const runtime = pool.getActive().runtime;
                    const docs = await Promise.resolve(runtime.collectRobotsDocuments({ limit: 3, shuffle: true }));
                    const samples = (Array.isArray(docs) ? docs : []).map((d) => d.text).filter(Boolean).slice(0, 3);
                    if (samples.length) {
                        await gateway._ensureADV().attackAndDefend(samples);
                    }
                } catch (e) {
                    console.warn('[Bootstrap] Adversarial warmup failed:', e.message);
                }
            }
        })();
    }
    process.on('SIGINT', async () => {
        console.log('Received SIGINT, saving snapshot...');
        try {
            const file = await snapshots.create('autosave');
            console.log(`Snapshot saved: ${path.basename(file)}`);
        } catch (err) {
            console.warn('[Shutdown] Failed to save snapshot:', err.message);
        }
        try {
            if (__inferencePool) {
                await __inferencePool.terminate();
            }
        } catch (_e) {}
        try {
            if (CONFIG.groupProc && pool && typeof pool.shutdown === 'function') {
                pool.shutdown();
            }
        } catch (_e) {}
        process.exit(0);
    });
};

if (require.main === module) {
    bootstrap().catch((err) => {
        console.error('Fatal error during bootstrap:', err);
        process.exit(1);
    });
}

module.exports = {
    bootstrap,
    CONFIG,
    MODEL_DEFAULTS: modelDefaults,
    BUILTIN_ACTIVATION_TYPES: Object.keys(BuiltinActivations),
    BUILTIN_TRANSFER_TYPES: Object.keys(BuiltinTransfers)
};
