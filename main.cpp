#include <drogon/drogon.h>
#include <nlohmann/json.hpp>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/nlohmann-json/traits.h>
#include <json/json.h>
#include <trantor/utils/Utilities.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <deque>
#include <future>
#include <iomanip>
#include <functional>
#include <iostream>
#include <list>
#include <limits>
#include <map>
#include <mutex>
#include <queue>
#include <optional>
#include <random>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "transformer.hpp"
#include "transformer_main.cpp"
#include "addon.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <psapi.h>
#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "psapi.lib")
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <signal.h>
#include <unistd.h>
#include <sys/resource.h>
#endif

#ifdef HAVE_REDIS
#include <sw/redis++/redis++.h>
#endif

#ifdef HAVE_LMDB
#include <lmdb.h>
#endif

#ifdef HAVE_GUMBO
#include <gumbo.h>
#endif

#ifdef HAVE_POPPLER
#include <poppler-document.h>
#include <poppler-page.h>
#endif

#ifdef HAVE_CURL
#include <curl/curl.h>
#endif

using json = nlohmann::json;
namespace fs = std::filesystem;

static json fromJsoncpp(const Json::Value &v) {
    switch (v.type()) {
    case Json::nullValue: return nullptr;
    case Json::intValue: return (int64_t)v.asInt64();
    case Json::uintValue: return (uint64_t)v.asUInt64();
    case Json::realValue: return v.asDouble();
    case Json::stringValue: return v.asString();
    case Json::booleanValue: return v.asBool();
    case Json::arrayValue: {
        json out = json::array();
        for (const auto &item : v) out.push_back(fromJsoncpp(item));
        return out;
    }
    case Json::objectValue: {
        json out = json::object();
        for (auto it = v.begin(); it != v.end(); ++it) {
            out[it.name()] = fromJsoncpp(*it);
        }
        return out;
    }
    default:
        return nullptr;
    }
}

static Json::Value toJsoncpp(const json &v) {
    if (v.is_null()) return Json::Value();
    if (v.is_boolean()) return Json::Value(v.get<bool>());
    if (v.is_number_integer()) return Json::Value((Json::Int64)v.get<long long>());
    if (v.is_number_unsigned()) return Json::Value((Json::UInt64)v.get<unsigned long long>());
    if (v.is_number_float()) return Json::Value(v.get<double>());
    if (v.is_string()) return Json::Value(v.get<std::string>());
    if (v.is_array()) {
        Json::Value arr(Json::arrayValue);
        for (const auto &item : v) arr.append(toJsoncpp(item));
        return arr;
    }
    if (v.is_object()) {
        Json::Value obj(Json::objectValue);
        for (auto it = v.begin(); it != v.end(); ++it) obj[it.key()] = toJsoncpp(it.value());
        return obj;
    }
    return Json::Value();
}

static std::string g_selfPath;

static std::string getEnv(const std::string &key, const std::string &fallback = "") {
    const char *v = std::getenv(key.c_str());
    if (!v) return fallback;
    return std::string(v);
}

static bool boolFrom(const std::string &value, bool fallback = true) {
    std::string v = value;
    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
    return !(v == "0" || v == "false" || v == "off" || v == "no");
}

static std::string trimCopy(const std::string &s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

static bool parseNumberLikeJs(const std::string &s, double &out) {
    std::string t = trimCopy(s);
    if (t.empty()) {
        out = 0.0;
        return true;
    }
    if (t == "NaN" || t == "+NaN" || t == "-NaN") {
        out = std::numeric_limits<double>::quiet_NaN();
        return true;
    }
    if (t == "Infinity" || t == "+Infinity") {
        out = std::numeric_limits<double>::infinity();
        return true;
    }
    if (t == "-Infinity") {
        out = -std::numeric_limits<double>::infinity();
        return true;
    }

    int sign = 1;
    std::string body = t;
    if (!body.empty() && (body[0] == '+' || body[0] == '-')) {
        if (body[0] == '-') sign = -1;
        body = body.substr(1);
    }

    try {
        if (body.rfind("0x", 0) == 0 || body.rfind("0X", 0) == 0) {
            size_t idx = 0;
            unsigned long long v = std::stoull(body.substr(2), &idx, 16);
            if (idx != body.substr(2).size()) return false;
            out = (double)v * sign;
            return true;
        }
        if (body.rfind("0b", 0) == 0 || body.rfind("0B", 0) == 0) {
            size_t idx = 0;
            unsigned long long v = std::stoull(body.substr(2), &idx, 2);
            if (idx != body.substr(2).size()) return false;
            out = (double)v * sign;
            return true;
        }
        if (body.rfind("0o", 0) == 0 || body.rfind("0O", 0) == 0) {
            size_t idx = 0;
            unsigned long long v = std::stoull(body.substr(2), &idx, 8);
            if (idx != body.substr(2).size()) return false;
            out = (double)v * sign;
            return true;
        }

        size_t idx = 0;
        double v = std::stod(t, &idx);
        if (idx != t.size()) return false;
        out = v;
        return true;
    } catch (...) {
        return false;
    }
}

static double numberOr(const std::string &s, double fallback) {
    double v = 0.0;
    if (!parseNumberLikeJs(s, v)) return fallback;
    if (!std::isfinite(v)) return fallback;
    if (v == 0.0) return fallback;
    return v;
}

static double numberOrNaN(const std::string &s, double fallback) {
    double v = 0.0;
    if (!parseNumberLikeJs(s, v)) return fallback;
    if (!std::isfinite(v)) return fallback;
    return v;
}

static void ensureDir(const fs::path &dir) {
    if (!fs::exists(dir)) fs::create_directories(dir);
}

static fs::path getProjectRoot() {
    static fs::path cached;
    static bool inited = false;
    if (inited) return cached;
    inited = true;

    std::vector<fs::path> starts;
    starts.push_back(fs::current_path());
    if (!g_selfPath.empty()) {
        starts.push_back(fs::absolute(fs::path(g_selfPath)).parent_path());
    }
    starts.push_back(fs::absolute(fs::path(__FILE__).parent_path()));

    for (const auto &start : starts) {
        fs::path cur = start;
        for (int i = 0; i < 6; i++) {
            if (fs::exists(cur / "main.cjs")) {
                cached = fs::absolute(cur);
                return cached;
            }
            if (fs::exists(cur / "package.json")) {
                cached = fs::absolute(cur);
                return cached;
            }
            if (cur.has_parent_path()) cur = cur.parent_path();
            else break;
        }
    }
    cached = fs::absolute(fs::path(__FILE__).parent_path());
    return cached;
}

static std::map<std::string, std::string> parseArgs(int argc, char **argv) {
    std::map<std::string, std::string> out;
    for (int i = 1; i < argc; i++) {
        std::string item = argv[i];
        if (item.rfind("--", 0) != 0) continue;
        auto pos = item.find('=');
        if (pos == std::string::npos) {
            out[item.substr(2)] = "true";
        } else {
            out[item.substr(2)] = item.substr(pos + 1);
        }
    }
    return out;
}

struct Config {
    fs::path baseDir;
    std::string gatewayHost;
    int portGateway{5080};
    int portStudy{5081};
    int aiCount{7};
    int groupCount{3};
    int groupSize{7};
    int sparkNumAI{7};
    std::string sparkBudget{"default"};
    bool singleProc{false};
    bool groupProc{false};
    int groupProcTimeoutMs{12000};
    bool inferMP{false};
    int inferWorkers{1};
    int redisConnectTimeoutMs{1500};
    std::string redisUrl{"redis://127.0.0.1:6379"};
    std::string redisChannel{"AI-model-workspace"};
    fs::path snapshotDir;
    fs::path workerFile;
    int maxWorkers{1};
    fs::path shardCache;
    fs::path lmdbRoot;
    std::size_t lmdbMapSizeBytes{512ull * 1024ull * 1024ull};
    std::string searchEndpoint;
    fs::path robotsDir;
    fs::path lemmaCsv;
    bool lemmaAutoload{false};
    std::size_t lemmaMaxBytes{64ull * 1024ull * 1024ull};
    bool lemmaForce{false};
    int robotsWarmupLimit{200};
    bool robotsAutoload{true};
    bool robotsWarmupShuffle{false};
    int robotsChunkMinWords{2};
    int robotsChunkMaxWords{5};
    int kvmCacheMaxEntries{50000};
    bool learningWarmup{false};
    bool syncStandbyOnBoot{false};
    bool testsAutoload{true};
    fs::path testsDir;
    bool disableBarrier{false};
    bool disableRL{false};
    bool disableADV{false};
    bool disableLearning{false};
    fs::path exportDir;
};

static Config loadConfig(int argc, char **argv) {
    const auto args = parseArgs(argc, argv);
    const auto rootDir = getProjectRoot();
    auto arg = [&](const std::string &k, const std::string &def = "") -> std::string {
        auto it = args.find(k);
        if (it != args.end()) return it->second;
        return def;
    };
    auto argOrEnv = [&](const std::string &argKey, const std::string &env, const std::string &def = "") {
        auto it = args.find(argKey);
        if (it != args.end() && !it->second.empty()) return it->second;
        std::string v = getEnv(env);
        if (!v.empty()) return v;
        return def;
    };

    Config c;
    c.baseDir = fs::absolute(argOrEnv("base-dir", "AI_BASE_DIR", (rootDir / "runtime_store").string()));
    c.gatewayHost = argOrEnv("gateway-host", "AI_GATEWAY_HOST", "127.0.0.1");
    c.portGateway = (int)numberOrNaN(argOrEnv("port", "CONTROLLER_PORT", "5080"), 5080);
    c.portStudy = (int)numberOrNaN(argOrEnv("study-port", "AI_STUDY_PORT", "5081"), 5081);

    auto aiCountRaw = argOrEnv("ai-count", "AI_COUNT", getEnv("AI_NUM", "7"));
    auto groupCountRaw = argOrEnv("group-count", "AI_GROUP_COUNT", getEnv("GROUP_COUNT", "3"));
    auto groupSizeRaw = argOrEnv("group-size", "AI_GROUP_SIZE", getEnv("GROUP_SIZE", aiCountRaw));
    auto sparkNumAiRaw = argOrEnv("spark-num-ai", "AI_SPARK_NUM_AI", groupSizeRaw);

    double aiCountNum = numberOr(aiCountRaw, 7);
    double groupCountNum = numberOr(groupCountRaw, 3);
    double groupSizeNum = numberOr(groupSizeRaw, numberOr(aiCountRaw, 7));
    double sparkNumAiNum = numberOr(sparkNumAiRaw, numberOr(groupSizeRaw, numberOr(aiCountRaw, 7)));

    c.aiCount = std::max(3, (int)aiCountNum);
    c.groupCount = std::max(1, (int)groupCountNum);
    c.groupSize = std::max(1, (int)groupSizeNum);
    c.sparkNumAI = std::max(1, (int)sparkNumAiNum);
    c.sparkBudget = argOrEnv("spark-budget", "AI_SPARK_BUDGET", "default");

    c.singleProc = boolFrom(argOrEnv("single-proc", "AI_SINGLE_PROC", "false"), false);
    c.groupProc = c.singleProc ? false : boolFrom(argOrEnv("group-proc", "AI_GROUP_PROC", "false"), false);
    c.groupProcTimeoutMs = std::max(500, (int)numberOr(argOrEnv("group-proc-timeout-ms", "AI_GROUP_PROC_TIMEOUT_MS", "12000"), 12000));

    c.inferMP = c.singleProc ? false : boolFrom(argOrEnv("infer-mp", "AI_INFER_MP", "false"), false);
    int cpuCount = std::max(2u, std::thread::hardware_concurrency());
    int inferWorkersRaw = 0;
    inferWorkersRaw = (int)numberOr(argOrEnv("infer-workers", "AI_INFER_WORKERS", "0"), 0);
    c.inferWorkers = std::max(1, inferWorkersRaw ? inferWorkersRaw : (cpuCount - 1));

    c.redisConnectTimeoutMs = std::max(200, (int)numberOr(argOrEnv("redis-timeout-ms", "AI_REDIS_CONNECT_TIMEOUT_MS", "1500"), 1500));
    c.redisUrl = argOrEnv("redis-url", "REDIS_URL", "redis://127.0.0.1:6379");
    c.redisChannel = argOrEnv("channel", "AI_REDIS_CHANNEL", "AI-model-workspace");

    c.snapshotDir = fs::absolute(argOrEnv("snapshot-dir", "AI_SNAPSHOT_DIR", (rootDir / "snapshots").string()));
    c.workerFile = fs::absolute(rootDir / "memeMergeWorker.cjs");
    c.maxWorkers = std::max(1, (int)cpuCount - 1);
    c.shardCache = fs::absolute(rootDir / "shards_cache.json");

    c.lmdbRoot = fs::absolute(argOrEnv("lmdb-dir", "LMDB_DIR", (rootDir / "lmdb").string()));
    c.lmdbMapSizeBytes = std::max<std::size_t>(64, (std::size_t)numberOr(argOrEnv("lmdb-map-mb", "AI_LMDB_MAP_MB", "512"), 512)) * 1024ull * 1024ull;

    c.searchEndpoint = argOrEnv("search-endpoint", "AI_SEARCH_ENDPOINT", "");
    c.robotsDir = fs::absolute(argOrEnv("robots-dir", "AI_ROBOTS_DIR", (rootDir / "robots").string()));
    c.lemmaCsv = fs::absolute(argOrEnv("lemma-csv", "AI_LEMMA_CSV", (rootDir / "lemma.csv").string()));

    c.lemmaAutoload = boolFrom(argOrEnv("lemma-autoload", "AI_LEMMA_AUTOLOAD", "false"), false);
    c.lemmaMaxBytes = std::max<std::size_t>(1, (std::size_t)numberOr(argOrEnv("lemma-max-mb", "AI_LEMMA_MAX_MB", "64"), 64)) * 1024ull * 1024ull;
    c.lemmaForce = boolFrom(argOrEnv("lemma-force", "AI_LEMMA_FORCE", "false"), false);

    c.robotsWarmupLimit = std::max(0, (int)numberOr(argOrEnv("robots-limit", "AI_ROBOTS_LIMIT", "200"), 0));
    c.robotsAutoload = boolFrom(argOrEnv("robots-autoload", "AI_ROBOTS_AUTOLOAD", "true"), true);
    c.robotsWarmupShuffle = boolFrom(argOrEnv("robots-warmup-shuffle", "AI_ROBOTS_WARMUP_SHUFFLE", "false"), false);
    c.robotsChunkMinWords = std::max(1, (int)numberOr(argOrEnv("robots-chunk-min", "AI_ROBOTS_CHUNK_MIN", "3"), 2));
    c.robotsChunkMaxWords = std::max(c.robotsChunkMinWords, (int)numberOr(argOrEnv("robots-chunk-max", "AI_ROBOTS_CHUNK_MAX", "20"), 5));

    c.kvmCacheMaxEntries = std::max(0, (int)numberOr(argOrEnv("kvm-cache-max", "AI_KVM_CACHE_MAX", "50000"), 0));
    c.learningWarmup = boolFrom(argOrEnv("learning-warmup", "AI_LEARNING_WARMUP", "false"), false);
    c.syncStandbyOnBoot = boolFrom(argOrEnv("sync-standby", "AI_SYNC_STANDBY_ON_BOOT", "false"), false);
    c.testsAutoload = boolFrom(argOrEnv("tests-autoload", "AI_TESTS_AUTOLOAD", "true"), true);
    c.testsDir = fs::absolute(rootDir / "tests");

    c.disableBarrier = boolFrom(argOrEnv("disable-memebarrier", "AI_DISABLE_MEMEBARRIER", "false"), false);
    c.disableRL = boolFrom(argOrEnv("disable-rl", "AI_DISABLE_RL", "false"), false);
    c.disableADV = boolFrom(argOrEnv("disable-adv", "AI_DISABLE_ADV", "false"), false);
    c.disableLearning = boolFrom(argOrEnv("disable-learning", "AI_DISABLE_LEARNING", "false"), false);

    c.exportDir = fs::absolute(argOrEnv("export-dir", "AI_EXPORT_DIR", (rootDir / "runtime_store").string()));
    return c;
}

// ------------------ Utilities ------------------

static std::string nowIso() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm);
    std::ostringstream oss;
    oss << buf << '.' << std::setfill('0') << std::setw(3) << ms.count() << 'Z';
    return oss.str();
}

static int64_t nowEpochMs() {
    return (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

static json getLoadAvg() {
    json out = json::array();
#ifdef _WIN32
    out.push_back(0.0);
    out.push_back(0.0);
    out.push_back(0.0);
#else
    double loads[3] = {0.0, 0.0, 0.0};
    if (getloadavg(loads, 3) != -1) {
        out.push_back(loads[0]);
        out.push_back(loads[1]);
        out.push_back(loads[2]);
    } else {
        out.push_back(0.0);
        out.push_back(0.0);
        out.push_back(0.0);
    }
#endif
    return out;
}

static json getMemoryUsage() {
    json mem{{"rss", 0}, {"heapTotal", 0}, {"heapUsed", 0}, {"external", 0}, {"arrayBuffers", 0}};
#ifdef _WIN32
    PROCESS_MEMORY_COUNTERS_EX pmc{};
    if (GetProcessMemoryInfo(GetCurrentProcess(), (PROCESS_MEMORY_COUNTERS*)&pmc, sizeof(pmc))) {
        mem["rss"] = (int64_t)pmc.WorkingSetSize;
        mem["heapTotal"] = (int64_t)pmc.PrivateUsage;
        mem["heapUsed"] = (int64_t)pmc.PrivateUsage;
        mem["external"] = 0;
        mem["arrayBuffers"] = 0;
    }
#else
    long pageSize = sysconf(_SC_PAGESIZE);
    std::ifstream statm("/proc/self/statm");
    long size = 0, resident = 0;
    if (statm >> size >> resident) {
        int64_t rss = (int64_t)resident * pageSize;
        mem["rss"] = rss;
        mem["heapTotal"] = rss;
        mem["heapUsed"] = rss;
        mem["external"] = 0;
        mem["arrayBuffers"] = 0;
    }
#endif
    return mem;
}

static std::string sha1Hex(const std::string &s) {
    auto digest = trantor::utils::sha1(s);
    return trantor::utils::toHexString(digest);
}

static std::string randomHex(size_t bytes) {
    std::random_device rd;
    std::uniform_int_distribution<int> dist(0, 255);
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < bytes; i++) {
        int v = dist(rd);
        oss << std::setw(2) << (v & 0xff);
    }
    return oss.str();
}

static std::string randomSessionId() {
    return randomHex(8);
}

static uint32_t hashStrSimple(const std::string &s, uint32_t seed = 1315423911u) {
    uint32_t hash = seed;
    for (unsigned char c : s) {
        hash ^= ((hash << 5) + c + (hash >> 2));
    }
    return hash;
}

static std::string readTextFile(const fs::path &file) {
    std::ifstream in(file, std::ios::binary);
    if (!in) return {};
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

#ifdef _WIN32
using SocketHandle = SOCKET;
static constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
static void closeSocket(SocketHandle s) { if (s != INVALID_SOCKET) closesocket(s); }
#else
using SocketHandle = int;
static constexpr SocketHandle kInvalidSocket = -1;
static void closeSocket(SocketHandle s) { if (s >= 0) close(s); }
#endif

static bool sendAll(SocketHandle sock, const std::string &data) {
    const char *buf = data.c_str();
    size_t total = 0;
    while (total < data.size()) {
#ifdef _WIN32
    int sent = ::send(sock, buf + total, (int)(data.size() - total), 0);
#else
    ssize_t sent = ::send(sock, buf + total, data.size() - total, 0);
#endif
    if (sent <= 0) return false;
    total += (size_t)sent;
    }
    return true;
}

static bool recvLine(SocketHandle sock, std::string &out, int timeoutMs) {
    out.clear();
    char ch;
    while (true) {
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(sock, &rfds);
    timeval tv;
    tv.tv_sec = timeoutMs / 1000;
    tv.tv_usec = (timeoutMs % 1000) * 1000;
    int rc = select((int)(sock + 1), &rfds, nullptr, nullptr, &tv);
    if (rc <= 0) return false;
#ifdef _WIN32
    int n = ::recv(sock, &ch, 1, 0);
#else
    ssize_t n = ::recv(sock, &ch, 1, 0);
#endif
    if (n <= 0) return false;
    if (ch == '\n') break;
    if (ch != '\r') out.push_back(ch);
    }
    return true;
}

static std::string urlEncode(const std::string &s) {
    std::ostringstream oss;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            oss << c;
        } else if (c == ' ') {
            oss << '%';
            oss << std::hex << std::uppercase << (int)c << std::nouppercase << std::dec;
        } else {
            oss << '%';
            oss << std::hex << std::uppercase << (int)c << std::nouppercase << std::dec;
        }
    }
    return oss.str();
}

static std::string urlDecode(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); i++) {
        char c = s[i];
        if (c == '+') {
            out.push_back(' ');
            continue;
        }
        if (c == '%' && i + 2 < s.size()) {
            auto hex = s.substr(i + 1, 2);
            char *end = nullptr;
            long v = std::strtol(hex.c_str(), &end, 16);
            if (end && *end == '\0') {
                out.push_back(static_cast<char>(v));
                i += 2;
                continue;
            }
        }
        out.push_back(c);
    }
    return out;
}

static json parseFormUrlEncoded(const std::string &body) {
    json out = json::object();
    size_t start = 0;
    while (start <= body.size()) {
        size_t amp = body.find('&', start);
        if (amp == std::string::npos) amp = body.size();
        std::string pair = body.substr(start, amp - start);
        size_t eq = pair.find('=');
        std::string key = eq == std::string::npos ? pair : pair.substr(0, eq);
        std::string val = eq == std::string::npos ? "" : pair.substr(eq + 1);
        key = urlDecode(key);
        val = urlDecode(val);
        if (!key.empty()) {
            if (!out.contains(key)) {
                out[key] = val;
            } else if (out[key].is_array()) {
                out[key].push_back(val);
            } else {
                json arr = json::array();
                arr.push_back(out[key]);
                arr.push_back(val);
                out[key] = arr;
            }
        }
        if (amp == body.size()) break;
        start = amp + 1;
    }
    return out;
}

static std::string normalizeUrlSimple(const std::string &url) {
    if (url.empty()) return {};
    std::string out = url;
    auto hashPos = out.find('#');
    if (hashPos != std::string::npos) out.erase(hashPos);
    auto qPos = out.find('?');
    if (qPos != std::string::npos) {
        std::string base = out.substr(0, qPos);
        std::string query = out.substr(qPos + 1);
        std::vector<std::string> parts;
        std::string cur;
        for (char c : query) {
            if (c == '&') { if (!cur.empty()) parts.push_back(cur); cur.clear(); }
            else cur.push_back(c);
        }
        if (!cur.empty()) parts.push_back(cur);
        std::vector<std::string> kept;
        for (auto &p : parts) {
            auto eq = p.find('=');
            std::string key = (eq == std::string::npos) ? p : p.substr(0, eq);
            std::string lower = key;
            std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            if (lower.rfind("utm_", 0) == 0 || lower == "fbclid" || lower == "gclid") continue;
            kept.push_back(p);
        }
        if (!kept.empty()) {
            std::string q;
            for (size_t i = 0; i < kept.size(); i++) { if (i) q += "&"; q += kept[i]; }
            out = base + "?" + q;
        } else {
            out = base;
        }
    }
    return out;
}

static bool sameSiteSimple(const std::string &baseUrl, const std::string &targetUrl) {
    auto hostOf = [](const std::string &u) {
        auto pos = u.find("//");
        if (pos == std::string::npos) return std::string();
        auto start = pos + 2;
        auto end = u.find('/', start);
        std::string host = u.substr(start, end == std::string::npos ? u.size() - start : end - start);
        if (host.rfind("www.", 0) == 0) host = host.substr(4);
        return host;
    };
    auto h1 = hostOf(baseUrl);
    auto h2 = hostOf(targetUrl);
    return !h1.empty() && h1 == h2;
}

#ifdef HAVE_CURL
struct FetchResult {
    std::string body;
    std::string contentType;
    long status{0};
    std::string finalUrl;
    size_t bytes{0};
};

struct CurlWriteCtx {
    std::string *out;
    size_t maxBytes;
};

static size_t curlWriteCb(char *ptr, size_t size, size_t nmemb, void *userdata) {
    auto *ctx = reinterpret_cast<CurlWriteCtx *>(userdata);
    size_t total = size * nmemb;
    if (ctx->maxBytes > 0 && ctx->out->size() + total > ctx->maxBytes) {
        size_t allowed = ctx->maxBytes - ctx->out->size();
        if (allowed > 0) ctx->out->append(ptr, allowed);
        return 0;
    }
    ctx->out->append(ptr, total);
    return total;
}

static FetchResult fetchUrl(const std::string &url, int timeoutMs = 10000, const std::string &userAgent = "079ProjectCrawler/1.0", size_t maxBytes = 0) {
    FetchResult res;
    CURL *curl = curl_easy_init();
    if (!curl) return res;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeoutMs);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, userAgent.c_str());
    CurlWriteCtx ctx{&res.body, maxBytes};
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlWriteCb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);
    curl_easy_perform(curl);
    char *ct = nullptr;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_TYPE, &ct);
    if (ct) res.contentType = ct;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &res.status);
    char *finalUrl = nullptr;
    curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &finalUrl);
    if (finalUrl) res.finalUrl = finalUrl;
    res.bytes = res.body.size();
    curl_easy_cleanup(curl);
    return res;
}
#endif

#ifdef HAVE_GUMBO
static void gumboExtractText(GumboNode *node, std::string &out) {
    if (!node) return;
    if (node->type == GUMBO_NODE_TEXT) {
        out.append(node->v.text.text);
        out.push_back(' ');
        return;
    }
    if (node->type != GUMBO_NODE_ELEMENT && node->type != GUMBO_NODE_DOCUMENT) return;
    const GumboVector *children = &node->v.element.children;
    for (size_t i = 0; i < children->length; i++) {
        gumboExtractText(static_cast<GumboNode *>(children->data[i]), out);
    }
}

static std::string extractTextFromHtml(const std::string &html) {
    GumboOutput *output = gumbo_parse(html.c_str());
    std::string text;
    gumboExtractText(output->root, text);
    gumbo_destroy_output(&kGumboDefaultOptions, output);
    return text;
}
#endif

#ifdef HAVE_POPPLER
static std::string extractTextFromPdfFile(const fs::path &file) {
    std::unique_ptr<poppler::document> doc(poppler::document::load_from_file(file.string()));
    if (!doc) return {};
    std::string out;
    for (int i = 0; i < doc->pages(); i++) {
        auto page = std::unique_ptr<poppler::page>(doc->create_page(i));
        if (!page) continue;
        auto bytes = page->text().to_utf8();
        out.append(bytes.begin(), bytes.end());
        out.push_back('\n');
    }
    return out;
}
#endif

static std::vector<std::string> parseJsStringLiterals(const std::string &src) {
    std::vector<std::string> out;
    const size_t n = src.size();
    for (size_t i = 0; i < n; i++) {
        char quote = src[i];
        if (quote != '\'' && quote != '"') continue;
        std::string buf;
        i++;
        for (; i < n; i++) {
            char c = src[i];
            if (c == '\\' && i + 1 < n) {
                char next = src[i + 1];
                if (next == 'n') { buf.push_back('\n'); i++; continue; }
                if (next == 'r') { buf.push_back('\r'); i++; continue; }
                if (next == 't') { buf.push_back('\t'); i++; continue; }
                if (next == '\\' || next == '\'' || next == '"') { buf.push_back(next); i++; continue; }
                buf.push_back(next);
                i++;
                continue;
            }
            if (c == quote) break;
            buf.push_back(c);
        }
        if (!buf.empty()) out.push_back(buf);
    }
    return out;
}

static bool loadNaturalStopwords(std::unordered_set<std::string> &out) {
    std::vector<fs::path> roots;
    roots.push_back(fs::current_path());
    if (!g_selfPath.empty()) {
        auto exeDir = fs::absolute(fs::path(g_selfPath)).parent_path();
        roots.push_back(exeDir);
        roots.push_back(exeDir.parent_path());
    }
    roots.push_back(fs::absolute(fs::path(__FILE__).parent_path()));
    roots.push_back(fs::absolute(fs::path(__FILE__).parent_path().parent_path()));

    for (const auto &root : roots) {
        fs::path file = root / "node_modules" / "natural" / "lib" / "natural" / "util" / "stopwords.js";
        if (!fs::exists(file)) continue;
        std::string content = readTextFile(file);
        if (content.empty()) continue;
        auto words = parseJsStringLiterals(content);
        for (const auto &w : words) {
            if (w.empty()) continue;
            if (w.find_first_of(" \t\r\n") != std::string::npos) continue;
            out.insert(w);
        }
        return true;
    }
    return false;
}

static const std::unordered_set<std::string> &getStopWords() {
    static std::unordered_set<std::string> stop;
    static bool loaded = false;
    if (!loaded) {
        loaded = true;
        loadNaturalStopwords(stop);
    }
    return stop;
}

static bool isAsciiWordToken(const std::string &token) {
    if (token.empty()) return false;
    for (unsigned char c : token) {
        if (c >= 0x80) return false;
        if (!std::isalnum(c) && c != '_' && c != '-') return false;
    }
    return true;
}

static std::vector<std::string> tokenize(const std::string &text) {
    auto isCjk = [](uint32_t cp) {
        return cp >= 0x4e00 && cp <= 0x9fff;
    };
    auto decode = [](const std::string &s, size_t i, uint32_t &cp, size_t &len) {
        unsigned char c = (unsigned char)s[i];
        if (c < 0x80) { cp = c; len = 1; return true; }
        if ((c >> 5) == 0x6 && i + 1 < s.size()) {
            cp = ((c & 0x1f) << 6) | (s[i + 1] & 0x3f);
            len = 2; return true;
        }
        if ((c >> 4) == 0xe && i + 2 < s.size()) {
            cp = ((c & 0x0f) << 12) | ((s[i + 1] & 0x3f) << 6) | (s[i + 2] & 0x3f);
            len = 3; return true;
        }
        if ((c >> 3) == 0x1e && i + 3 < s.size()) {
            cp = ((c & 0x07) << 18) | ((s[i + 1] & 0x3f) << 12) | ((s[i + 2] & 0x3f) << 6) | (s[i + 3] & 0x3f);
            len = 4; return true;
        }
        cp = c; len = 1; return false;
    };

    std::vector<std::string> parts;
    std::string cur;
    auto flush = [&]() {
        if (cur.empty()) return;
        if (isAsciiWordToken(cur) && getStopWords().count(cur)) {
            cur.clear();
            return;
        }
        parts.push_back(cur);
        cur.clear();
    };

    for (size_t i = 0; i < text.size();) {
        uint32_t cp = 0; size_t len = 1;
        decode(text, i, cp, len);
        if (cp == '\r' || cp == '\n' || cp == '\t' || cp == ' ') {
            flush();
            i += len; continue;
        }
        if (cp < 0x80) {
            unsigned char c = (unsigned char)cp;
            if (std::isalnum(c) || c == '_' || c == '-') {
                cur.push_back((char)std::tolower(c));
            } else {
                flush();
            }
            i += len; continue;
        }
        if (isCjk(cp)) {
            cur.append(text.substr(i, len));
        } else {
            flush();
        }
        i += len;
    }
    flush();
    return parts;
}

static double cosineSim(const std::vector<float> &a, const std::vector<float> &b) {
    if (a.empty() || b.empty() || a.size() != b.size()) return 0.0;
    double dot = 0.0, na = 0.0, nb = 0.0;
    for (size_t i = 0; i < a.size(); i++) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if (na == 0.0 || nb == 0.0) return 0.0;
    return dot / (std::sqrt(na) * std::sqrt(nb));
}

static std::vector<float> textToMiniEmbedding(const std::string &text, int dim = 64) {
    std::vector<float> vec(std::max(1, dim), 0.0f);
    auto words = tokenize(text);
    for (const auto &w : words) {
        auto idx = hashStrSimple(w) % vec.size();
        vec[idx] += 1.0f;
    }
    if (words.size() > 1) {
        for (size_t i = 0; i + 1 < words.size(); i++) {
            std::string pair = words[i] + "_" + words[i + 1];
            auto idx = hashStrSimple(pair) % vec.size();
            vec[idx] += 0.5f;
        }
    }
    return vec;
}

static std::vector<std::string> perturbTokens(const std::vector<std::string> &words) {
    if (words.size() <= 1) return words;
    size_t idx = (size_t)(std::rand() % words.size());
    std::vector<std::string> out;
    out.reserve(words.size() - 1);
    for (size_t i = 0; i < words.size(); i++) {
        if (i != idx) out.push_back(words[i]);
    }
    return out;
}

static std::vector<std::string> buildVariants(const std::string &text, int count = 0) {
    std::vector<std::string> variants;
    if (count <= 0) return variants;
    auto words = tokenize(text);
    for (int i = 0; i < count; i++) {
        words = perturbTokens(words);
        std::ostringstream oss;
        for (size_t j = 0; j < words.size(); j++) {
            if (j) oss << " ";
            oss << words[j];
        }
        variants.push_back(oss.str());
        if (words.size() <= 1) break;
    }
    return variants;
}

static double clamp01(double x) { return std::max(0.0, std::min(1.0, x)); }
static double clamp11(double x) { return std::max(-1.0, std::min(1.0, x)); }

static int clampInt(const json &value, int minVal, int maxVal, int fallback) {
    if (!value.is_number()) return fallback;
    int n = value.get<int>();
    if (n < minVal) return minVal;
    if (n > maxVal) return maxVal;
    return n;
}

static std::string extractFirstUrl(const std::string &text) {
    std::string s = text;
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char c){ return !std::isspace(c); }));
    if (s.empty()) return {};
    std::regex r1(R"(https?://[\w\-._~:/?#[\]@!$&'()*+,;=%]+)" , std::regex::icase);
    std::smatch m;
    if (std::regex_search(s, m, r1) && m.size() > 0) return m[0].str();
    std::regex r2(R"(\b(?:www\.)[a-z0-9\-]+(?:\.[a-z0-9\-]+)+(?:/[\w\-._~:/?#[\]@!$&'()*+,;=%]*)?)", std::regex::icase);
    if (std::regex_search(s, m, r2) && m.size() > 0) return std::string("https://") + m[0].str();
    return {};
}

class Rng32 {
public:
    explicit Rng32(uint32_t seed) : x_(seed ? seed : 0x9e3779b9u) {}
    double next() {
        x_ ^= x_ << 13;
        x_ ^= x_ >> 17;
        x_ ^= x_ << 5;
        return (double)(x_ & 0xffffffffu) / (double)0xffffffffu;
    }
private:
    uint32_t x_{0x9e3779b9u};
};

static std::optional<json> normalizeBudget(const json &raw) {
    if (raw.is_null() || raw.is_boolean()) return std::nullopt;
    if (raw.is_string()) {
        auto s = raw.get<std::string>();
        std::string lowered;
        lowered.reserve(s.size());
        for (char c : s) lowered.push_back((char)std::tolower((unsigned char)c));
        if (lowered == "default" || lowered == "balanced" || lowered == "medium" || lowered == "none") return std::nullopt;
        if (lowered == "low" || lowered == "fast") return json{{"iteration", 3}, {"reflectionTopMemes", 12}, {"reflectionTopWords", 16}};
        if (lowered == "high" || lowered == "slow" || lowered == "quality") return json{{"iteration", 7}, {"reflectionTopMemes", 22}, {"reflectionTopWords", 32}};
        if (!s.empty() && s.front() == '{' && s.back() == '}') {
            try { return normalizeBudget(json::parse(s)); } catch (...) { return std::nullopt; }
        }
        return std::nullopt;
    }
    if (!raw.is_object()) return std::nullopt;
    json out = json::object();
    auto pickNum = [&](const std::string &key, const std::vector<std::string> &aliases) {
        if (raw.contains(key) && raw[key].is_number()) { out[key] = raw[key].get<double>(); return; }
        for (auto &a : aliases) {
            if (raw.contains(a) && raw[a].is_number()) { out[key] = raw[a].get<double>(); return; }
        }
    };
    pickNum("mappingDepth", {"depth"});
    pickNum("iteration", {"iters"});
    pickNum("reflectionTopMemes", {"topMemes"});
    pickNum("reflectionTopWords", {"topWords"});
    pickNum("reflectionMinScore", {"minScore"});
    pickNum("radius", {"windowRadius"});
    return out.empty() ? std::nullopt : std::optional<json>(out);
}

static std::optional<json> mergeBudgets(const std::optional<json> &base, const std::optional<json> &override) {
    if (!base && !override) return std::nullopt;
    if (!base) return override;
    if (!override) return base;
    json out = *base;
    for (auto it = override->begin(); it != override->end(); ++it) out[it.key()] = it.value();
    return out;
}

static std::vector<std::string> splitSentences(const std::string &text) {
    std::string raw = text;
    raw.erase(0, raw.find_first_not_of(" \n\r\t"));
    if (raw.empty()) return {};
    raw.erase(raw.find_last_not_of(" \n\r\t") + 1);

    std::vector<std::string> rough;
    std::string cur;
    for (size_t i = 0; i < raw.size(); i++) {
        char c = raw[i];
        if (c == '\r' || c == '\n') {
            auto t = cur;
            t.erase(0, t.find_first_not_of(" \n\r\t"));
            if (!t.empty()) {
                t.erase(t.find_last_not_of(" \n\r\t") + 1);
                if (!t.empty()) rough.push_back(t);
            }
            cur.clear();
            continue;
        }
        cur.push_back(c);
        if (c == '。' || c == '！' || c == '？' || c == '!' || c == '?') {
            auto t = cur;
            t.erase(0, t.find_first_not_of(" \n\r\t"));
            if (!t.empty()) {
                t.erase(t.find_last_not_of(" \n\r\t") + 1);
                if (!t.empty()) rough.push_back(t);
            }
            cur.clear();
            while (i + 1 < raw.size()) {
                char n = raw[i + 1];
                if (n == ' ' || n == '\t' || n == '\r' || n == '\n') { i++; continue; }
                break;
            }
        }
    }
    if (!cur.empty()) {
        auto t = cur;
        t.erase(0, t.find_first_not_of(" \n\r\t"));
        if (!t.empty()) {
            t.erase(t.find_last_not_of(" \n\r\t") + 1);
            if (!t.empty()) rough.push_back(t);
        }
    }

    const int maxWords = 10;
    const int minWords = 2;
    std::vector<std::string> out;

    for (const auto &unit : rough) {
        auto tokens = tokenize(unit);
        if ((int)tokens.size() < minWords) continue;
        for (size_t i = 0; i < tokens.size(); i += maxWords) {
            std::ostringstream oss;
            int count = 0;
            for (size_t j = i; j < tokens.size() && j < i + maxWords; j++) {
                if (count++) oss << " ";
                oss << tokens[j];
            }
            if (count < minWords) {
                if (!out.empty() && count > 0) {
                    out.back() += " " + oss.str();
                }
                continue;
            }
            out.push_back(oss.str());
            if (out.size() >= 12) return out;
        }
        if (out.size() >= 12) break;
    }
    if (out.size() > 12) out.resize(12);
    return out;
}

struct SurfacePhrase { std::string phrase; double weight; };

static std::vector<SurfacePhrase> extractSurfacePhrases(const std::string &text, int maxPhrases = 24) {
    std::vector<SurfacePhrase> out;
    std::unordered_set<std::string> seen;
    auto push = [&](const std::string &p, double w) {
        std::string s = p;
        s.erase(0, s.find_first_not_of(" \n\r\t"));
        if (s.empty()) return;
        s.erase(s.find_last_not_of(" \n\r\t") + 1);
        if (s.empty()) return;
        if (s.size() < 2 || s.size() > 160) return;
        if (seen.count(s)) return;
        seen.insert(s);
        out.push_back({s, w});
    };

    for (const auto &s : splitSentences(text)) push(s, 1.0);

    auto tokens = tokenize(text);
    for (size_t i = 0; i < tokens.size() && (int)out.size() < maxPhrases; i++) {
        push(tokens[i], 2.0);
        if ((int)out.size() >= maxPhrases) return out;
    }

    int maxN = std::min<int>(5, (int)tokens.size());
    for (int n = 2; n <= maxN; n++) {
        for (size_t i = 0; i + n <= tokens.size(); i++) {
            std::ostringstream oss;
            for (int j = 0; j < n; j++) {
                if (j) oss << " ";
                oss << tokens[i + j];
            }
            double w = (n == 2 ? 3.0 : (n == 3 ? 2.0 : 1.0));
            push(oss.str(), w);
            if ((int)out.size() >= maxPhrases) return out;
        }
    }
    return out;
}

// ------------------ Storage ------------------

class KeyValueStore {
public:
    virtual ~KeyValueStore() = default;
    virtual std::optional<json> get(const std::string &key) = 0;
    virtual void put(const std::string &key, const json &value) = 0;
    virtual void del(const std::string &key) = 0;
    virtual std::vector<std::pair<std::string, json>> entries(const std::string &prefix) = 0;
    virtual void flush() {}
};

#ifdef HAVE_LMDB
class LmdbStore : public KeyValueStore {
public:
    LmdbStore(const std::string &name, const fs::path &rootDir, std::size_t mapSizeBytes, bool encodeJSON = true)
        : name_(name), rootDir_(rootDir), mapSizeBytes_(mapSizeBytes), encodeJSON_(encodeJSON) {
        ensureDir(rootDir_ / name_);
        fs::path envPath = rootDir_ / name_;
        if (mdb_env_create(&env_) != 0) {
            env_ = nullptr; return;
        }
        mdb_env_set_maxreaders(env_, 64);
        mdb_env_set_mapsize(env_, mapSizeBytes_);
        if (mdb_env_open(env_, envPath.string().c_str(), 0, 0664) != 0) {
            mdb_env_close(env_); env_ = nullptr; return;
        }
        MDB_txn *txn = nullptr;
        if (mdb_txn_begin(env_, nullptr, 0, &txn) != 0) return;
        if (mdb_dbi_open(txn, "default", MDB_CREATE, &dbi_) != 0) {
            mdb_txn_abort(txn); return;
        }
        mdb_txn_commit(txn);
        ok_ = true;
    }

    ~LmdbStore() override {
        if (env_) {
            mdb_dbi_close(env_, dbi_);
            mdb_env_close(env_);
        }
    }

    bool ok() const { return ok_; }

    std::optional<json> get(const std::string &key) override {
        if (!ok_) return std::nullopt;
        MDB_txn *txn = nullptr;
        MDB_val k{key.size(), (void *)key.data()};
        MDB_val v;
        if (mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn) != 0) return std::nullopt;
        int rc = mdb_get(txn, dbi_, &k, &v);
        mdb_txn_abort(txn);
        if (rc != 0) return std::nullopt;
        std::string raw((char *)v.mv_data, v.mv_size);
        try {
            return encodeJSON_ ? json::parse(raw) : json(raw);
        } catch (...) {
            return json(raw);
        }
    }

    void put(const std::string &key, const json &value) override {
        if (!ok_) return;
        MDB_txn *txn = nullptr;
        mdb_txn_begin(env_, nullptr, 0, &txn);
        std::string encoded = encodeJSON_ ? value.dump() : value.get<std::string>();
        MDB_val k{key.size(), (void *)key.data()};
        MDB_val v{encoded.size(), (void *)encoded.data()};
        mdb_put(txn, dbi_, &k, &v, 0);
        mdb_txn_commit(txn);
    }

    void del(const std::string &key) override {
        if (!ok_) return;
        MDB_txn *txn = nullptr;
        mdb_txn_begin(env_, nullptr, 0, &txn);
        MDB_val k{key.size(), (void *)key.data()};
        mdb_del(txn, dbi_, &k, nullptr);
        mdb_txn_commit(txn);
    }

    std::vector<std::pair<std::string, json>> entries(const std::string &prefix) override {
        std::vector<std::pair<std::string, json>> out;
        if (!ok_) return out;
        MDB_txn *txn = nullptr;
        MDB_cursor *cursor = nullptr;
        mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
        mdb_cursor_open(txn, dbi_, &cursor);
        MDB_val k, v;
        std::string start = prefix;
        k.mv_size = start.size();
        k.mv_data = (void *)start.data();
        int rc = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);
        while (rc == 0) {
            std::string key((char *)k.mv_data, k.mv_size);
            if (!prefix.empty() && key.rfind(prefix, 0) != 0) break;
            std::string raw((char *)v.mv_data, v.mv_size);
            try {
                out.push_back({key, encodeJSON_ ? json::parse(raw) : json(raw)});
            } catch (...) {
                out.push_back({key, json(raw)});
            }
            rc = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
        }
        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        return out;
    }

private:
    std::string name_;
    fs::path rootDir_;
    std::size_t mapSizeBytes_;
    bool encodeJSON_{true};
    bool ok_{false};
    MDB_env *env_{nullptr};
    MDB_dbi dbi_{0};
};
#endif

class JsonFileStore : public KeyValueStore {
public:
    JsonFileStore(const std::string &name, const fs::path &rootDir)
        : name_(name), rootDir_(rootDir) {
        ensureDir(rootDir_);
        file_ = rootDir_ / (name_ + ".json");
        if (fs::exists(file_)) {
            try {
                std::ifstream in(file_);
                nlohmann::ordered_json j;
                in >> j;
                if (j.is_object()) {
                    for (auto it = j.begin(); it != j.end(); ++it) {
                        json value = it.value();
                        if (value.is_string()) {
                            const auto raw = value.get<std::string>();
                            try {
                                data_[it.key()] = json::parse(raw);
                            } catch (...) {
                                data_[it.key()] = value;
                            }
                        } else {
                            data_[it.key()] = value;
                        }
                        order_.push_back(it.key());
                    }
                }
            } catch (...) {}
        }
        startFlushThread();
    }

    ~JsonFileStore() override {
        stopFlushThread();
        flush();
    }

    std::optional<json> get(const std::string &key) override {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = data_.find(key);
        if (it == data_.end()) return std::nullopt;
        return it->second;
    }

    void put(const std::string &key, const json &value) override {
        std::lock_guard<std::mutex> lock(mu_);
        if (!data_.count(key)) order_.push_back(key);
        data_[key] = value;
        dirty_ = true;
    }

    void del(const std::string &key) override {
        std::lock_guard<std::mutex> lock(mu_);
        if (data_.erase(key) > 0) {
            order_.erase(std::remove(order_.begin(), order_.end(), key), order_.end());
        }
        dirty_ = true;
    }

    std::vector<std::pair<std::string, json>> entries(const std::string &prefix) override {
        std::vector<std::pair<std::string, json>> out;
        std::lock_guard<std::mutex> lock(mu_);
        for (auto &key : order_) {
            auto it = data_.find(key);
            if (it == data_.end()) continue;
            if (prefix.empty() || key.rfind(prefix, 0) == 0) out.push_back(*it);
        }
        return out;
    }

    void flush() override {
        std::lock_guard<std::mutex> lock(mu_);
        if (!dirty_) return;
        std::ofstream out(file_);
        nlohmann::ordered_json j = nlohmann::ordered_json::object();
        for (auto &key : order_) {
            auto it = data_.find(key);
            if (it != data_.end()) j[key] = it->second.dump();
        }
        out << j.dump(2);
        dirty_ = false;
    }

private:
    void startFlushThread() {
        running_.store(true);
        flushThread_ = std::thread([this]() {
            while (running_.load()) {
                std::this_thread::sleep_for(std::chrono::seconds(10));
                if (!running_.load()) break;
                flush();
            }
        });
    }

    void stopFlushThread() {
        running_.store(false);
        if (flushThread_.joinable()) flushThread_.join();
    }

    std::string name_;
    fs::path rootDir_;
    fs::path file_;
    bool dirty_{false};
    std::unordered_map<std::string, json> data_;
    std::vector<std::string> order_;
    std::mutex mu_;
    std::atomic<bool> running_{false};
    std::thread flushThread_;
};

class NamespacedStore : public KeyValueStore {
public:
    NamespacedStore(std::shared_ptr<KeyValueStore> base, const std::string &ns)
        : base_(std::move(base)) {
        std::string trimmed = ns;
        trimmed.erase(trimmed.begin(), std::find_if(trimmed.begin(), trimmed.end(), [](unsigned char ch) {
            return !std::isspace(ch);
        }));
        trimmed.erase(std::find_if(trimmed.rbegin(), trimmed.rend(), [](unsigned char ch) {
            return !std::isspace(ch);
        }).base(), trimmed.end());
        if (trimmed.empty()) throw std::runtime_error("NamespacedStore requires a namespace");
        prefix_ = "ns:" + trimmed + ":";
    }

    std::optional<json> get(const std::string &key) override { return base_->get(prefix_ + key); }
    void put(const std::string &key, const json &value) override { base_->put(prefix_ + key, value); }
    void del(const std::string &key) override { base_->del(prefix_ + key); }
    std::vector<std::pair<std::string, json>> entries(const std::string &prefix) override {
        auto all = base_->entries(prefix_ + prefix);
        std::vector<std::pair<std::string, json>> out;
        for (auto &kv : all) {
            if (kv.first.rfind(prefix_, 0) == 0) {
                out.push_back({kv.first.substr(prefix_.size()), kv.second});
            }
        }
        return out;
    }

private:
    std::shared_ptr<KeyValueStore> base_;
    std::string prefix_;
};

// ------------------ Core model ------------------

class RuntimeState;
struct RobotsDocument;

class RuntimeBase {
public:
    virtual ~RuntimeBase() = default;
    virtual json processInput(const json &payload) = 0;
    virtual json cloneParams() const = 0;
    virtual void setParams(const json &patch) = 0;
    virtual json toSnapshot() = 0;
    virtual void fromSnapshot(const json &snapshot) = 0;
    virtual json ingestDocument(const json &doc) = 0;
    virtual json forgetMemes(const json &payload) = 0;
    virtual json onlineLookup(const std::string &query, const json &options) = 0;
    virtual json onlineLookup(const json &input, const json &options) = 0;
    virtual json getSearchConfig() const = 0;
    virtual json setSearchConfig(const json &patch) = 0;
    virtual std::vector<RobotsDocument> collectRobotsDocuments(const json &options) const = 0;
    virtual std::vector<std::string> listRobotsFiles() const = 0;
    virtual fs::path exportGraphToFile(const json &options) = 0;
    virtual json learnFromDialog(const json &payload, const json &result) = 0;
    virtual std::unordered_map<std::string, double> mapWordsToMemes(const std::vector<std::string> &words) = 0;
    virtual std::vector<std::string> getMemeWords(const std::string &memeId) = 0;
    virtual void graphLink(const std::string &fromId, const std::string &toId, double weight, int direction) = 0;
    virtual const Config &config() const = 0;
};

struct SearchConfig {
    bool enabled{false};
    std::vector<std::string> endpoints;
    std::string active;

    json toJson() const {
        return json{{"enabled", enabled}, {"endpoints", endpoints}, {"active", active}};
    }
};

struct ModelDefaults {
    double decayFactor{0.5};
    int maxMemeWords{100};
    int minOverlapThreshold{2};
    int memeNgramMin{3};
    int memeNgramMax{14};
    double maliciousThreshold{0.7};
    int learningIterations{3};
    int iteration{5};
    double threshold{3};
    double decay{1};
    double decayK{1};
    int maxLen{16};
    double edgeWeight{1};
    std::string activationType{"relu"};
    std::string transferType{"linear"};
    std::string activationCustom;
    std::string transferCustom;
    int mappingDepth{1};
    int reflectionTopMemes{18};
    int reflectionTopWords{24};
    double reflectionMinScore{1e-6};
};

static ModelDefaults MODEL_DEFAULTS;

class OnlineResearcher {
public:
    OnlineResearcher(RuntimeState *runtime, const std::string &endpoint, int cooldownMs = 5 * 60 * 1000, int cacheSize = 128)
        : runtime_(runtime), endpoint_(endpoint), cooldownMs_(cooldownMs), cacheSize_(cacheSize) {}

    void setEndpoint(const std::string &endpoint) { endpoint_ = endpoint; }
    void setEnabled(bool enabled) { enabled_ = enabled; }

    json lookup(const json &input, const json &options);

private:
    struct CacheEntry { int64_t ts{0}; json payload; };

    RuntimeState *runtime_{nullptr};
    std::string endpoint_;
    int cooldownMs_{5 * 60 * 1000};
    int cacheSize_{128};
    bool enabled_{true};
    std::unordered_map<std::string, CacheEntry> cache_;

    std::string normalizeKey(const std::vector<std::string> &words) const;
    void pruneCache();
    void remember(const std::string &key, const json &payload);
    std::optional<json> fromCache(const std::string &key);
    json fallback(const std::vector<std::string> &words);
    json crawl(const std::string &startUrl, const json &crawlReq, const json &options);
};
static double relu(double x) { return x > 0 ? x : 0; }
static double leaky_relu(double x) { return x > 0 ? x : 0.01 * x; }
static double sigmoid(double x) { return 1.0 / (1.0 + std::exp(-x)); }
static double gelu(double x) { return 0.5 * x * (1 + std::tanh(std::sqrt(2 / M_PI) * (x + 0.044715 * std::pow(x, 3)))); }
static double identity_fn(double x) { return x; }

struct ExprToken {
    enum class Type { Number, Var, Op, Func, LParen, RParen, Comma } type{Type::Number};
    std::string text;
    double value{0.0};
    int precedence{0};
    bool rightAssoc{false};
    int arity{0};
};

static bool isIdentChar(char c) {
    return std::isalnum((unsigned char)c) || c == '_' || c == '.';
}

static int funcArity(const std::string &name) {
    if (name == "min" || name == "max" || name == "pow") return 2;
    return 1;
}

static bool isFuncName(const std::string &name) {
    static const std::unordered_set<std::string> funcs = {
        "sin", "cos", "tan", "tanh", "exp", "log", "sqrt", "abs", "min", "max", "pow"
    };
    return funcs.count(name) > 0;
}

static std::vector<ExprToken> tokenizeExpr(const std::string &expr, bool &ok) {
    ok = true;
    std::vector<ExprToken> out;
    for (size_t i = 0; i < expr.size();) {
        char c = expr[i];
        if (std::isspace((unsigned char)c)) { i++; continue; }
        if (std::isdigit((unsigned char)c) || c == '.') {
            size_t j = i + 1;
            while (j < expr.size() && (std::isdigit((unsigned char)expr[j]) || expr[j] == '.' || expr[j] == 'e' || expr[j] == 'E' || expr[j] == '+' || expr[j] == '-')) {
                if ((expr[j] == '+' || expr[j] == '-') && !(expr[j - 1] == 'e' || expr[j - 1] == 'E')) break;
                j++;
            }
            double v = 0.0;
            try { v = std::stod(expr.substr(i, j - i)); } catch (...) { ok = false; return {}; }
            out.push_back(ExprToken{ExprToken::Type::Number, "", v});
            i = j;
            continue;
        }
        if (std::isalpha((unsigned char)c) || c == '_') {
            size_t j = i + 1;
            while (j < expr.size() && isIdentChar(expr[j])) j++;
            std::string name = expr.substr(i, j - i);
            if (name == "x") {
                out.push_back(ExprToken{ExprToken::Type::Var, name, 0.0});
            } else if (name == "pi") {
                out.push_back(ExprToken{ExprToken::Type::Number, "", M_PI});
            } else if (name == "e") {
                out.push_back(ExprToken{ExprToken::Type::Number, "", std::exp(1.0)});
            } else if (isFuncName(name)) {
                out.push_back(ExprToken{ExprToken::Type::Func, name, 0.0, 0, false, funcArity(name)});
            } else {
                ok = false;
                return {};
            }
            i = j;
            continue;
        }
        if (c == '(') { out.push_back(ExprToken{ExprToken::Type::LParen}); i++; continue; }
        if (c == ')') { out.push_back(ExprToken{ExprToken::Type::RParen}); i++; continue; }
        if (c == ',') { out.push_back(ExprToken{ExprToken::Type::Comma}); i++; continue; }
        if (c == '+' || c == '-' || c == '*' || c == '/' || c == '^') {
            ExprToken tok;
            tok.type = ExprToken::Type::Op;
            tok.text = std::string(1, c);
            if (c == '^') { tok.precedence = 3; tok.rightAssoc = true; }
            else if (c == '*' || c == '/') { tok.precedence = 2; }
            else { tok.precedence = 1; }
            out.push_back(tok);
            i++;
            continue;
        }
        ok = false;
        return {};
    }
    return out;
}

static std::vector<ExprToken> toRpn(const std::vector<ExprToken> &tokens, bool &ok) {
    ok = true;
    std::vector<ExprToken> output;
    std::vector<ExprToken> ops;
    ExprToken prev;
    bool hasPrev = false;
    for (size_t i = 0; i < tokens.size(); i++) {
        auto tok = tokens[i];
        if (tok.type == ExprToken::Type::Op && tok.text == "-") {
            if (!hasPrev || prev.type == ExprToken::Type::Op || prev.type == ExprToken::Type::LParen || prev.type == ExprToken::Type::Comma) {
                tok.text = "neg";
                tok.precedence = 4;
                tok.rightAssoc = true;
                tok.arity = 1;
            }
        }
        if (tok.type == ExprToken::Type::Number || tok.type == ExprToken::Type::Var) {
            output.push_back(tok);
        } else if (tok.type == ExprToken::Type::Func) {
            ops.push_back(tok);
        } else if (tok.type == ExprToken::Type::Comma) {
            while (!ops.empty() && ops.back().type != ExprToken::Type::LParen) {
                output.push_back(ops.back());
                ops.pop_back();
            }
            if (ops.empty()) { ok = false; return {}; }
        } else if (tok.type == ExprToken::Type::Op) {
            while (!ops.empty()) {
                auto top = ops.back();
                if (top.type == ExprToken::Type::Op) {
                    if ((tok.rightAssoc && tok.precedence < top.precedence) || (!tok.rightAssoc && tok.precedence <= top.precedence)) {
                        output.push_back(top);
                        ops.pop_back();
                        continue;
                    }
                } else if (top.type == ExprToken::Type::Func) {
                    output.push_back(top);
                    ops.pop_back();
                    continue;
                }
                break;
            }
            ops.push_back(tok);
        } else if (tok.type == ExprToken::Type::LParen) {
            ops.push_back(tok);
        } else if (tok.type == ExprToken::Type::RParen) {
            bool matched = false;
            while (!ops.empty()) {
                auto top = ops.back();
                ops.pop_back();
                if (top.type == ExprToken::Type::LParen) { matched = true; break; }
                output.push_back(top);
            }
            if (!matched) { ok = false; return {}; }
            if (!ops.empty() && ops.back().type == ExprToken::Type::Func) {
                output.push_back(ops.back());
                ops.pop_back();
            }
        }
        prev = tok;
        hasPrev = true;
    }
    while (!ops.empty()) {
        if (ops.back().type == ExprToken::Type::LParen || ops.back().type == ExprToken::Type::RParen) { ok = false; return {}; }
        output.push_back(ops.back());
        ops.pop_back();
    }
    return output;
}

static double evalRpn(const std::vector<ExprToken> &rpn, double x, bool &ok) {
    ok = true;
    std::vector<double> st;
    st.reserve(rpn.size());
    for (const auto &tok : rpn) {
        if (tok.type == ExprToken::Type::Number) {
            st.push_back(tok.value);
        } else if (tok.type == ExprToken::Type::Var) {
            st.push_back(x);
        } else if (tok.type == ExprToken::Type::Op) {
            if (tok.text == "neg") {
                if (st.empty()) { ok = false; return 0.0; }
                double v = st.back(); st.pop_back();
                st.push_back(-v);
            } else {
                if (st.size() < 2) { ok = false; return 0.0; }
                double b = st.back(); st.pop_back();
                double a = st.back(); st.pop_back();
                if (tok.text == "+") st.push_back(a + b);
                else if (tok.text == "-") st.push_back(a - b);
                else if (tok.text == "*") st.push_back(a * b);
                else if (tok.text == "/") st.push_back(b == 0.0 ? 0.0 : a / b);
                else if (tok.text == "^") st.push_back(std::pow(a, b));
                else { ok = false; return 0.0; }
            }
        } else if (tok.type == ExprToken::Type::Func) {
            int arity = tok.arity ? tok.arity : funcArity(tok.text);
            if ((int)st.size() < arity) { ok = false; return 0.0; }
            double b = 0.0, a = 0.0;
            if (arity >= 2) { b = st.back(); st.pop_back(); a = st.back(); st.pop_back(); }
            else { a = st.back(); st.pop_back(); }
            double v = 0.0;
            if (tok.text == "sin") v = std::sin(a);
            else if (tok.text == "cos") v = std::cos(a);
            else if (tok.text == "tan") v = std::tan(a);
            else if (tok.text == "tanh") v = std::tanh(a);
            else if (tok.text == "exp") v = std::exp(a);
            else if (tok.text == "log") v = a <= 0 ? 0.0 : std::log(a);
            else if (tok.text == "sqrt") v = a < 0 ? 0.0 : std::sqrt(a);
            else if (tok.text == "abs") v = std::abs(a);
            else if (tok.text == "min") v = std::min(a, b);
            else if (tok.text == "max") v = std::max(a, b);
            else if (tok.text == "pow") v = std::pow(a, b);
            else { ok = false; return 0.0; }
            st.push_back(v);
        }
    }
    if (st.size() != 1 || !std::isfinite(st.back())) { ok = false; return 0.0; }
    return st.back();
}

static std::shared_ptr<std::vector<ExprToken>> compileExpression(const std::string &expr) {
    static std::mutex mu;
    static std::unordered_map<std::string, std::shared_ptr<std::vector<ExprToken>>> cache;
    if (expr.empty()) return nullptr;
    {
        std::lock_guard<std::mutex> lock(mu);
        auto it = cache.find(expr);
        if (it != cache.end()) return it->second;
    }
    bool ok = true;
    auto tokens = tokenizeExpr(expr, ok);
    if (!ok) return nullptr;
    auto rpn = toRpn(tokens, ok);
    if (!ok) return nullptr;
    auto ptr = std::make_shared<std::vector<ExprToken>>(std::move(rpn));
    {
        std::lock_guard<std::mutex> lock(mu);
        cache[expr] = ptr;
    }
    return ptr;
}

static std::function<double(double)> activationFn(const ModelDefaults &params) {
    if (params.activationType == "identity" || params.activationType == "linear") return identity_fn;
    if (params.activationType == "tanh") return [](double x) { return std::tanh(x); };
    if (params.activationType == "sigmoid") return sigmoid;
    if (params.activationType == "leaky_relu") return leaky_relu;
    if (params.activationType == "gelu") return gelu;
    if (params.activationType == "elu") return [](double x) { return x >= 0 ? x : (std::exp(x) - 1); };
    if (params.activationType == "softplus") return [](double x) { return std::log(1 + std::exp(x)); };
    if (params.activationType == "custom" && !params.activationCustom.empty()) {
        auto rpn = compileExpression(params.activationCustom);
        if (rpn) {
            return [rpn](double x) {
                bool ok = true;
                double v = evalRpn(*rpn, x, ok);
                return ok ? v : 0.0;
            };
        }
    }
    return relu;
}

struct CSRMatrix {
    std::vector<uint32_t> rowPtr;
    std::vector<uint32_t> colIdx;
    std::vector<float> values;
    uint32_t nRows{0};
    uint32_t nCols{0};
};

class TensorEngine {
public:
    std::vector<float> spmm(const CSRMatrix &csr, const std::vector<float> &x) {
        std::vector<float> out(csr.nRows, 0.0f);
        for (uint32_t row = 0; row < csr.nRows; row++) {
            float acc = 0;
            for (uint32_t idx = csr.rowPtr[row]; idx < csr.rowPtr[row + 1]; idx++) {
                uint32_t col = csr.colIdx[idx];
                float w = csr.values[idx];
                float xv = (col < x.size()) ? x[col] : 0;
                if (!std::isfinite(w) || !std::isfinite(xv)) continue;
                acc += w * xv;
            }
            out[row] = std::isfinite(acc) ? acc : 0;
        }
        return out;
    }

    std::vector<float> iteratePropagation(const CSRMatrix &csr, const std::vector<float> &seeds, int steps, const std::function<double(double)> &actFn, double decayK, double damp = 0.02) {
        std::vector<float> state = seeds;
        std::vector<float> next(seeds.size());
        for (int s = 0; s < steps; s++) {
            next = spmm(csr, state);
            for (size_t i = 0; i < next.size(); i++) {
                double si = std::isfinite(state[i]) ? state[i] : 0;
                double ni = std::isfinite(next[i]) ? next[i] : 0;
                double raw = si + ni - (decayK * si * damp);
                double y = 0;
                try { y = actFn(raw); } catch (...) { y = 0; }
                state[i] = std::isfinite(y) ? (float)y : 0.0f;
            }
        }
        for (auto &v : state) if (!std::isfinite(v)) v = 0;
        return state;
    }

    void l2NormalizeRows(std::vector<float> &data, size_t nRows, size_t nCols) {
        if (nRows == 0 || nCols == 0) return;
        for (size_t row = 0; row < nRows; row++) {
            double acc = 0.0;
            size_t base = row * nCols;
            for (size_t col = 0; col < nCols; col++) {
                double v = data[base + col];
                acc += v * v;
            }
            double norm = std::sqrt(acc);
            if (norm <= 0) continue;
            for (size_t col = 0; col < nCols; col++) {
                data[base + col] = (float)(data[base + col] / norm);
            }
        }
    }
};

class KVMStore {
public:
    explicit KVMStore(std::shared_ptr<KeyValueStore> store, int maxCacheEntries)
        : store_(std::move(store)), maxCacheEntries_(maxCacheEntries) {}

    std::vector<std::string> getWordMemeSet(const std::string &word) {
        std::string key = "word:" + word;
        auto cached = cacheGet(key);
        if (cached) return cached->order;
        auto out = loadOrdered(key);
        return out.order;
    }

    std::vector<std::string> getMemeWords(const std::string &memeId) {
        std::string key = "meme:" + memeId;
        auto cached = cacheGet(key);
        if (cached) return cached->order;
        auto out = loadOrdered(key);
        return out.order;
    }

    void link(const std::string &word, const std::string &memeId) {
        std::string wordKey = "word:" + word;
        std::string memeKey = "meme:" + memeId;
        auto wordEntry = loadOrdered(wordKey);
        auto memeEntry = loadOrdered(memeKey);
        if (!wordEntry.set.count(memeId)) {
            wordEntry.set.insert(memeId);
            wordEntry.order.push_back(memeId);
            store_->put(wordKey, json(wordEntry.order));
            cacheSet(wordKey, wordEntry);
        }
        if (!memeEntry.set.count(word)) {
            memeEntry.set.insert(word);
            memeEntry.order.push_back(word);
            store_->put(memeKey, json(memeEntry.order));
            cacheSet(memeKey, memeEntry);
        }
    }

    void unlink(const std::string &word, const std::string &memeId) {
        std::string wordKey = "word:" + word;
        std::string memeKey = "meme:" + memeId;
        auto wordEntry = loadOrdered(wordKey);
        auto memeEntry = loadOrdered(memeKey);
        if (wordEntry.set.erase(memeId)) {
            auto it = std::find(wordEntry.order.begin(), wordEntry.order.end(), memeId);
            if (it != wordEntry.order.end()) wordEntry.order.erase(it);
            store_->put(wordKey, json(wordEntry.order));
            cacheSet(wordKey, wordEntry);
        }
        if (memeEntry.set.erase(word)) {
            auto it = std::find(memeEntry.order.begin(), memeEntry.order.end(), word);
            if (it != memeEntry.order.end()) memeEntry.order.erase(it);
            store_->put(memeKey, json(memeEntry.order));
            cacheSet(memeKey, memeEntry);
        }
    }

    json exportEntries() {
        json out;
        json words = json::object();
        json memes = json::object();
        for (auto &kv : cache_) {
            if (kv.first.rfind("word:", 0) == 0) {
                words[kv.first.substr(5)] = json(kv.second.value.order);
            } else if (kv.first.rfind("meme:", 0) == 0) {
                memes[kv.first.substr(5)] = json(kv.second.value.order);
            }
        }
        out["words"] = words;
        out["memes"] = memes;
        return out;
    }

private:
    std::shared_ptr<KeyValueStore> store_;
    struct OrderedSet {
        std::vector<std::string> order;
        std::unordered_set<std::string> set;
    };
    struct CacheEntry {
        OrderedSet value;
        std::list<std::string>::iterator it;
    };
    std::unordered_map<std::string, CacheEntry> cache_;
    std::list<std::string> lru_;
    int maxCacheEntries_{50000};

    std::optional<OrderedSet> cacheGet(const std::string &key) {
        if (maxCacheEntries_ <= 0) return std::nullopt;
        auto it = cache_.find(key);
        if (it == cache_.end()) return std::nullopt;
        lru_.erase(it->second.it);
        lru_.push_back(key);
        it->second.it = std::prev(lru_.end());
        return it->second.value;
    }

    void cacheSet(const std::string &key, const OrderedSet &value) {
        if (maxCacheEntries_ <= 0) return;
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            it->second.value = value;
            lru_.erase(it->second.it);
            lru_.push_back(key);
            it->second.it = std::prev(lru_.end());
        } else {
            lru_.push_back(key);
            cache_[key] = CacheEntry{value, std::prev(lru_.end())};
        }
        while ((int)cache_.size() > maxCacheEntries_) {
            auto oldest = lru_.front();
            lru_.pop_front();
            cache_.erase(oldest);
        }
    }

    OrderedSet loadOrdered(const std::string &key) {
        auto cached = cacheGet(key);
        if (cached) return *cached;
        OrderedSet out;
        auto v = store_->get(key);
        if (v && v->is_array()) {
            for (auto &x : *v) {
                if (!x.is_string()) continue;
                std::string s = x.get<std::string>();
                if (out.set.insert(s).second) out.order.push_back(s);
            }
        }
        cacheSet(key, out);
        return out;
    }
};

class MemeGraph {
public:
    explicit MemeGraph(std::shared_ptr<KeyValueStore> store) : store_(std::move(store)) {}

    void ensureNode(const std::string &memeId) {
        if (!nodes_.count(memeId)) {
            nodes_[memeId] = NodeEdges{};
            nodeOrder_.push_back(memeId);
        }
        if (!meta_.count(memeId)) {
            json meta;
            meta["degree"] = 0;
            meta["lastTouched"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();
            meta_[memeId] = meta;
            metaOrder_.push_back(memeId);
            store_->put("node:" + memeId, meta);
        }
        if (!store_->get("row:" + memeId)) {
            store_->put("row:" + memeId, json{{"neighbors", json::array()}});
        }
    }

    void link(const std::string &fromId, const std::string &toId, double weight = 1, int direction = 0) {
        if (fromId == toId) return;
        ensureNode(fromId);
        ensureNode(toId);
        auto &table = nodes_[fromId];
        auto it = table.map.find(toId);
        if (it == table.map.end()) {
            table.order.push_back(toId);
            table.map[toId] = {weight, direction};
        } else {
            it->second.weight += weight;
            it->second.direction = direction;
        }
        meta_[fromId]["degree"] = (int)table.map.size();
        meta_[fromId]["lastTouched"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();
        persistNode(fromId);
        if (direction == 0) {
            auto &back = nodes_[toId];
            auto it2 = back.map.find(fromId);
            if (it2 == back.map.end()) {
                back.order.push_back(fromId);
                back.map[fromId] = {weight, direction};
            } else {
                it2->second.weight += weight;
                it2->second.direction = direction;
            }
            meta_[toId]["degree"] = (int)back.map.size();
            meta_[toId]["lastTouched"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();
            persistNode(toId);
        }
    }

    void decayEdge(const std::string &fromId, const std::string &toId, double factor) {
        auto table = ensureRowLoaded(fromId);
        if (!table) return;
        auto it = table->map.find(toId);
        if (it == table->map.end()) return;
        it->second.weight *= factor;
        if (it->second.weight < 1e-5) {
            table->map.erase(it);
            table->order.erase(std::remove(table->order.begin(), table->order.end(), toId), table->order.end());
        }
        meta_[fromId]["degree"] = (int)table->map.size();
        meta_[fromId]["lastTouched"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();
        persistNode(fromId);
    }

    std::vector<std::string> getAllPoints() {
        loadAll();
        std::vector<std::string> ids;
        ids.reserve(metaOrder_.size());
        for (auto &id : metaOrder_) {
            if (meta_.count(id)) ids.push_back(id);
        }
        return ids;
    }

    struct WindowInfo {
        CSRMatrix csr;
        std::unordered_map<std::string, uint32_t> index;
        std::vector<std::string> ids;
    };

    WindowInfo buildWindow(const std::vector<std::string> &seedIds, int radius = 1) {
        std::unordered_set<std::string> visited;
        std::vector<std::string> ids;
        ids.reserve(seedIds.size());
        for (auto &id : seedIds) {
            if (visited.insert(id).second) ids.push_back(id);
        }
        std::vector<std::string> border = ids;
        int depth = 0;
        while (!border.empty() && visited.size() < 4096 && depth < radius) {
            std::vector<std::string> next;
            for (const auto &id : border) {
                auto table = ensureRowLoaded(id);
                if (!table) continue;
                for (auto &to : table->order) {
                    if (visited.insert(to).second) {
                        next.push_back(to);
                        ids.push_back(to);
                    }
                }
            }
            border = next;
            depth++;
        }
        std::unordered_map<std::string, uint32_t> index;
        for (uint32_t i = 0; i < ids.size(); i++) index[ids[i]] = i;
        CSRMatrix csr;
        csr.nRows = csr.nCols = (uint32_t)ids.size();
        csr.rowPtr.resize(ids.size() + 1);
        for (uint32_t i = 0; i < ids.size(); i++) {
            auto table = ensureRowLoaded(ids[i]);
            csr.rowPtr[i] = (uint32_t)csr.colIdx.size();
            if (table) {
                for (auto &to : table->order) {
                    auto itEdge = table->map.find(to);
                    if (itEdge == table->map.end()) continue;
                    auto it = index.find(to);
                    if (it == index.end()) continue;
                    csr.colIdx.push_back(it->second);
                    csr.values.push_back((float)itEdge->second.weight);
                }
            }
        }
        csr.rowPtr[ids.size()] = (uint32_t)csr.colIdx.size();
        return {csr, index, ids};
    }

    std::vector<std::vector<float>> messagePassingEmbeddings(const std::vector<std::string> &ids,
                                                             const std::vector<std::vector<float>> &baseEmb,
                                                             int rounds = 1,
                                                             float selfWeight = 0.6f,
                                                             float neighborWeight = 0.4f) {
        if (ids.empty() || baseEmb.empty()) return baseEmb;
        std::unordered_map<std::string, size_t> index;
        for (size_t i = 0; i < ids.size(); i++) index[ids[i]] = i;
        std::vector<std::vector<float>> cur = baseEmb;
        for (int r = 0; r < rounds; r++) {
            std::vector<std::vector<float>> next = cur;
            for (size_t i = 0; i < ids.size(); i++) {
                auto table = ensureRowLoaded(ids[i]);
                if (!table) continue;
                std::vector<float> agg(cur[i].size(), 0.0f);
                double wsum = 0.0;
                for (auto &to : table->order) {
                    auto it = table->map.find(to);
                    if (it == table->map.end()) continue;
                    auto itIdx = index.find(to);
                    if (itIdx == index.end()) continue;
                    double w = it->second.weight;
                    if (it->second.direction == 1) w *= 0.85; // inbound
                    else if (it->second.direction == 2) w *= 1.15; // outbound
                    for (size_t d = 0; d < agg.size() && d < cur[itIdx->second].size(); d++) {
                        agg[d] += (float)(w * cur[itIdx->second][d]);
                    }
                    wsum += std::abs(w);
                }
                if (wsum > 0.0) {
                    for (auto &v : agg) v = (float)(v / wsum);
                }
                for (size_t d = 0; d < next[i].size() && d < agg.size(); d++) {
                    next[i][d] = selfWeight * cur[i][d] + neighborWeight * agg[d];
                }
                float norm = 0.0f;
                for (float v : next[i]) norm += v * v;
                norm = std::sqrt(norm);
                if (norm > 0.0f) for (auto &v : next[i]) v /= norm;
            }
            cur = std::move(next);
        }
        return cur;
    }

    json exportSnapshot() {
        loadAll();
        json nodes = json::array();
        for (auto &id : nodeOrder_) {
            auto it = nodes_.find(id);
            if (it == nodes_.end()) continue;
            json neighbors = json::array();
            for (auto &to : it->second.order) {
                auto itEdge = it->second.map.find(to);
                if (itEdge == it->second.map.end()) continue;
                neighbors.push_back({{"to", to}, {"weight", itEdge->second.weight}, {"direction", itEdge->second.direction}});
            }
            nodes.push_back({{"id", id}, {"neighbors", neighbors}});
        }
        json meta = json::object();
        for (auto &id : metaOrder_) {
            auto it = meta_.find(id);
            if (it != meta_.end()) meta[id] = it->second;
        }
        return { {"nodes", nodes}, {"meta", meta} };
    }

    std::vector<std::pair<std::string, std::pair<double, int>>> neighborsOf(const std::string &memeId) {
        auto table = ensureRowLoaded(memeId);
        std::vector<std::pair<std::string, std::pair<double, int>>> out;
        if (!table) return out;
        for (auto &to : table->order) {
            auto it = table->map.find(to);
            if (it == table->map.end()) continue;
            out.push_back({to, {it->second.weight, it->second.direction}});
        }
        return out;
    }

    int degreeOf(const std::string &memeId) {
        auto table = ensureRowLoaded(memeId);
        return table ? (int)table->map.size() : 0;
    }

    int removeOutgoingEdges(const std::string &memeId) {
        auto tablePtr = ensureRowLoaded(memeId);
        if (!tablePtr) return 0;
        auto &table = nodes_[memeId];
        int cut = 0;
        for (auto it = table.order.begin(); it != table.order.end();) {
            auto itEdge = table.map.find(*it);
            if (itEdge != table.map.end() && itEdge->second.direction == 2) {
                std::string targetId = *it;
                table.map.erase(itEdge);
                it = table.order.erase(it);
                cut++;
                auto targetTable = ensureRowLoaded(targetId);
                if (targetTable) {
                    auto &t = nodes_[targetId];
                    for (auto it2 = t.order.begin(); it2 != t.order.end();) {
                        auto itEdge2 = t.map.find(*it2);
                        if (itEdge2 != t.map.end() && *it2 == memeId && itEdge2->second.direction == 1) {
                            t.map.erase(itEdge2);
                            it2 = t.order.erase(it2);
                        } else {
                            ++it2;
                        }
                    }
                    meta_[targetId]["degree"] = (int)t.map.size();
                    meta_[targetId]["lastTouched"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();
                    persistNode(targetId);
                }
            } else {
                ++it;
            }
        }
        meta_[memeId]["degree"] = (int)table.map.size();
        meta_[memeId]["lastTouched"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();
        persistNode(memeId);
        return cut;
    }
    bool removeNode(const std::string &memeId) {
        loadAll();
        if (!nodes_.count(memeId)) return false;
        nodes_.erase(memeId);
        store_->del("row:" + memeId);
        meta_.erase(memeId);
        store_->del("node:" + memeId);
        nodeOrder_.erase(std::remove(nodeOrder_.begin(), nodeOrder_.end(), memeId), nodeOrder_.end());
        metaOrder_.erase(std::remove(metaOrder_.begin(), metaOrder_.end(), memeId), metaOrder_.end());
        for (auto &kv : nodes_) {
            auto &table = kv.second;
            if (table.map.erase(memeId)) {
                table.order.erase(std::remove(table.order.begin(), table.order.end(), memeId), table.order.end());
                meta_[kv.first]["degree"] = (int)table.map.size();
                meta_[kv.first]["lastTouched"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();
                persistNode(kv.first);
            }
        }
        return true;
    }

    json getMeta(const std::string &memeId) {
        loadAll();
        auto it = meta_.find(memeId);
        return it == meta_.end() ? json::object() : it->second;
    }

    void setMeta(const std::string &memeId, const json &meta) {
        if (!meta_.count(memeId)) metaOrder_.push_back(memeId);
        meta_[memeId] = meta;
        store_->put("node:" + memeId, meta);
    }

    std::vector<std::pair<std::string, json>> metaEntries() {
        loadAll();
        std::vector<std::pair<std::string, json>> out;
        out.reserve(meta_.size());
        for (auto &id : metaOrder_) {
            auto it = meta_.find(id);
            if (it != meta_.end()) out.push_back({id, it->second});
        }
        return out;
    }

    void importSnapshot(const json &snapshot) {
        nodes_.clear();
        meta_.clear();
        nodeOrder_.clear();
        metaOrder_.clear();
        for (auto &item : snapshot.value("nodes", json::array())) {
            std::string id = item.value("id", "");
            if (id.empty()) continue;
            NodeEdges node;
            for (auto &n : item.value("neighbors", json::array())) {
                std::string to = n.value("to", "");
                if (to.empty()) continue;
                if (node.map.find(to) == node.map.end()) node.order.push_back(to);
                node.map[to] = {n.value("weight", 0.0), n.value("direction", 0)};
            }
            nodes_[id] = std::move(node);
            nodeOrder_.push_back(id);
            persistNode(id);
        }
        for (auto it = snapshot.value("meta", json::object()).begin(); it != snapshot.value("meta", json::object()).end(); ++it) {
            meta_[it.key()] = it.value();
            metaOrder_.push_back(it.key());
            store_->put("node:" + it.key(), it.value());
        }
    }

private:
    struct Edge { double weight{0}; int direction{0}; };
    struct NodeEdges {
        std::vector<std::string> order;
        std::unordered_map<std::string, Edge> map;
    };
    std::shared_ptr<KeyValueStore> store_;
    std::unordered_map<std::string, NodeEdges> nodes_;
    std::vector<std::string> nodeOrder_;
    std::unordered_map<std::string, json> meta_;
    std::vector<std::string> metaOrder_;
    bool fullyLoaded_{false};

    void loadAll() {
        if (fullyLoaded_) return;
        auto metaEntries = store_->entries("node:");
        for (auto &kv : metaEntries) {
            std::string id = kv.first.substr(5);
            meta_[id] = kv.second;
            if (std::find(metaOrder_.begin(), metaOrder_.end(), id) == metaOrder_.end()) metaOrder_.push_back(id);
        }
        auto rowEntries = store_->entries("row:");
        for (auto &kv : rowEntries) {
            std::string id = kv.first.substr(4);
            NodeEdges node;
            for (auto &n : kv.second.value("neighbors", json::array())) {
                std::string to = n.value("to", "");
                if (to.empty()) continue;
                if (node.map.find(to) == node.map.end()) node.order.push_back(to);
                node.map[to] = {n.value("weight", 0.0), n.value("direction", 0)};
            }
            if (std::find(nodeOrder_.begin(), nodeOrder_.end(), id) == nodeOrder_.end()) nodeOrder_.push_back(id);
            nodes_[id] = std::move(node);
        }
        fullyLoaded_ = true;
    }

    NodeEdges *ensureRowLoaded(const std::string &id) {
        auto it = nodes_.find(id);
        if (it != nodes_.end()) return &it->second;
        auto row = store_->get("row:" + id);
        if (row && row->is_object()) {
            NodeEdges node;
            for (auto &n : row->value("neighbors", json::array())) {
                std::string to = n.value("to", "");
                if (to.empty()) continue;
                if (node.map.find(to) == node.map.end()) node.order.push_back(to);
                node.map[to] = {n.value("weight", 0.0), n.value("direction", 0)};
            }
            nodes_[id] = std::move(node);
            if (std::find(nodeOrder_.begin(), nodeOrder_.end(), id) == nodeOrder_.end()) nodeOrder_.push_back(id);
            return &nodes_[id];
        }
        return nullptr;
    }

    void persistNode(const std::string &id) {
        auto &table = nodes_[id];
        json neighbors = json::array();
        for (auto &to : table.order) {
            auto it = table.map.find(to);
            if (it == table.map.end()) continue;
            neighbors.push_back({{"to", to}, {"weight", it->second.weight}, {"direction", it->second.direction}});
        }
        store_->put("row:" + id, json{{"neighbors", neighbors}});
    }
};

class GraphExportBuilder {
public:
    static json fromWindow(const MemeGraph::WindowInfo &windowInfo, const std::vector<float> *activation = nullptr) {
        const auto &ids = windowInfo.ids;
        int n = (int)ids.size();
        json nodes = json::array();
        json edges = json::array();
        for (int i = 0; i < n; i++) {
            double ax = (activation && i < (int)activation->size() && std::isfinite((*activation)[i])) ? (*activation)[i] : 0.0;
            double ay = ((i * 9973) % 101) / 100.0;
            nodes.push_back({
                {"id", ids[i]},
                {"x", ax}, {"y", ay}, {"z", 0.0},
                {"value", ax},
                {"attrs", json::object()}
            });
        }
        const auto &csr = windowInfo.csr;
        for (uint32_t r = 0; r < csr.nRows; r++) {
            uint32_t start = csr.rowPtr[r];
            uint32_t end = csr.rowPtr[r + 1];
            for (uint32_t k = start; k < end; k++) {
                uint32_t c = csr.colIdx[k];
                double w = (k < csr.values.size()) ? csr.values[k] : 0.0;
                if (r < ids.size() && c < ids.size()) {
                    edges.push_back({
                        {"from", ids[r]},
                        {"to", ids[c]},
                        {"weight", std::isfinite(w) ? w : 0.0},
                        {"dir", 1}
                    });
                }
            }
        }
        return json{{"Nodes", nodes}, {"Edges", edges}};
    }
};

class GraphTensorBridge {
public:
    explicit GraphTensorBridge(KVMStore *kvm, TensorEngine *tensor)
        : kvm_(kvm), tensor_(tensor) {}

    std::vector<std::vector<float>> computeNodeEmbeddings(const std::vector<std::string> &ids, int D = 512) {
        rebuildRowIndex(ids);
        std::vector<std::vector<float>> embeddings(ids.size(), std::vector<float>(D, 0.0f));
        for (size_t i = 0; i < ids.size(); i++) {
            auto words = kvm_->getMemeWords(ids[i]);
            if (!words.empty()) {
                std::string joined;
                for (auto &word : words) {
                    if (word.empty()) continue;
                    if (!joined.empty()) joined.push_back(' ');
                    joined += word;
                }
                if (!joined.empty()) {
                    auto base = textToMiniEmbedding(joined, D);
                    if (base.size() == (size_t)D) {
                        embeddings[i] = std::move(base);
                        continue;
                    }
                }
            }
            uint32_t idx = fnv1a32(ids[i]) % (uint32_t)D;
            embeddings[i][idx] += 1.0f;
        }
        // l2NormalizeRows for std::vector<std::vector<float>>
        for (auto &vec : embeddings) {
            float norm = 0.0f;
            for (float v : vec) norm += v * v;
            norm = std::sqrt(norm);
            if (norm > 0.0f) {
                for (float &v : vec) v /= norm;
            }
        }
        return embeddings;
    }

    static uint32_t fnv1a32(const std::string &str) {
        uint32_t hash = 0x811c9dc5u;
        for (unsigned char c : str) {
            hash ^= c;
            hash *= 0x01000193u;
        }
        return hash;
    }

    void rebuildRowIndex(const std::vector<std::string> &ids) {
        memeIndex_.clear();
        for (size_t i = 0; i < ids.size(); i++) memeIndex_[ids[i]] = (uint32_t)i;
    }

    json buildEmbeddings(const std::vector<std::string> &ids, int D = 512) {
        rebuildRowIndex(ids);
        std::vector<float> mat(ids.size() * (size_t)D, 0.0f);
        for (size_t i = 0; i < ids.size(); i++) {
            auto words = kvm_->getMemeWords(ids[i]);
            if (words.empty()) continue;
            for (auto &word : words) {
                uint32_t idx = fnv1a32(word) % (uint32_t)D;
                mat[i * (size_t)D + idx] += 1.0f;
            }
        }
        tensor_->l2NormalizeRows(mat, ids.size(), (size_t)D);
        embeddings_ = json{{"data", mat}, {"nRows", (int)ids.size()}, {"nCols", D}};
        return embeddings_;
    }

    json buildMultiOrderCSR(const MemeGraph::WindowInfo &graphWindow) {
        const auto &csr = graphWindow.csr;
        const auto &ids = graphWindow.ids;
        std::vector<uint32_t> biRow(csr.rowPtr.size(), 0);
        std::vector<uint32_t> biCol;
        std::vector<float> biVal;
        std::vector<uint32_t> inRow(csr.rowPtr.size(), 0);
        std::vector<uint32_t> inCol;
        std::vector<float> inVal;

        std::vector<std::unordered_map<uint32_t, float>> adjacency(ids.size());
        for (uint32_t row = 0; row < ids.size(); row++) {
            uint32_t start = csr.rowPtr[row];
            uint32_t end = csr.rowPtr[row + 1];
            for (uint32_t idx = start; idx < end; idx++) {
                uint32_t col = csr.colIdx[idx];
                float weight = csr.values[idx];
                adjacency[row][col] = weight;
            }
        }

        uint32_t biCounter = 0;
        for (uint32_t row = 0; row < ids.size(); row++) {
            biRow[row] = biCounter;
            uint32_t start = csr.rowPtr[row];
            uint32_t end = csr.rowPtr[row + 1];
            for (uint32_t idx = start; idx < end; idx++) {
                uint32_t col = csr.colIdx[idx];
                float weight = csr.values[idx];
                auto it = adjacency[col].find(row);
                if (it != adjacency[col].end() && it->second != 0.0f) {
                    biCol.push_back(col);
                    biVal.push_back((weight + it->second) * 0.5f);
                    biCounter++;
                }
                inCol.push_back(row);
                inVal.push_back(weight);
            }
        }
        biRow[ids.size()] = biCounter;
        inRow[ids.size()] = (uint32_t)inCol.size();

        json all = json{{"rowPtr", csr.rowPtr}, {"colIdx", csr.colIdx}, {"values", csr.values}, {"nRows", csr.nRows}, {"nCols", csr.nCols}};
        json bi = json{{"rowPtr", biRow}, {"colIdx", biCol}, {"values", biVal}, {"nRows", (int)ids.size()}, {"nCols", (int)ids.size()}};
        json inbound = json{{"rowPtr", inRow}, {"colIdx", inCol}, {"values", inVal}, {"nRows", (int)ids.size()}, {"nCols", (int)ids.size()}};
        multi_ = json{{"all", all}, {"bi", bi}, {"out", all}, {"in", inbound}, {"ids", ids}};
        return multi_;
    }

private:
    KVMStore *kvm_{nullptr};
    TensorEngine *tensor_{nullptr};
    std::unordered_map<std::string, uint32_t> memeIndex_;
    json embeddings_ = json::object();
    json multi_ = json::object();
};

class DimReducer {
public:
    struct PCAResult { std::vector<float> coords; std::vector<float> v1; std::vector<float> v2; };

    PCAResult pca2D(const json &emb) {
        auto data = emb.value("data", std::vector<float>{});
        int nRows = emb.value("nRows", 0);
        int nCols = emb.value("nCols", 0);
        std::vector<float> mean(nCols, 0.0f);
        for (int row = 0; row < nRows; row++) {
            for (int col = 0; col < nCols; col++) {
                mean[col] += data[row * nCols + col];
            }
        }
        for (int col = 0; col < nCols; col++) mean[col] /= (nRows > 0 ? nRows : 1);

        std::vector<float> centered(data.size(), 0.0f);
        for (int row = 0; row < nRows; row++) {
            for (int col = 0; col < nCols; col++) {
                int idx = row * nCols + col;
                centered[idx] = data[idx] - mean[col];
            }
        }

        std::vector<std::vector<float>> cov(nCols, std::vector<float>(nCols, 0.0f));
        for (int row = 0; row < nRows; row++) {
            for (int i = 0; i < nCols; i++) {
                for (int j = i; j < nCols; j++) {
                    float v = centered[row * nCols + i] * centered[row * nCols + j];
                    cov[i][j] += v;
                    if (i != j) cov[j][i] += v;
                }
            }
        }

        auto powerIter = [&](const std::vector<float> &vec) {
            std::vector<float> next(nCols, 0.0f);
            for (int i = 0; i < nCols; i++) {
                double acc = 0.0;
                for (int j = 0; j < nCols; j++) acc += cov[i][j] * vec[j];
                next[i] = (float)acc;
            }
            double norm = 0.0;
            for (auto &v : next) norm += v * v;
            norm = std::sqrt(norm) ? std::sqrt(norm) : 1.0;
            for (auto &v : next) v = (float)(v / norm);
            return next;
        };

        std::mt19937 rng((unsigned)std::chrono::high_resolution_clock::now().time_since_epoch().count());
        std::uniform_real_distribution<float> dist(-0.5f, 0.5f);
        std::vector<float> v1(nCols, 0.0f);
        for (auto &v : v1) v = dist(rng);
        for (int i = 0; i < 8; i++) v1 = powerIter(v1);
        std::vector<float> v2(nCols, 0.0f);
        for (auto &v : v2) v = dist(rng);
        for (int i = 0; i < 8; i++) {
            double proj = 0.0;
            for (int j = 0; j < nCols; j++) proj += v2[j] * v1[j];
            for (int j = 0; j < nCols; j++) v2[j] = (float)(v2[j] - proj * v1[j]);
            v2 = powerIter(v2);
        }

        std::vector<float> coords(nRows * 2, 0.0f);
        for (int row = 0; row < nRows; row++) {
            double x = 0.0;
            double y = 0.0;
            for (int col = 0; col < nCols; col++) {
                float v = centered[row * nCols + col];
                x += v * v1[col];
                y += v * v2[col];
            }
            coords[row * 2] = (float)x;
            coords[row * 2 + 1] = (float)y;
        }
        return {coords, v1, v2};
    }

    json project2D(const json &emb, const std::string &method = "auto") {
        if (method == "umap") {
            auto coords2d = umap2D(emb, 15, 200);
            json coords = json::array();
            for (auto &c : coords2d) coords.push_back(json::array({c[0], c[1]}));
            return json{{"coords", coords}, {"method", "umap"}};
        }
        auto pca = pca2D(emb);
        int nRows = emb.value("nRows", 0);
        json coords = json::array();
        for (int row = 0; row < nRows; row++) {
            coords.push_back(json::array({pca.coords[row * 2], pca.coords[row * 2 + 1]}));
        }
        return json{{"coords", coords}, {"method", "pca"}};
    }

private:
    static std::vector<std::array<float, 2>> umap2D(const json &emb, int nNeighbors, int epochs) {
        int nRows = emb.value("nRows", 0);
        int nCols = emb.value("nCols", 0);
        if (nRows <= 0 || nCols <= 0) return {};
        std::vector<float> data;
        if (emb.contains("data") && emb["data"].is_array()) {
            data.reserve((size_t)nRows * (size_t)nCols);
            for (auto &v : emb["data"]) data.push_back(v.get<float>());
        }
        if ((int)data.size() != nRows * nCols) return {};

        int k = std::max(2, std::min(nNeighbors, nRows - 1));
        double target = std::log2((double)k);

        std::vector<std::vector<std::pair<int, float>>> knn(nRows);
        for (int i = 0; i < nRows; i++) {
            std::vector<std::pair<float, int>> dists;
            dists.reserve(nRows - 1);
            for (int j = 0; j < nRows; j++) {
                if (i == j) continue;
                double acc = 0.0;
                for (int c = 0; c < nCols; c++) {
                    double diff = data[i * nCols + c] - data[j * nCols + c];
                    acc += diff * diff;
                }
                dists.push_back({(float)std::sqrt(acc), j});
            }
            std::partial_sort(dists.begin(), dists.begin() + k, dists.end(), [](auto &a, auto &b) { return a.first < b.first; });
            for (int t = 0; t < k; t++) knn[i].push_back({dists[t].second, dists[t].first});
        }

        std::vector<float> rhos(nRows, 0.0f);
        std::vector<float> sigmas(nRows, 1.0f);
        for (int i = 0; i < nRows; i++) {
            float rho = 0.0f;
            for (auto &p : knn[i]) { if (p.second > 0) { rho = p.second; break; } }
            rhos[i] = rho;
            double lo = 1e-3, hi = 64.0;
            for (int it = 0; it < 32; it++) {
                double mid = (lo + hi) * 0.5;
                double sum = 0.0;
                for (auto &p : knn[i]) {
                    double d = std::max(0.0, (double)p.second - rho);
                    sum += std::exp(-d / mid);
                }
                if (sum > target) hi = mid; else lo = mid;
            }
            sigmas[i] = (float)((lo + hi) * 0.5);
            if (sigmas[i] <= 1e-6f) sigmas[i] = 1.0f;
        }

        std::vector<std::unordered_map<int, float>> weights(nRows);
        for (int i = 0; i < nRows; i++) {
            for (auto &p : knn[i]) {
                double d = std::max(0.0, (double)p.second - rhos[i]);
                double val = std::exp(-d / sigmas[i]);
                weights[i][p.first] = (float)val;
            }
        }

        struct Edge { int i; int j; float w; };
        std::vector<Edge> edges;
        edges.reserve(nRows * k);
        for (int i = 0; i < nRows; i++) {
            for (auto &kv : weights[i]) {
                int j = kv.first;
                if (i >= j) continue;
                float w1 = kv.second;
                float w2 = weights[j].count(i) ? weights[j][i] : 0.0f;
                float ws = w1 + w2 - w1 * w2;
                if (ws > 0.0f) edges.push_back({i, j, ws});
            }
        }

        auto find_ab = [&](double spread, double minDist) {
            double bestA = 1.0, bestB = 1.0, bestErr = 1e9;
            double x1 = minDist + spread;
            double x2 = minDist + 2.0 * spread;
            double t1 = std::exp(-1.0);
            double t2 = std::exp(-2.0);
            for (double b = 0.5; b <= 2.5; b += 0.01) {
                double a = (1.0 / t1 - 1.0) / std::pow(x1, 2.0 * b);
                if (!std::isfinite(a) || a <= 0) continue;
                double f2 = 1.0 / (1.0 + a * std::pow(x2, 2.0 * b));
                double err = std::abs(f2 - t2);
                if (err < bestErr) { bestErr = err; bestA = a; bestB = b; }
            }
            return std::pair<double, double>(bestA, bestB);
        };

        double minDist = 0.1;
        double spread = 1.0;
        auto ab = find_ab(spread, minDist);
        double a = ab.first;
        double b = ab.second;
        double gamma = 1.0;

        DimReducer reducer;
        auto pca = reducer.pca2D(emb);
        std::vector<std::array<float, 2>> coords(nRows, {0.0f, 0.0f});
        if ((int)pca.coords.size() >= nRows * 2) {
            for (int i = 0; i < nRows; i++) {
                coords[i][0] = pca.coords[i * 2];
                coords[i][1] = pca.coords[i * 2 + 1];
            }
        } else {
            std::mt19937 rng(0xBADC0DEu);
            std::uniform_real_distribution<float> dist(-0.5f, 0.5f);
            for (int i = 0; i < nRows; i++) { coords[i][0] = dist(rng); coords[i][1] = dist(rng); }
        }

        std::mt19937 rng(0x12345678u);
        std::uniform_int_distribution<int> pick(0, nRows - 1);
        int negSamples = 5;
        for (int epoch = 0; epoch < epochs; epoch++) {
            double lr = 1.0 * (1.0 - (double)epoch / std::max(1, epochs));
            for (const auto &e : edges) {
                auto &pa = coords[e.i];
                auto &pb = coords[e.j];
                double dx = pa[0] - pb[0];
                double dy = pa[1] - pb[1];
                double dist2 = dx * dx + dy * dy + 1e-6;
                double gradCoeff = -2.0 * a * b * std::pow(dist2, b - 1.0) / (1.0 + a * std::pow(dist2, b));
                double step = lr * e.w * gradCoeff;
                pa[0] += (float)(step * dx);
                pa[1] += (float)(step * dy);
                pb[0] -= (float)(step * dx);
                pb[1] -= (float)(step * dy);

                for (int t = 0; t < negSamples; t++) {
                    int r = pick(rng);
                    if (r == e.i) continue;
                    auto &pn = coords[r];
                    double ndx = pa[0] - pn[0];
                    double ndy = pa[1] - pn[1];
                    double nd2 = ndx * ndx + ndy * ndy + 1e-6;
                    double repCoeff = 2.0 * gamma * b / ((0.001 + nd2) * (1.0 + a * std::pow(nd2, b)));
                    double rep = lr * repCoeff;
                    pa[0] += (float)(rep * ndx);
                    pa[1] += (float)(rep * ndy);
                    pn[0] -= (float)(rep * ndx);
                    pn[1] -= (float)(rep * ndy);
                }
            }
        }
        return coords;
    }
};

class PatternMatrix {
public:
    PatternMatrix(KVMStore *kvm, TensorEngine *tensor)
        : bridge_(kvm, tensor) {}

    json rebuild(const MemeGraph::WindowInfo &windowInfo) {
        auto emb = bridge_.buildEmbeddings(windowInfo.ids, 512);
        auto multi = bridge_.buildMultiOrderCSR(windowInfo);
        auto projection = reducer_.project2D(emb, "auto");
        state_ = json{{"ids", windowInfo.ids}, {"embeddings", emb}, {"multi", multi}, {"projection", projection}};
        return state_;
    }

    json state() const { return state_; }

private:
    GraphTensorBridge bridge_;
    DimReducer reducer_;
    json state_ = json::object();
};

class MemeSurfaceLexicon {
public:
    MemeSurfaceLexicon(std::shared_ptr<KeyValueStore> store, int maxEntriesPerMeme = 64, double decay = 0.985)
        : store_(std::move(store)), maxEntries_(std::max(8, maxEntriesPerMeme)), decay_(decay) {}

    void learn(const std::string &memeId, const std::string &replyText, double weight = 1.0) {
        if (memeId.empty()) return;
        auto rec = load(memeId);
        json phrases = rec.value("phrases", json::object());
        for (auto it = phrases.begin(); it != phrases.end();) {
            double v = 0.0;
            if (it.value().is_number()) {
                v = it.value().get<double>();
            } else if (it.value().is_string()) {
                try { v = std::stod(it.value().get<std::string>()); } catch (...) { v = 0.0; }
            }
            double nv = v * decay_;
            if (nv <= 1e-6) {
                it = phrases.erase(it);
            } else {
                it.value() = nv;
                ++it;
            }
        }
        for (auto &p : extractSurfacePhrases(replyText, 24)) {
            double prev = phrases.value(p.phrase, 0.0);
            phrases[p.phrase] = prev + p.weight * weight;
        }
        std::vector<std::pair<std::string, double>> ordered;
        for (auto it = phrases.begin(); it != phrases.end(); ++it) {
            if (it.value().is_number()) ordered.push_back({it.key(), it.value().get<double>()});
        }
        std::sort(ordered.begin(), ordered.end(), [](auto &a, auto &b) { return a.second > b.second; });
        if ((int)ordered.size() > maxEntries_) ordered.resize(maxEntries_);
        json next;
        next["updatedAt"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();
        json obj = json::object();
        for (auto &kv : ordered) obj[kv.first] = kv.second;
        next["phrases"] = obj;
        store_->put("m:" + memeId, next);
    }

    std::vector<SurfacePhrase> getTop(const std::string &memeId, int limit = 6) {
        auto rec = load(memeId);
        std::vector<SurfacePhrase> out;
        for (auto it = rec.value("phrases", json::object()).begin(); it != rec.value("phrases", json::object()).end(); ++it) {
            double v = 0.0;
            if (it.value().is_number()) {
                v = it.value().get<double>();
            } else if (it.value().is_string()) {
                try { v = std::stod(it.value().get<std::string>()); } catch (...) { v = 0.0; }
            }
            out.push_back({it.key(), v});
        }
        std::sort(out.begin(), out.end(), [](auto &a, auto &b) { return a.weight > b.weight; });
        if ((int)out.size() > limit) out.resize(limit);
        return out;
    }

    json exportSnapshot(int limitMemes = 512) {
        json out = json::array();
        auto items = store_->entries("m:");
        for (auto &kv : items) {
            out.push_back({kv.first, kv.second});
            if ((int)out.size() >= limitMemes) break;
        }
        return out;
    }

    void importSnapshot(const json &entries) {
        if (!entries.is_array()) return;
        for (auto &item : entries) {
            if (!item.is_array() || item.size() != 2) continue;
            std::string key = item[0].get<std::string>();
            if (key.rfind("m:", 0) != 0) continue;
            store_->put(key, item[1]);
        }
    }

private:
    std::shared_ptr<KeyValueStore> store_;
    int maxEntries_;
    double decay_;

    json load(const std::string &memeId) {
        auto v = store_->get("m:" + memeId);
        if (v && v->is_object()) return *v;
        return json{{"phrases", json::object()}, {"updatedAt", 0}};
    }
};

class DialogMemory {
public:
    DialogMemory(std::shared_ptr<KeyValueStore> store, int maxItems = 2048, int maxPerIndex = 64)
        : store_(std::move(store)), maxItems_(std::max(128, maxItems)), maxPerIndex_(std::max(8, maxPerIndex)) {}

    json remember(const std::string &signature, const std::vector<std::string> &memes, const std::string &question, const std::string &reply, double scoreHint = 0.0) {
        if (signature.empty() || reply.empty()) return json();
        std::string id = sha1Hex(signature);
        auto key = "d:" + id;
        auto prev = store_->get(key);
        int count = prev ? prev->value("count", 0) : 0;
        double prevHint = prev ? prev->value("scoreHint", 0.0) : 0.0;
        double hint = std::isfinite(scoreHint) ? scoreHint : prevHint;
        json rec = {
            {"id", id}, {"signature", signature}, {"memes", memes}, {"question", question.substr(0, 800)},
            {"reply", reply.substr(0, 1200)}, {"updatedAt", (int64_t)std::chrono::system_clock::now().time_since_epoch().count()},
            {"count", count + 1}, {"scoreHint", hint}
        };
        store_->put(key, rec);
        for (const auto &memeId : std::unordered_set<std::string>(memes.begin(), memes.end())) {
            std::string ik = "i:" + memeId;
            auto ids = store_->get(ik);
            std::vector<std::string> list;
            if (ids && ids->is_array()) {
                for (auto &x : *ids) {
                    if (x.is_string()) list.push_back(x.get<std::string>());
                }
            }
            list.erase(std::remove(list.begin(), list.end(), id), list.end());
            list.insert(list.begin(), id);
            if ((int)list.size() > maxPerIndex_) list.resize(maxPerIndex_);
            store_->put(ik, list);
        }
        return rec;
    }

    json retrieve(const std::vector<std::string> &memes, const std::string &signature, double minSim = 0.45) {
        std::unordered_set<std::string> candidateIds;
        for (size_t i = 0; i < std::min<size_t>(8, memes.size()); i++) {
            auto ids = store_->get("i:" + memes[i]);
            if (ids && ids->is_array()) {
                for (auto &x : *ids) {
                    if (x.is_string() && !x.get<std::string>().empty()) candidateIds.insert(x.get<std::string>());
                }
            }
        }
        if (candidateIds.empty() && !signature.empty()) candidateIds.insert(sha1Hex(signature));

        auto toSet = [](const std::string &sig) {
            std::unordered_set<std::string> out;
            std::stringstream ss(sig);
            std::string item;
            while (std::getline(ss, item, '|')) if (!item.empty()) out.insert(item);
            return out;
        };

        auto sigSet = toSet(signature);
        json best;
        double bestScore = 0;
        for (const auto &id : candidateIds) {
            auto rec = store_->get("d:" + id);
            if (!rec || !rec->contains("reply")) continue;
            if ((*rec)["reply"].is_string()) {
                auto r = (*rec)["reply"].get<std::string>();
                auto start = r.find_first_not_of(" \n\r\t");
                if (start == std::string::npos) continue;
            }
            auto recSet = toSet(rec->value("signature", ""));
            double inter = 0;
            for (auto &x : sigSet) if (recSet.count(x)) inter++;
            double uni = sigSet.size() + recSet.size() - inter;
            double sim = (uni <= 0) ? 0 : inter / uni;
            if (sim < minSim) continue;
            double freq = std::log(1 + rec->value("count", 0));
            double score = sim * (1 + 0.15 * freq);
            if (score > bestScore) {
                bestScore = score;
                best = *rec;
                best["similarity"] = sim;
                best["score"] = score;
            }
        }
        return best;
    }

    json exportSnapshot(int limit = 512) {
        json out{{"dialogs", json::array()}, {"indexes", json::array()}};
        auto dialogs = store_->entries("d:");
        for (auto &kv : dialogs) {
            out["dialogs"].push_back({kv.first, kv.second});
            if ((int)out["dialogs"].size() >= limit) break;
        }
        auto indexes = store_->entries("i:");
        for (auto &kv : indexes) {
            out["indexes"].push_back({kv.first, kv.second});
            if ((int)out["indexes"].size() >= limit) break;
        }
        return out;
    }

    void importSnapshot(const json &snapshot) {
        for (auto &item : snapshot.value("dialogs", json::array())) {
            if (!item.is_array() || item.size() != 2) continue;
            store_->put(item[0].get<std::string>(), item[1]);
        }
        for (auto &item : snapshot.value("indexes", json::array())) {
            if (!item.is_array() || item.size() != 2) continue;
            store_->put(item[0].get<std::string>(), item[1]);
        }
    }

private:
    std::shared_ptr<KeyValueStore> store_;
    int maxItems_;
    int maxPerIndex_;
};

// ------------------ Robots / Study / Shards ------------------

class ControllerPoolBase;
class RedisSynchronizer;

struct RobotsDocument {
    std::string id;
    std::string text;
    std::vector<std::string> tokens;
    std::string source;
    std::string file;
};

class RobotsCorpus {
public:
    RobotsCorpus(fs::path dir,
                 fs::path lemmaCsv,
                 bool lemmaAutoload,
                 std::size_t lemmaMaxBytes,
                 bool lemmaForce,
                 int chunkMinWords,
                 int chunkMaxWords)
        : dir_(std::move(dir)),
          lemmaCsv_(std::move(lemmaCsv)),
          lemmaAutoload_(lemmaAutoload),
          lemmaForce_(lemmaForce),
          lemmaMaxBytes_(lemmaMaxBytes > 0 ? lemmaMaxBytes : 64ull * 1024ull * 1024ull),
          chunkMinWords_(std::max(1, chunkMinWords)),
          chunkMaxWords_(std::max(chunkMinWords_, chunkMaxWords)) {}

    std::vector<std::string> listFiles() const {
        std::vector<std::string> files;
        if (dir_.empty() || !fs::exists(dir_)) return files;
        try {
            for (auto &entry : fs::directory_iterator(dir_)) {
                if (!entry.is_regular_file()) continue;
                auto ext = entry.path().extension().string();
                std::string lowerExt;
                lowerExt.reserve(ext.size());
                for (char c : ext) lowerExt.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
                if (lowerExt == ".txt") files.push_back(entry.path().filename().string());
            }
            std::sort(files.begin(), files.end());
        } catch (const std::exception &e) {
            std::cerr << "[RobotsCorpus] Failed to list files: " << e.what() << std::endl;
        }
        return files;
    }

    std::vector<std::string> normalizeWords(const std::vector<std::string> &words) {
        std::vector<std::string> out;
        out.reserve(words.size());
        for (auto &w : words) {
            auto lw = lemmatize(w);
            if (!lw.empty()) out.push_back(lw);
        }
        return out;
    }

    std::vector<RobotsDocument> collect(const json &options) {
        std::vector<RobotsDocument> docs;
        auto files = listFiles();
        if (options.contains("files") && options["files"].is_array()) {
            std::unordered_set<std::string> allow;
            for (auto &v : options["files"]) if (v.is_string()) allow.insert(v.get<std::string>());
            files.erase(std::remove_if(files.begin(), files.end(), [&](const std::string &f){ return !allow.count(f); }), files.end());
        }
        if (options.value("shuffle", false) && files.size() > 1) {
            std::mt19937 rng((unsigned)std::chrono::high_resolution_clock::now().time_since_epoch().count());
            std::shuffle(files.begin(), files.end(), rng);
        }
        int offset = options.value("offset", 0);
        if (offset < 0) offset = 0;
        int limit = options.contains("limit") ? options.value("limit", 0) : 0;
        int maxDocs = (limit > 0) ? limit : 0;

        for (const auto &file : files) {
            auto content = readFileLimited(dir_ / file, maxArticleSize_);
            if (content.empty()) continue;
            auto paragraphs = splitParagraphs(content);
            int localIndex = 0;
            for (const auto &paragraph : paragraphs) {
                std::string trimmed = paragraph;
                trimmed.erase(0, trimmed.find_first_not_of(" \n\r\t"));
                trimmed.erase(trimmed.find_last_not_of(" \n\r\t") + 1);
                if ((int)trimmed.size() < minParagraphLength_) continue;
                auto units = splitSentences(trimmed);
                for (const auto &unit : units) {
                    std::string unitText = unit;
                    unitText.erase(0, unitText.find_first_not_of(" \n\r\t"));
                    unitText.erase(unitText.find_last_not_of(" \n\r\t") + 1);
                    if ((int)unitText.size() < minParagraphLength_) continue;
                    auto tokens = normalizeWords(tokenize(unitText));
                    if (tokens.empty()) continue;
                    if (offset > 0) { offset--; continue; }
                    RobotsDocument doc;
                    doc.id = "robots:" + file + "#" + std::to_string(localIndex);
                    doc.file = file;
                    doc.source = "robots:" + file;
                    doc.text = unitText;
                    doc.tokens = std::move(tokens);
                    docs.push_back(std::move(doc));
                    localIndex++;
                    if (maxDocs > 0 && (int)docs.size() >= maxDocs) return docs;
                }
            }
        }
        if (options.value("shuffle", false) && docs.size() > 1) {
            std::mt19937 rng((unsigned)std::chrono::high_resolution_clock::now().time_since_epoch().count());
            std::shuffle(docs.begin(), docs.end(), rng);
        }
        return docs;
    }

    std::string lemmatize(const std::string &word) {
        ensureLemmaLoaded();
        std::string lower;
        lower.reserve(word.size());
        for (unsigned char c : word) {
            if (c < 0x80) lower.push_back(static_cast<char>(std::tolower(c)));
            else lower.push_back(static_cast<char>(c));
        }
        auto it = lemmaMap_.find(lower);
        return it == lemmaMap_.end() ? lower : it->second;
    }

private:
    fs::path dir_;
    fs::path lemmaCsv_;
    bool lemmaAutoload_{false};
    bool lemmaForce_{false};
    std::size_t lemmaMaxBytes_{64ull * 1024ull * 1024ull};
    int chunkMinWords_{2};
    int chunkMaxWords_{5};
    bool lemmaLoaded_{false};
    std::unordered_map<std::string, std::string> lemmaMap_;
    std::size_t maxArticleSize_{5000000};
    int minParagraphLength_{12};

    void ensureLemmaLoaded() {
        if (lemmaLoaded_) return;
        lemmaLoaded_ = true;
        if (!lemmaAutoload_ && !lemmaForce_) return;
        lemmaMap_ = loadLemmaMap();
    }

    std::unordered_map<std::string, std::string> loadLemmaMap() {
        std::unordered_map<std::string, std::string> map;
        if (lemmaCsv_.empty() || !fs::exists(lemmaCsv_)) return map;
        try {
            if (!lemmaForce_) {
                try {
                    auto st = fs::file_size(lemmaCsv_);
                    if (st > lemmaMaxBytes_) {
                        std::cerr << "[RobotsCorpus] lemma.csv too large (" << (st / 1024 / 1024) << "MB), skip autoload. Set AI_LEMMA_FORCE=true or increase AI_LEMMA_MAX_MB to load." << std::endl;
                        return map;
                    }
                } catch (...) {}
            }
            std::ifstream in(lemmaCsv_, std::ios::binary);
            std::string line;
            while (std::getline(in, line)) {
                if (line.empty()) continue;
                auto cols = splitCsvLine(line);
                if (cols.empty()) continue;
                std::string base = toLower(cols[0]);
                if (base.empty()) continue;
                map[base] = base;
                for (auto &c : cols) {
                    auto token = toLower(c);
                    if (!token.empty()) map[token] = base;
                }
            }
        } catch (const std::exception &e) {
            std::cerr << "[RobotsCorpus] Failed to load lemma map: " << e.what() << std::endl;
        }
        return map;
    }

    static std::string toLower(const std::string &s) {
        std::string out;
        out.reserve(s.size());
        for (unsigned char c : s) {
            if (c < 0x80) out.push_back(static_cast<char>(std::tolower(c)));
            else out.push_back(static_cast<char>(c));
        }
        return out;
    }

    static std::vector<std::string> splitCsvLine(const std::string &line) {
        std::vector<std::string> cols;
        std::string cur;
        bool inQuotes = false;
        for (size_t i = 0; i < line.size(); i++) {
            char c = line[i];
            if (c == '"') {
                if (inQuotes && i + 1 < line.size() && line[i + 1] == '"') {
                    cur.push_back('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                cols.push_back(cur);
                cur.clear();
            } else {
                cur.push_back(c);
            }
        }
        cols.push_back(cur);
        return cols;
    }

    static std::string readFileLimited(const fs::path &file, std::size_t maxBytes) {
        try {
            std::ifstream in(file, std::ios::binary);
            if (!in) return {};
            std::string out;
            out.reserve(4096);
            char buf[4096];
            std::size_t total = 0;
            while (in && total < maxBytes) {
                in.read(buf, sizeof(buf));
                auto got = (std::size_t)in.gcount();
                if (!got) break;
                if (total + got > maxBytes) got = maxBytes - total;
                out.append(buf, got);
                total += got;
            }
            return out;
        } catch (...) {
            return {};
        }
    }

    static std::vector<std::string> splitParagraphs(const std::string &content) {
        std::vector<std::string> out;
        std::regex re(R"(\r?\n\s*\r?\n)");
        std::sregex_token_iterator it(content.begin(), content.end(), re, -1);
        std::sregex_token_iterator end;
        for (; it != end; ++it) {
            std::string s = it->str();
            s.erase(0, s.find_first_not_of(" \n\r\t"));
            s.erase(s.find_last_not_of(" \n\r\t") + 1);
            if (!s.empty()) out.push_back(s);
        }
        return out;
    }
};

class SessionManager {
public:
    SessionManager(std::shared_ptr<KeyValueStore> store, int idleMs = 10 * 60 * 1000, int maxSessions = 200)
        : store_(std::move(store)), idleMs_(idleMs), maxSessions_(maxSessions) {}

    std::string ensure(const std::string &sessionId) {
        if (!sessionId.empty()) {
            std::string sid = sessionId;
            auto it = active_.find(sid);
            if (it != active_.end()) {
                auto &data = it->second;
                data["lastActivity"] = nowMs();
                data["count"] = data.value("count", 0) + 1;
                save(sid, data);
                if ((int)active_.size() > maxSessions_) truncate();
                return sid;
            }
            auto stored = store_->get("session:" + sid);
            if (stored && stored->is_object()) {
                json data = *stored;
                data["id"] = data.value("id", sid);
                data["lastActivity"] = nowMs();
                data["count"] = data.value("count", 0) + 1;
                active_[sid] = data;
                if (std::find(order_.begin(), order_.end(), sid) == order_.end()) order_.push_back(sid);
                save(sid, data);
                if ((int)active_.size() > maxSessions_) truncate();
                return sid;
            }
        }
        std::string id = newId();
        json data{{"id", id}, {"createdAt", nowMs()}, {"lastActivity", nowMs()}, {"count", 1}, {"meta", json::object()}};
        active_[id] = data;
        order_.push_back(id);
        save(id, data);
        if ((int)active_.size() > maxSessions_) truncate();
        return id;
    }

    json exportSessions() const {
        json out = json::array();
        for (auto &id : order_) {
            auto it = active_.find(id);
            if (it != active_.end()) out.push_back(it->second);
        }
        return out;
    }

    void importSessions(const json &list) {
        active_.clear();
        order_.clear();
        if (!list.is_array()) return;
        for (auto &item : list) {
            if (!item.is_object()) continue;
            std::string id = item.value("id", "");
            if (id.empty()) continue;
            active_[id] = item;
            order_.push_back(id);
            save(id, item);
        }
    }

private:
    std::shared_ptr<KeyValueStore> store_;
    int idleMs_{10 * 60 * 1000};
    int maxSessions_{200};
    std::unordered_map<std::string, json> active_;
    std::vector<std::string> order_;

    static int64_t nowMs() {
        return (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    void save(const std::string &sid, const json &data) {
        store_->put("session:" + sid, data);
    }

    std::string newId() const {
        return randomHex(8);
    }

    void truncate() {
        std::vector<json> items;
        items.reserve(active_.size());
        for (auto &kv : active_) items.push_back(kv.second);
        std::sort(items.begin(), items.end(), [](const json &a, const json &b) {
            return a.value("lastActivity", 0) > b.value("lastActivity", 0);
        });
        if ((int)items.size() <= maxSessions_) return;
        std::unordered_set<std::string> keep;
        for (int i = 0; i < maxSessions_ && i < (int)items.size(); i++) keep.insert(items[i].value("id", ""));
        for (auto it = active_.begin(); it != active_.end();) {
            if (!keep.count(it->first)) {
                store_->del("session:" + it->first);
                it = active_.erase(it);
            } else {
                ++it;
            }
        }
        if (!order_.empty()) {
            std::vector<std::string> next;
            next.reserve(order_.size());
            for (auto &id : order_) {
                if (keep.count(id)) next.push_back(id);
            }
            order_.swap(next);
        }
    }
};

struct LemmaCacheEntry {
    fs::file_time_type mtime;
    std::unordered_map<std::string, std::string> map;
};

static std::mutex gLemmaCacheMu;
static std::unordered_map<std::string, LemmaCacheEntry> gLemmaCache;

static std::string trimString(const std::string &s) {
    auto start = s.find_first_not_of(" \n\r\t");
    if (start == std::string::npos) return std::string();
    auto end = s.find_last_not_of(" \n\r\t");
    return s.substr(start, end - start + 1);
}

static std::string lowerAscii(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        if (c < 0x80) out.push_back(static_cast<char>(std::tolower(c)));
        else out.push_back(static_cast<char>(c));
    }
    return out;
}

static std::vector<std::string> splitCsvLineSimple(const std::string &line) {
    std::vector<std::string> cols;
    std::string cur;
    bool inQuotes = false;
    for (size_t i = 0; i < line.size(); i++) {
        char c = line[i];
        if (c == '"') {
            if (inQuotes && i + 1 < line.size() && line[i + 1] == '"') {
                cur.push_back('"');
                i++;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (c == ',' && !inQuotes) {
            cols.push_back(trimString(cur));
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    cols.push_back(trimString(cur));
    return cols;
}

static std::unordered_map<std::string, std::string> loadLemmaMapCached(const std::string &lemmaCsvPath) {
    std::unordered_map<std::string, std::string> map;
    if (lemmaCsvPath.empty()) return map;
    fs::path file(lemmaCsvPath);
    try {
        if (!fs::exists(file)) return map;
        auto mtime = fs::last_write_time(file);
        {
            std::lock_guard<std::mutex> lock(gLemmaCacheMu);
            auto it = gLemmaCache.find(file.string());
            if (it != gLemmaCache.end() && it->second.mtime >= mtime) {
                return it->second.map;
            }
        }
        std::ifstream in(file, std::ios::binary);
        if (!in) return map;
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            auto cols = splitCsvLineSimple(line);
            if (cols.empty()) continue;
            std::string base = lowerAscii(cols[0]);
            base = trimString(base);
            if (base.empty()) continue;
            map[base] = base;
            for (auto &c : cols) {
                auto token = trimString(lowerAscii(c));
                if (!token.empty()) map[token] = base;
            }
        }
        {
            std::lock_guard<std::mutex> lock(gLemmaCacheMu);
            gLemmaCache[file.string()] = LemmaCacheEntry{mtime, map};
        }
    } catch (...) {
        return map;
    }
    return map;
}

class LemmaWorkerPool {
public:
    explicit LemmaWorkerPool(int workers) {
        int count = std::max(1, workers);
        running_ = true;
        threads_.reserve(count);
        for (int i = 0; i < count; i++) {
            threads_.emplace_back([this]() { workerLoop(); });
        }
    }

    ~LemmaWorkerPool() {
        stop();
    }

    std::vector<std::string> execBatchLemmatize(const std::vector<std::string> &tokens, const std::string &lemmaCsvPath) {
        if (tokens.empty()) return {};
        auto task = std::make_shared<Task>();
        task->tokens = tokens;
        task->lemmaCsvPath = lemmaCsvPath;
        auto fut = task->promise.get_future();
        {
            std::lock_guard<std::mutex> lock(mu_);
            queue_.push(task);
        }
        cv_.notify_one();
        return fut.get();
    }

    void stop() {
        if (!running_) return;
        running_ = false;
        cv_.notify_all();
        for (auto &t : threads_) {
            if (t.joinable()) t.join();
        }
        threads_.clear();
    }

private:
    struct Task {
        std::vector<std::string> tokens;
        std::string lemmaCsvPath;
        std::promise<std::vector<std::string>> promise;
    };

    std::atomic<bool> running_{false};
    std::vector<std::thread> threads_;
    std::queue<std::shared_ptr<Task>> queue_;
    std::mutex mu_;
    std::condition_variable cv_;

    void workerLoop() {
        while (running_) {
            std::shared_ptr<Task> task;
            {
                std::unique_lock<std::mutex> lock(mu_);
                cv_.wait(lock, [&]() { return !running_ || !queue_.empty(); });
                if (!running_ && queue_.empty()) return;
                task = queue_.front();
                queue_.pop();
            }
            try {
                auto lemmaMap = loadLemmaMapCached(task->lemmaCsvPath);
                std::vector<std::string> out;
                out.reserve(task->tokens.size());
                for (auto &w : task->tokens) {
                    std::string normalized = trimString(lowerAscii(w));
                    if (normalized.empty()) {
                        out.push_back(std::string());
                        continue;
                    }
                    auto it = lemmaMap.find(normalized);
                    if (it != lemmaMap.end()) out.push_back(it->second);
                    else out.push_back(normalized);
                }
                task->promise.set_value(out);
            } catch (...) {
                try { task->promise.set_value({}); } catch (...) {}
            }
        }
    }
};

class StudyEngine {
public:
    StudyEngine(std::shared_ptr<ControllerPoolBase> pool, std::shared_ptr<RedisSynchronizer> redis);

    void start();
    void enqueueDocument(const json &doc);
    json status() const;
    bool running() const { return running_; }

private:
    void tick();
    std::shared_ptr<LemmaWorkerPool> ensureWorkerPool();

    std::shared_ptr<ControllerPoolBase> pool_;
    std::shared_ptr<RedisSynchronizer> redis_;
    std::atomic<bool> running_{false};
    std::thread worker_;
    mutable std::mutex mu_;
    std::deque<json> queue_;
    json metrics_ = json{{"enqueued", 0}, {"processed", 0}, {"lastTickAt", 0}, {"lastError", nullptr}};
    mutable std::mutex workerMu_;
    std::shared_ptr<LemmaWorkerPool> workerPool_;
};

// ------------------ Runtime ------------------

class RuntimeState : public RuntimeBase {
public:
    RuntimeState(std::shared_ptr<KeyValueStore> kvmStore, std::shared_ptr<KeyValueStore> memeStore, std::shared_ptr<KeyValueStore> sessionStore, const Config &config)
        : config_(config), kvm_(kvmStore, config.kvmCacheMaxEntries), graph_(memeStore), sessions_(sessionStore), pattern_(&kvm_, &tensor_) {
        surfaceStore_ = std::make_shared<NamespacedStore>(sessionStore, "surface");
        dialogStore_ = std::make_shared<NamespacedStore>(sessionStore, "dialog");
        surfaceLexicon_ = std::make_shared<MemeSurfaceLexicon>(surfaceStore_);
        dialogMemory_ = std::make_shared<DialogMemory>(dialogStore_);
        sessionManager_ = std::make_shared<SessionManager>(sessionStore);
        params_ = MODEL_DEFAULTS;
        searchConfig_.enabled = true;
        if (!config_.searchEndpoint.empty()) {
            searchConfig_.active = config_.searchEndpoint;
            searchConfig_.endpoints.push_back(config_.searchEndpoint);
        }
        if (!searchConfig_.active.empty() && std::find(searchConfig_.endpoints.begin(), searchConfig_.endpoints.end(), searchConfig_.active) == searchConfig_.endpoints.end()) {
            searchConfig_.endpoints.push_back(searchConfig_.active);
        }
        researcher_ = std::make_shared<OnlineResearcher>(this, searchConfig_.active);
        researcher_->setEnabled(searchConfig_.enabled);
        addons_ = addon::createDefaultAddons();
    }

    json cloneParams() const override {
        json j;
        j["decayFactor"] = params_.decayFactor;
        j["maxMemeWords"] = params_.maxMemeWords;
        j["minOverlapThreshold"] = params_.minOverlapThreshold;
        j["memeNgramMin"] = params_.memeNgramMin;
        j["memeNgramMax"] = params_.memeNgramMax;
        j["maliciousThreshold"] = params_.maliciousThreshold;
        j["learningIterations"] = params_.learningIterations;
        j["iteration"] = params_.iteration;
        j["threshold"] = params_.threshold;
        j["decay"] = params_.decay;
        j["decayK"] = params_.decayK;
        j["maxLen"] = params_.maxLen;
        j["edgeWeight"] = params_.edgeWeight;
        j["activationType"] = params_.activationType;
        j["transferType"] = params_.transferType;
        j["activationCustom"] = params_.activationCustom;
        j["transferCustom"] = params_.transferCustom;
        j["mappingDepth"] = params_.mappingDepth;
        j["reflectionTopMemes"] = params_.reflectionTopMemes;
        j["reflectionTopWords"] = params_.reflectionTopWords;
        j["reflectionMinScore"] = params_.reflectionMinScore;
        return j;
    }

    json listAddons() const {
        return addons_ ? addons_->listAddons() : json::array();
    }

    bool addAddon(const std::string &type, const std::string &name, std::string &error) {
        if (!addons_) {
            error = "addon manager unavailable";
            return false;
        }
        return addons_->addBuiltin(type, name, &error);
    }

    bool addAddon(const std::string &path, std::string &error) {
        if (!addons_) {
            error = "addon manager unavailable";
            return false;
        }
        return addons_->loadLibrary(path, &error);
    }

    bool removeAddon(const std::string &name, std::string &error) {
        if (!addons_) {
            error = "addon manager unavailable";
            return false;
        }
        return addons_->removeAddon(name, &error);
    }

    void setParams(const json &patch) override {
        if (patch.contains("decayFactor")) params_.decayFactor = patch["decayFactor"].get<double>();
        if (patch.contains("maxMemeWords")) params_.maxMemeWords = patch["maxMemeWords"].get<int>();
        if (patch.contains("minOverlapThreshold")) params_.minOverlapThreshold = patch["minOverlapThreshold"].get<int>();
        if (patch.contains("memeNgramMin")) params_.memeNgramMin = patch["memeNgramMin"].get<int>();
        if (patch.contains("memeNgramMax")) params_.memeNgramMax = patch["memeNgramMax"].get<int>();
        if (patch.contains("maliciousThreshold")) params_.maliciousThreshold = patch["maliciousThreshold"].get<double>();
        if (patch.contains("learningIterations")) params_.learningIterations = patch["learningIterations"].get<int>();
        if (patch.contains("iteration")) params_.iteration = patch["iteration"].get<int>();
        if (patch.contains("threshold")) params_.threshold = patch["threshold"].get<double>();
        if (patch.contains("decay")) params_.decay = patch["decay"].get<double>();
        if (patch.contains("decayK")) params_.decayK = patch["decayK"].get<double>();
        if (patch.contains("maxLen")) params_.maxLen = patch["maxLen"].get<int>();
        if (patch.contains("edgeWeight")) params_.edgeWeight = patch["edgeWeight"].get<double>();
        if (patch.contains("activationType")) params_.activationType = patch["activationType"].get<std::string>();
        if (patch.contains("transferType")) params_.transferType = patch["transferType"].get<std::string>();
        if (patch.contains("activationCustom")) params_.activationCustom = patch["activationCustom"].get<std::string>();
        if (patch.contains("transferCustom")) params_.transferCustom = patch["transferCustom"].get<std::string>();
        if (patch.contains("mappingDepth")) params_.mappingDepth = patch["mappingDepth"].get<int>();
        if (patch.contains("reflectionTopMemes")) params_.reflectionTopMemes = patch["reflectionTopMemes"].get<int>();
        if (patch.contains("reflectionTopWords")) params_.reflectionTopWords = patch["reflectionTopWords"].get<int>();
        if (patch.contains("reflectionMinScore")) params_.reflectionMinScore = patch["reflectionMinScore"].get<double>();
    }

    struct SeedMapping {
        std::unordered_map<std::string, double> seeds;
        std::vector<std::string> order;
    };

    SeedMapping mapWordsToMemesOrdered(const std::vector<std::string> &words) {
        SeedMapping out;
        auto trim = [](const std::string &s) {
            auto start = s.find_first_not_of(" \n\r\t");
            if (start == std::string::npos) return std::string();
            auto end = s.find_last_not_of(" \n\r\t");
            return s.substr(start, end - start + 1);
        };
        std::vector<std::string> tokens;
        tokens.reserve(words.size());
        for (auto &w : words) {
            std::string t = trim(w);
            if (!t.empty()) tokens.push_back(t);
        }

        auto addSeed = [&](const std::string &memeId, double delta) {
            if (memeId.empty()) return;
            auto it = out.seeds.find(memeId);
            if (it == out.seeds.end()) {
                out.seeds[memeId] = delta;
                out.order.push_back(memeId);
            } else {
                it->second += delta;
            }
        };

        for (auto &word : tokens) {
            auto memes = kvm_.getWordMemeSet(word);
            if (memes.empty()) {
                std::string memeId = "meme_" + sha1Hex(word);
                graph_.ensureNode(memeId);
                kvm_.link(word, memeId);
                addSeed(memeId, 1.0);
            } else {
                for (auto &mid : memes) addSeed(mid, 1.0);
            }
        }

        int nMin = std::max(2, params_.memeNgramMin);
        int nMax = std::max(nMin, params_.memeNgramMax);
        int minOverlap = std::max(1, params_.minOverlapThreshold);
        int maxWordSet = std::max(4, params_.maxMemeWords);
        int units = 0;
        int maxUnits = 128;

        auto resolveOrCreate = [&](const std::vector<std::string> &tokenSet) -> std::string {
            std::vector<std::string> uniq;
            std::unordered_set<std::string> seen;
            for (auto &t : tokenSet) {
                if (t.empty()) continue;
                if (seen.insert(t).second) uniq.push_back(t);
            }
            if ((int)uniq.size() > maxWordSet) uniq.resize(maxWordSet);
            if (uniq.size() <= 1) return uniq.empty() ? "" : ("meme_" + sha1Hex(uniq[0]));

            std::unordered_map<std::string, int> counts;
            for (auto &w : uniq) {
                for (auto &mid : kvm_.getWordMemeSet(w)) counts[mid]++;
            }
            std::string best; int bestOverlap = 0;
            for (auto &kv : counts) { if (kv.second > bestOverlap) { bestOverlap = kv.second; best = kv.first; } }
            if (!best.empty() && bestOverlap >= minOverlap) {
                for (auto &w : uniq) kvm_.link(w, best);
                graph_.ensureNode(best);
                return best;
            }
            std::vector<std::string> sorted = uniq;
            std::sort(sorted.begin(), sorted.end());
            std::string memeId = "meme_p_" + sha1Hex([&](){
                std::string join; for (size_t i = 0; i < sorted.size(); i++) { if (i) join += "|"; join += sorted[i]; } return join; }());
            graph_.ensureNode(memeId);
            for (auto &w : sorted) kvm_.link(w, memeId);
            return memeId;
        };

        for (size_t i = 0; i < tokens.size() && units < maxUnits; i++) {
            for (int n = nMin; n <= nMax && units < maxUnits; n++) {
            if (i + n > tokens.size()) break;
            std::vector<std::string> gram(tokens.begin() + i, tokens.begin() + i + n);
                std::string memeId = resolveOrCreate(gram);
                if (memeId.empty()) continue;
                double w = 1.0 + 0.5 * (n - 1);
                addSeed(memeId, w);
                units++;
            }
        }

        return out;
    }

    std::unordered_map<std::string, double> mapWordsToMemes(const std::vector<std::string> &words) override {
        return mapWordsToMemesOrdered(words).seeds;
    }

    json processInput(const json &payload) override {
        auto started = std::chrono::steady_clock::now();
        std::function<std::string(const json &)> jsStringForJoin;
        std::function<std::string(const json &)> jsStringValue;
        jsStringForJoin = [&](const json &v) -> std::string {
            if (v.is_null()) return "";
            if (v.is_string()) return v.get<std::string>();
            if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
            if (v.is_number()) return v.dump();
            if (v.is_array()) {
                std::string out;
                bool first = true;
                for (auto &item : v) {
                    if (!first) out += ",";
                    first = false;
                    out += jsStringForJoin(item);
                }
                return out;
            }
            return "[object Object]";
        };
        jsStringValue = [&](const json &v) -> std::string {
            if (v.is_null()) return "null";
            if (v.is_string()) return v.get<std::string>();
            if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
            if (v.is_number()) return v.dump();
            if (v.is_array()) return jsStringForJoin(v);
            return "[object Object]";
        };
        auto jsStringOrEmpty = [&](const json &v) -> std::string {
            if (v.is_null()) return "";
            if (v.is_boolean()) return v.get<bool>() ? "true" : "";
            if (v.is_number()) {
                double n = v.get<double>();
                if (n == 0.0) return "";
                return v.dump();
            }
            if (v.is_string()) return v.get<std::string>();
            if (v.is_array()) {
                std::string out;
                bool first = true;
                for (auto &item : v) {
                    if (!first) out += ",";
                    first = false;
                    out += jsStringForJoin(item);
                }
                return out;
            }
            return "[object Object]";
        };
        json textVal = json::object();
        if (payload.contains("text") && !payload["text"].is_null()) textVal = payload["text"]; 
        else if (payload.contains("message") && !payload["message"].is_null()) textVal = payload["message"]; 
        std::string text = jsStringOrEmpty(textVal);
        auto tokensRaw = payload.value("tokens", payload.value("words", payload.value("vocab", json::array())));
        std::vector<std::string> words;
        if (tokensRaw.is_array() && !tokensRaw.empty()) {
            for (auto &w : tokensRaw) {
                words.push_back(jsStringValue(w));
            }
        } else {
            words = tokenize(text);
        }
        auto jsTruthy = [&](const json &v) -> bool {
            if (v.is_null()) return false;
            if (v.is_boolean()) return v.get<bool>();
            if (v.is_number()) {
                double n = v.get<double>();
                return std::isfinite(n) && n != 0.0;
            }
            if (v.is_string()) return !v.get<std::string>().empty();
            return true;
        };
        std::string sessionId;
        if (payload.contains("sessionId") && jsTruthy(payload["sessionId"])) {
            sessionId = jsStringValue(payload["sessionId"]);
        }
        if (sessionManager_) sessionId = sessionManager_->ensure(sessionId);
        if (sessionId.empty()) sessionId = randomSessionId();

        bool disableAddon = payload.value("disableAddon", false);
        if (!disableAddon && addons_) {
            auto addonResult = addons_->run(text, payload);
            if (addonResult.handled) {
                auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();
                metrics_["requests"] = metrics_.value("requests", 0) + 1;
                metrics_["lastLatency"] = latency;
                metrics_["updatedAt"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();
                json out;
                out["reply"] = addonResult.reply;
                out["addon"] = addonResult.meta;
                out["sessionId"] = sessionId;
                out["latency"] = latency;
                out["params"] = cloneParams();
                return out;
            }
            if (!addonResult.extraTokens.empty()) {
                words.insert(words.end(), addonResult.extraTokens.begin(), addonResult.extraTokens.end());
            }
        }

        json budget = payload.contains("budget") && payload["budget"].is_object() ? payload["budget"] : json::object();
        auto jsToNumber = [&](const json &v, bool &ok) -> double {
            ok = true;
            if (v.is_null()) { ok = false; return std::numeric_limits<double>::quiet_NaN(); }
            if (v.is_number()) return v.get<double>();
            if (v.is_boolean()) return v.get<bool>() ? 1.0 : 0.0;
            if (v.is_string()) {
                std::string s = v.get<std::string>();
                auto start = s.find_first_not_of(" \n\r\t");
                if (start == std::string::npos) return 0.0;
                auto end = s.find_last_not_of(" \n\r\t");
                s = s.substr(start, end - start + 1);
                if (s.empty()) return 0.0;
                try { return std::stod(s); } catch (...) { ok = false; return std::numeric_limits<double>::quiet_NaN(); }
            }
            ok = false;
            return std::numeric_limits<double>::quiet_NaN();
        };
        auto pickValue = [&](const json &obj, const std::vector<std::string> &keys) -> const json * {
            for (auto &k : keys) {
                if (obj.contains(k) && !obj[k].is_null()) return &obj[k];
            }
            return nullptr;
        };
        auto numberOrHard = [&](const json *v, double paramFallback, double hardFallback) -> double {
            double n = paramFallback;
            if (v) {
                bool ok = false;
                n = jsToNumber(*v, ok);
            }
            if (!std::isfinite(n) || n == 0.0) return hardFallback;
            return n;
        };
        auto numberFinite = [&](const json *v, double fallback) -> double {
            if (!v) return fallback;
            bool ok = false;
            double n = jsToNumber(*v, ok);
            return std::isfinite(n) ? n : fallback;
        };

        int depth = std::max(1, (int)numberOrHard(pickValue(budget, {"mappingDepth", "depth"}), params_.mappingDepth ? params_.mappingDepth : 1, 1));
        int topMemesK = std::max(3, (int)numberOrHard(pickValue(budget, {"reflectionTopMemes", "topMemes"}), params_.reflectionTopMemes ? params_.reflectionTopMemes : 18, 18));
        int topWordsK = std::max(3, (int)numberOrHard(pickValue(budget, {"reflectionTopWords", "topWords"}), params_.reflectionTopWords ? params_.reflectionTopWords : 24, 24));
        double minScoreRaw = numberFinite(pickValue(budget, {"reflectionMinScore", "minScore"}), params_.reflectionMinScore);
        double minScore = std::isfinite(minScoreRaw) ? minScoreRaw : 1e-6;
        int iteration = std::max(1, (int)numberOrHard(pickValue(budget, {"iteration"}), params_.iteration ? params_.iteration : 5, 5));
        int radius = std::max(1, std::min(6, (int)numberOrHard(pickValue(budget, {"radius", "windowRadius"}), 2, 2)));

        auto mapping = mapWordsToMemesOrdered(words);
        auto seeds = mapping.seeds;
        auto seedOrder = mapping.order;
        PropagationResult result;
        if (depth > 1) {
            for (int hop = 1; hop < depth; hop++) {
                result = runPropagation(seeds, json{{"iteration", iteration}, {"radius", radius}, {"seedOrder", seedOrder}});
                auto topMemes = pickTopActivatedMemes(result, seeds, topMemesK, minScore);
                std::unordered_map<std::string, double> wordScore;
                for (auto &m : topMemes) {
                    auto linked = kvm_.getMemeWords(m.memeId);
                    for (auto &w : linked) {
                        if (w.empty()) continue;
                        auto prev = wordScore.count(w) ? wordScore[w] : 0.0;
                        wordScore[w] = std::max(prev, m.score);
                    }
                }
                std::vector<std::pair<std::string, double>> sorted(wordScore.begin(), wordScore.end());
                std::sort(sorted.begin(), sorted.end(), [](auto &a, auto &b){ return a.second > b.second; });
                std::vector<std::string> expanded;
                for (size_t i = 0; i < sorted.size() && (int)expanded.size() < topWordsK; i++) expanded.push_back(sorted[i].first);
                std::vector<std::string> merged;
                merged.reserve(std::min<size_t>(64, words.size()) + expanded.size());
                for (size_t i = 0; i < words.size() && i < 64; i++) merged.push_back(words[i]);
                for (auto &w : expanded) merged.push_back(w);
                std::unordered_set<std::string> dedup;
                std::vector<std::string> final;
                for (auto &w : merged) if (!w.empty() && !dedup.count(w)) { dedup.insert(w); final.push_back(w); }
                auto nextMapping = mapWordsToMemesOrdered(final);
                seeds = nextMapping.seeds;
                seedOrder = nextMapping.order;
            }
        }
        result = runPropagation(seeds, json{{"iteration", iteration}, {"radius", radius}, {"seedOrder", seedOrder}});
        std::string reply = composeReply(result, words, seeds);
        auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();
        metrics_["requests"] = metrics_.value("requests", 0) + 1;
        metrics_["lastLatency"] = latency;
        metrics_["updatedAt"] = (int64_t)std::chrono::system_clock::now().time_since_epoch().count();

        json out;
        out["reply"] = reply;
        out["seeds"] = json::array();
        for (auto &id : seedOrder) {
            auto it = seeds.find(id);
            if (it != seeds.end()) out["seeds"].push_back({id, it->second});
        }
        out["activation"] = result.activation;
        out["memes"] = result.windowInfo.ids;
        out["sessionId"] = sessionId;
        out["latency"] = latency;
        out["params"] = cloneParams();
        return out;
    }

    json metrics() const { return metrics_; }

    json toSnapshot() override {
        json out;
        out["params"] = cloneParams();
        out["graph"] = graph_.exportSnapshot();
        if (sessionManager_) out["sessions"] = sessionManager_->exportSessions();
        out["kvm"] = kvm_.exportEntries();
        out["surface"] = surfaceLexicon_->exportSnapshot(512);
        out["dialog"] = dialogMemory_->exportSnapshot(512);
        return out;
    }

    void fromSnapshot(const json &snapshot) override {
        if (snapshot.is_null()) return;
        if (snapshot.contains("params")) setParams(snapshot["params"]);
        graph_.importSnapshot(snapshot.value("graph", json::object()));
        if (snapshot.contains("sessions") && sessionManager_) sessionManager_->importSessions(snapshot["sessions"]);
        if (snapshot.contains("kvm")) {
            for (auto &kv : snapshot["kvm"]["words"].items()) {
                for (auto &m : kv.value()) kvm_.link(kv.key(), m.get<std::string>());
            }
        }
        if (snapshot.contains("surface")) surfaceLexicon_->importSnapshot(snapshot["surface"]);
        if (snapshot.contains("dialog")) dialogMemory_->importSnapshot(snapshot["dialog"]);
    }

    MemeGraph &graph() { return graph_; }
    KVMStore &kvm() { return kvm_; }
    MemeSurfaceLexicon &surfaceLexicon() { return *surfaceLexicon_; }
    DialogMemory &dialogMemory() { return *dialogMemory_; }
    const Config &config() const override { return config_; }

    std::vector<std::string> getMemeWords(const std::string &memeId) override {
        return kvm_.getMemeWords(memeId);
    }

    void graphLink(const std::string &fromId, const std::string &toId, double weight, int direction) override {
        graph_.link(fromId, toId, weight, direction);
    }

    json getSearchConfig() const override { return searchConfig_.toJson(); }

    json setSearchConfig(const json &patch) override {
        auto trim = [](std::string s) {
            auto start = s.find_first_not_of(" \n\r\t");
            if (start == std::string::npos) return std::string();
            auto end = s.find_last_not_of(" \n\r\t");
            return s.substr(start, end - start + 1);
        };

        if (patch.contains("enabled") && patch["enabled"].is_boolean()) {
            searchConfig_.enabled = patch["enabled"].get<bool>();
        }
        if (patch.contains("endpoints") && patch["endpoints"].is_array()) {
            searchConfig_.endpoints.clear();
            std::unordered_set<std::string> seen;
            for (auto &v : patch["endpoints"]) {
                std::string s;
                if (v.is_string()) s = v.get<std::string>();
                else if (v.is_number() || v.is_boolean()) s = v.dump();
                else if (v.is_null()) s.clear();
                else s = v.dump();
                s = trim(s);
                if (!s.empty() && !seen.count(s)) {
                    seen.insert(s);
                    searchConfig_.endpoints.push_back(s);
                }
            }
        }
        if (patch.contains("active") && patch["active"].is_string()) {
            searchConfig_.active = trim(patch["active"].get<std::string>());
            if (!searchConfig_.active.empty() && std::find(searchConfig_.endpoints.begin(), searchConfig_.endpoints.end(), searchConfig_.active) == searchConfig_.endpoints.end()) {
                searchConfig_.endpoints.push_back(searchConfig_.active);
            }
        }

        config_.searchEndpoint = searchConfig_.active;
        if (!researcher_) researcher_ = std::make_shared<OnlineResearcher>(this, searchConfig_.active);
        researcher_->setEnabled(searchConfig_.enabled);
        researcher_->setEndpoint(searchConfig_.active);
        return searchConfig_.toJson();
    }

    std::vector<std::string> listRobotsFiles() const override {
        return ensureRobotsCorpus()->listFiles();
    }

    std::vector<RobotsDocument> collectRobotsDocuments(const json &options) const override {
        return ensureRobotsCorpus()->collect(options);
    }

    std::vector<std::string> lemmatizeTokens(const std::vector<std::string> &tokens) {
        return ensureRobotsCorpus()->normalizeWords(tokens);
    }

    json ingestDocument(const json &doc) override {
        std::string text = doc.value("text", "");
        std::vector<std::string> tokens;
        if (doc.contains("tokens") && doc["tokens"].is_array()) {
            std::function<std::string(const json &)> jsStringForJoin;
            jsStringForJoin = [&](const json &v) -> std::string {
                if (v.is_null()) return "";
                if (v.is_string()) return v.get<std::string>();
                if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
                if (v.is_number()) return v.dump();
                if (v.is_array()) {
                    std::string out;
                    bool first = true;
                    for (auto &item : v) {
                        if (!first) out += ",";
                        first = false;
                        out += jsStringForJoin(item);
                    }
                    return out;
                }
                return "[object Object]";
            };
            auto jsStringOrEmpty = [&](const json &v) -> std::string {
                if (v.is_null()) return "";
                if (v.is_boolean()) return v.get<bool>() ? "true" : "";
                if (v.is_number()) {
                    double n = v.get<double>();
                    if (n == 0.0) return "";
                    return v.dump();
                }
                if (v.is_string()) return v.get<std::string>();
                if (v.is_array()) {
                    std::string out;
                    bool first = true;
                    for (auto &item : v) {
                        if (!first) out += ",";
                        first = false;
                        out += jsStringForJoin(item);
                    }
                    return out;
                }
                return "[object Object]";
            };
            for (auto &t : doc["tokens"]) {
                std::string s = jsStringOrEmpty(t);
                if (!s.empty()) tokens.push_back(s);
            }
        }
        if (tokens.empty() && !text.empty()) tokens = tokenize(text);
        if (text.empty() && tokens.empty()) return json{{"ok", false}, {"reason", "empty-text"}};
        if (tokens.empty()) return json{{"ok", false}, {"reason", "no-tokens"}};

        auto memeIds = buildMemeSequenceFromTokens(tokens);
        for (size_t i = 0; i + 1 < memeIds.size(); i++) {
            graph_.link(memeIds[i], memeIds[i + 1], 1.0, 0);
        }
        std::string source = doc.value("source", "");
        for (auto &memeId : memeIds) {
            json meta = graph_.getMeta(memeId);
            meta["lastTouched"] = nowMs();
            meta["degree"] = graph_.degreeOf(memeId);
            if (!source.empty()) {
                if (!meta.contains("sources") || !meta["sources"].is_object()) meta["sources"] = json::object();
                meta["sources"][source] = meta["sources"].value(source, 0) + 1;
            }
            graph_.setMeta(memeId, meta);
        }
        corpusStats_["ingested"] = corpusStats_.value("ingested", 0) + 1;
        corpusStats_["lastIngest"] = nowMs();
        return json{{"ok", true}, {"memes", (int)memeIds.size()}, {"edges", (int)std::max<int>(0, (int)memeIds.size() - 1)}, {"source", source.empty() ? json(nullptr) : json(source)}};
    }

    json forgetMemes(const json &payload) override {
        int64_t threshold = payload.contains("olderThan") ? payload.value("olderThan", 0LL)
            : (nowMs() - 7LL * 24LL * 60LL * 60LL * 1000LL);
        int limit = payload.value("limit", 64);
        json removed = json::array();
        for (auto &kv : graph_.metaEntries()) {
            auto &meta = kv.second;
            if (meta.value("lastTouched", 0LL) >= threshold) continue;
            auto words = kvm_.getMemeWords(kv.first);
            for (auto &w : words) kvm_.unlink(w, kv.first);
            if (graph_.removeNode(kv.first)) {
                json ws = json::array();
                for (auto &w : words) ws.push_back(w);
                removed.push_back(json{{"memeId", kv.first}, {"words", ws}});
            }
            if (limit > 0 && (int)removed.size() >= limit) break;
        }
        return json{{"ok", true}, {"removed", removed}};
    }

    json onlineLookup(const std::string &query, const json &options) override {
        return onlineLookup(json(query), options);
    }

    json onlineLookup(const json &input, const json &options) override {
        if (!researcher_) {
            researcher_ = std::make_shared<OnlineResearcher>(this, searchConfig_.active);
            researcher_->setEnabled(searchConfig_.enabled);
        }
        return researcher_->lookup(input, options);
    }

    fs::path exportGraphToFile(const json &options) override {
        ensureDir(config_.exportDir);
        auto jsToNumber = [&](const json &v) -> double {
            if (v.is_null()) return std::numeric_limits<double>::quiet_NaN();
            if (v.is_number()) return v.get<double>();
            if (v.is_boolean()) return v.get<bool>() ? 1.0 : 0.0;
            if (v.is_string()) {
                std::string s = v.get<std::string>();
                auto start = s.find_first_not_of(" \n\r\t");
                if (start == std::string::npos) return 0.0;
                auto end = s.find_last_not_of(" \n\r\t");
                s = s.substr(start, end - start + 1);
                if (s.empty()) return 0.0;
                try { return std::stod(s); } catch (...) { return std::numeric_limits<double>::quiet_NaN(); }
            }
            return std::numeric_limits<double>::quiet_NaN();
        };
        int radius = 2;
        if (options.contains("radius")) {
            double r = jsToNumber(options["radius"]);
            if (!std::isfinite(r)) radius = 0;
            else radius = (int)r;
        }
        std::unordered_map<std::string, double> usedSeeds;
        std::vector<std::string> seedOrder;
        if (options.contains("seeds") && options["seeds"].is_array()) {
            std::vector<std::string> seedWords;
            for (auto &s : options["seeds"]) {
                if (s.is_string()) seedWords.push_back(s.get<std::string>());
                else if (s.is_number()) seedWords.push_back(s.dump());
                else if (s.is_boolean()) seedWords.push_back(s.get<bool>() ? "true" : "false");
            }
            auto mapping = mapWordsToMemesOrdered(seedWords);
            usedSeeds = mapping.seeds;
            seedOrder = mapping.order;
        } else {
            auto mapping = mapWordsToMemesOrdered(std::vector<std::string>{});
            usedSeeds = mapping.seeds;
            seedOrder = mapping.order;
        }
        std::vector<std::string> seedIds;
        if (!seedOrder.empty()) {
            seedIds = seedOrder;
        } else {
            for (auto &kv : usedSeeds) seedIds.push_back(kv.first);
        }
        auto win = graph_.buildWindow(seedIds, radius);
        std::vector<float> seedVec(win.ids.size(), 0.0f);
        for (auto &kv : usedSeeds) {
            auto it = win.index.find(kv.first);
            if (it != win.index.end()) seedVec[it->second] = (float)kv.second;
        }
        auto actFn = activationFn(params_);
        int iteration = params_.iteration ? params_.iteration : 5;
        if (iteration == 0) iteration = 5;
        auto activation = tensor_.iteratePropagation(win.csr, seedVec, iteration, actFn, params_.decayK, 0.02);
        auto graphObj = GraphExportBuilder::fromWindow(win, &activation);

        std::string fileArg = options.value("file", "");
        fs::path outFile;
        if (!fileArg.empty()) {
            outFile = fs::absolute(fs::path(fileArg));
        } else {
            std::string ts = nowIso();
            std::replace(ts.begin(), ts.end(), ':', '-');
            std::replace(ts.begin(), ts.end(), '.', '-');
            outFile = config_.exportDir / ("graph_export_" + ts + ".json");
        }
        std::ofstream o(outFile);
        o << graphObj.dump();
        return outFile;
    }

    json learnFromDialog(const json &payload, const json &result) override {
        try {
            json actualPayload = payload;
            json actualResult = result;
            if (payload.is_object() && payload.contains("payload") && payload.contains("result")) {
                if (result.is_null() || (result.is_object() && result.empty())) {
                    actualPayload = payload.value("payload", json::object());
                    actualResult = payload.value("result", json::object());
                }
            }
            auto trim = [](const std::string &s) {
                auto start = s.find_first_not_of(" \n\r\t");
                if (start == std::string::npos) return std::string();
                auto end = s.find_last_not_of(" \n\r\t");
                return s.substr(start, end - start + 1);
            };
            std::function<std::string(const json &)> jsStringForJoin;
            jsStringForJoin = [&](const json &v) -> std::string {
                if (v.is_null()) return "";
                if (v.is_string()) return v.get<std::string>();
                if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
                if (v.is_number()) return v.dump();
                if (v.is_array()) {
                    std::string out;
                    bool first = true;
                    for (auto &item : v) {
                        if (!first) out += ",";
                        first = false;
                        out += jsStringForJoin(item);
                    }
                    return out;
                }
                return "[object Object]";
            };

            std::string question;
            if (actualPayload.contains("text") && actualPayload["text"].is_string()) {
                question = actualPayload["text"].get<std::string>();
            } else if (actualPayload.contains("tokens") && actualPayload["tokens"].is_array()) {
                std::ostringstream oss;
                bool first = true;
                for (auto &t : actualPayload["tokens"]) {
                    std::string token = jsStringForJoin(t);
                    if (!first) oss << " ";
                    oss << token;
                    first = false;
                }
                question = oss.str();
            }

            std::string reply = actualResult.contains("reply") && actualResult["reply"].is_string()
                ? actualResult["reply"].get<std::string>()
                : std::string();

            if (trim(question).empty() || trim(reply).empty()) {
                return json{{"ok", false}, {"reason", "missing-text"}};
            }

            std::unordered_map<std::string, double> seeds;
            std::vector<std::string> seedOrder;
            auto jsToNumber = [&](const json &v) -> double {
                if (v.is_null()) return 0.0;
                if (v.is_number()) return v.get<double>();
                if (v.is_boolean()) return v.get<bool>() ? 1.0 : 0.0;
                if (v.is_string()) {
                    std::string s = v.get<std::string>();
                    auto start = s.find_first_not_of(" \n\r\t");
                    if (start == std::string::npos) return 0.0;
                    auto end = s.find_last_not_of(" \n\r\t");
                    s = s.substr(start, end - start + 1);
                    if (s.empty()) return 0.0;
                    try { return std::stod(s); } catch (...) { return std::numeric_limits<double>::quiet_NaN(); }
                }
                return std::numeric_limits<double>::quiet_NaN();
            };

            if (actualResult.contains("seeds") && actualResult["seeds"].is_array()) {
                for (auto &m : actualResult["seeds"]) {
                    if (m.is_array() && m.size() >= 2 && m[0].is_string()) {
                        std::string id = m[0].get<std::string>();
                        seeds[id] = jsToNumber(m[1]);
                        seedOrder.push_back(id);
                    }
                }
            } else {
                auto mapping = mapWordsToMemesOrdered(tokenize(question));
                seeds = mapping.seeds;
                seedOrder = mapping.order;
            }

            PropagationResult resObj;
            if (actualResult.contains("memes") && actualResult["memes"].is_array() && actualResult.contains("activation") && actualResult["activation"].is_array()) {
                for (auto &m : actualResult["memes"]) if (m.is_string()) resObj.windowInfo.ids.push_back(m.get<std::string>());
                for (size_t i = 0; i < resObj.windowInfo.ids.size(); i++) resObj.windowInfo.index[resObj.windowInfo.ids[i]] = (uint32_t)i;
                for (auto &a : actualResult["activation"]) resObj.activation.push_back((float)jsToNumber(a));
            } else {
                resObj = runPropagation(seeds, seedOrder.empty() ? json::object() : json{{"seedOrder", seedOrder}});
            }

            auto topMemes = pickTopActivatedMemes(resObj, seeds, 18, 1e-6);
            auto signature = makeSignatureFromTopMemes(topMemes, 12);
            std::vector<std::string> memeIds;
            for (auto &m : topMemes) memeIds.push_back(m.memeId);

            for (size_t i = 0; i < topMemes.size() && i < 10; i++) {
                double w = std::max(0.5, std::min(3.0, topMemes[i].score));
                surfaceLexicon_->learn(topMemes[i].memeId, reply, w);
            }
            double hint = topMemes.empty() ? 0.0 : topMemes.front().score;
            dialogMemory_->remember(signature, memeIds, question, reply, hint);
            int sigLen = 0;
            if (!signature.empty()) {
                sigLen = 1;
                for (char c : signature) if (c == '|') sigLen++;
            }
            return json{{"ok", true}, {"memes", (int)memeIds.size()}, {"signatureLen", sigLen}};
        } catch (const std::exception &e) {
            return json{{"ok", false}, {"error", e.what()}};
        }
    }

private:
    friend class OnlineResearcher;
    Config config_;
    KVMStore kvm_;
    MemeGraph graph_;
    TensorEngine tensor_;
    std::shared_ptr<KeyValueStore> sessions_;
    std::shared_ptr<SessionManager> sessionManager_;
    std::shared_ptr<NamespacedStore> surfaceStore_;
    std::shared_ptr<NamespacedStore> dialogStore_;
    std::shared_ptr<MemeSurfaceLexicon> surfaceLexicon_;
    std::shared_ptr<DialogMemory> dialogMemory_;
    mutable std::shared_ptr<RobotsCorpus> robotsCorpus_;
    ModelDefaults params_;
    json metrics_ = json{{"requests", 0}, {"lastLatency", 0}, {"updatedAt", 0}};
    json corpusStats_ = json{{"ingested", 0}, {"lastIngest", nullptr}};
    SearchConfig searchConfig_;
    std::shared_ptr<OnlineResearcher> researcher_;
    PatternMatrix pattern_;
    std::shared_ptr<addon::AddonManager> addons_;

    struct PropagationResult {
        MemeGraph::WindowInfo windowInfo;
        std::vector<float> seedVector;
        std::vector<float> activation;
    };

    struct TopMeme {
        std::string memeId;
        double score{0.0};
    };

    std::shared_ptr<RobotsCorpus> ensureRobotsCorpus() const {
        if (!robotsCorpus_) {
            robotsCorpus_ = std::make_shared<RobotsCorpus>(config_.robotsDir, config_.lemmaCsv, config_.lemmaAutoload,
                                                           config_.lemmaMaxBytes, config_.lemmaForce,
                                                           config_.robotsChunkMinWords, config_.robotsChunkMaxWords);
        }
        return robotsCorpus_;
    }

    static int64_t nowMs() {
        return (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    PropagationResult runPropagation(const std::unordered_map<std::string, double> &seeds, const json &options) {
        PropagationResult result;
        auto jsToNumber = [&](const json &v) -> double {
            if (v.is_null()) return std::numeric_limits<double>::quiet_NaN();
            if (v.is_number()) return v.get<double>();
            if (v.is_boolean()) return v.get<bool>() ? 1.0 : 0.0;
            if (v.is_string()) {
                std::string s = v.get<std::string>();
                auto start = s.find_first_not_of(" \n\r\t");
                if (start == std::string::npos) return 0.0;
                auto end = s.find_last_not_of(" \n\r\t");
                s = s.substr(start, end - start + 1);
                if (s.empty()) return 0.0;
                try { return std::stod(s); } catch (...) { return std::numeric_limits<double>::quiet_NaN(); }
            }
            return std::numeric_limits<double>::quiet_NaN();
        };
        auto numberOrHard = [&](const json *v, double paramFallback, double hardFallback) -> double {
            double n = paramFallback;
            if (v) n = jsToNumber(*v);
            if (!std::isfinite(n) || n == 0.0) return hardFallback;
            return n;
        };
        const json *radiusVal = options.contains("radius") && !options["radius"].is_null()
            ? &options["radius"]
            : (options.contains("windowRadius") && !options["windowRadius"].is_null() ? &options["windowRadius"] : nullptr);
        int radius = std::max(1, std::min(6, (int)numberOrHard(radiusVal, 2, 2)));
        int iteration = std::max(1, (int)numberOrHard(options.contains("iteration") && !options["iteration"].is_null() ? &options["iteration"] : nullptr,
                                                      params_.iteration ? params_.iteration : 5, 5));
        std::vector<std::string> seedIds;
        if (options.contains("seedOrder") && options["seedOrder"].is_array()) {
            for (auto &v : options["seedOrder"]) if (v.is_string()) seedIds.push_back(v.get<std::string>());
        }
        if (seedIds.empty()) {
            seedIds.reserve(seeds.size());
            for (auto &kv : seeds) seedIds.push_back(kv.first);
        }
        result.windowInfo = graph_.buildWindow(seedIds, radius);
        result.seedVector.assign(result.windowInfo.ids.size(), 0.0f);
        for (auto &kv : seeds) {
            auto it = result.windowInfo.index.find(kv.first);
            if (it != result.windowInfo.index.end()) result.seedVector[it->second] = (float)kv.second;
        }
        auto actFn = activationFn(params_);
        result.activation = tensor_.iteratePropagation(result.windowInfo.csr, result.seedVector, iteration, actFn, params_.decayK, 0.02);
        pattern_.rebuild(result.windowInfo);
        return result;
    }

    std::vector<TopMeme> pickTopActivatedMemes(const PropagationResult &result,
                                               const std::unordered_map<std::string, double> &seeds,
                                               int limit,
                                               double minScore) {
        std::vector<TopMeme> scored;
        std::unordered_set<std::string> seedIds;
        for (auto &kv : seeds) seedIds.insert(kv.first);

        auto isConnectedToSeeds = [&](const std::string &memeId) {
            if (seedIds.empty() || seedIds.count(memeId)) return true;
            for (auto &n : graph_.neighborsOf(memeId)) {
                if (seedIds.count(n.first)) return true;
            }
            for (auto &sid : seedIds) {
                for (auto &n : graph_.neighborsOf(sid)) {
                    if (n.first == memeId) return true;
                }
            }
            return false;
        };

        for (size_t i = 0; i < result.windowInfo.ids.size(); i++) {
            if (i >= result.activation.size()) break;
            double score = result.activation[i];
            if (!std::isfinite(score) || score <= minScore) continue;
            const auto &memeId = result.windowInfo.ids[i];
            if (!isConnectedToSeeds(memeId)) continue;
            scored.push_back({memeId, score});
        }
        std::sort(scored.begin(), scored.end(), [](auto &a, auto &b) { return a.score > b.score; });
        if ((int)scored.size() > limit) scored.resize(limit);
        return scored;
    }

    std::string makeSignatureFromTopMemes(const std::vector<TopMeme> &topMemes, int limit) {
        std::vector<std::string> ids;
        int cap = std::max(3, limit);
        for (auto &m : topMemes) {
            if (!m.memeId.empty()) ids.push_back(m.memeId);
            if ((int)ids.size() >= cap) break;
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        std::ostringstream oss;
        for (size_t i = 0; i < ids.size(); i++) {
            if (i) oss << "|";
            oss << ids[i];
        }
        return oss.str();
    }

    std::vector<std::string> buildMemeSequenceFromTokens(const std::vector<std::string> &tokens) {
        std::vector<std::string> list;
        auto trim = [](const std::string &s) {
            auto start = s.find_first_not_of(" \n\r\t");
            if (start == std::string::npos) return std::string();
            auto end = s.find_last_not_of(" \n\r\t");
            return s.substr(start, end - start + 1);
        };
        for (auto &t : tokens) {
            std::string s = trim(t);
            if (!s.empty()) list.push_back(s);
        }
        int nMin = std::max(2, params_.memeNgramMin);
        int nMax = std::max(nMin, params_.memeNgramMax);
        int minOverlap = std::max(1, params_.minOverlapThreshold);
        int maxWordSet = std::max(4, params_.maxMemeWords);

        auto resolveOrCreate = [&](const std::vector<std::string> &tokenSet) -> std::string {
            std::vector<std::string> uniq;
            std::unordered_set<std::string> seen;
            for (auto &t : tokenSet) {
                if (t.empty()) continue;
                if (seen.insert(t).second) uniq.push_back(t);
            }
            if ((int)uniq.size() > maxWordSet) uniq.resize(maxWordSet);
            if (uniq.size() <= 1) return uniq.empty() ? "" : ("meme_" + sha1Hex(uniq[0]));

            std::unordered_map<std::string, int> counts;
            for (auto &w : uniq) {
                for (auto &mid : kvm_.getWordMemeSet(w)) counts[mid]++;
            }
            std::string best; int bestOverlap = 0;
            for (auto &kv : counts) { if (kv.second > bestOverlap) { bestOverlap = kv.second; best = kv.first; } }
            if (!best.empty() && bestOverlap >= minOverlap) {
                for (auto &w : uniq) kvm_.link(w, best);
                graph_.ensureNode(best);
                return best;
            }
            std::vector<std::string> sorted = uniq;
            std::sort(sorted.begin(), sorted.end());
            std::string memeId = "meme_p_" + sha1Hex([&](){
                std::string join; for (size_t i = 0; i < sorted.size(); i++) { if (i) join += "|"; join += sorted[i]; } return join; }());
            graph_.ensureNode(memeId);
            for (auto &w : sorted) kvm_.link(w, memeId);
            return memeId;
        };

        std::vector<std::string> seq;
        for (size_t i = 0; i < list.size(); i++) {
            std::string picked;
            for (int n = nMax; n >= nMin; n--) {
                if (i + n > list.size()) continue;
                picked = resolveOrCreate(std::vector<std::string>(list.begin() + i, list.begin() + i + n));
                if (!picked.empty()) break;
            }
            if (picked.empty()) {
                auto &w = list[i];
                if (!w.empty()) {
                    picked = "meme_" + sha1Hex(w);
                    graph_.ensureNode(picked);
                    kvm_.link(w, picked);
                }
            }
            if (!picked.empty() && (seq.empty() || seq.back() != picked)) seq.push_back(picked);
        }
        return seq;
    }

    std::string composeReply(const PropagationResult &result,
                             const std::vector<std::string> &words,
                             const std::unordered_map<std::string, double> &seeds) {
        auto topMemes = pickTopActivatedMemes(result, seeds, 18, 1e-6);
        std::string signature = makeSignatureFromTopMemes(topMemes, 12);

        std::vector<std::string> topIds;
        for (auto &m : topMemes) topIds.push_back(m.memeId);
        json memoryHit = dialogMemory_ ? dialogMemory_->retrieve(topIds, signature) : json();
        if (!memoryHit.is_null() && memoryHit.contains("reply") && memoryHit["reply"].is_string()) {
            auto r = memoryHit["reply"].get<std::string>();
            auto start = r.find_first_not_of(" \n\r\t");
            if (start != std::string::npos) {
                auto end = r.find_last_not_of(" \n\r\t");
                r = r.substr(start, end - start + 1);
            } else {
                r.clear();
            }
            if (!r.empty()) return r;
        }

        std::unordered_map<std::string, double> phraseScores;
        for (auto &m : topMemes) {
            for (auto &p : surfaceLexicon_->getTop(m.memeId, 4)) {
                std::string key = p.phrase;
                auto start = key.find_first_not_of(" \n\r\t");
                if (start == std::string::npos) continue;
                auto end = key.find_last_not_of(" \n\r\t");
                key = key.substr(start, end - start + 1);
                if (key.empty()) continue;
                phraseScores[key] += std::max(0.0, m.score) * (0.5 + std::max(0.0, p.weight));
            }
        }
        if (!phraseScores.empty()) {
            std::vector<std::pair<std::string, double>> ordered(phraseScores.begin(), phraseScores.end());
            std::sort(ordered.begin(), ordered.end(), [](auto &a, auto &b) { return a.second > b.second; });
            if (ordered.size() > 2) ordered.resize(2);
            std::string out;
            for (size_t i = 0; i < ordered.size(); i++) {
                if (i) out += "。";
                out += ordered[i].first;
            }
            return out;
        }

        std::unordered_set<std::string> seedIds;
        for (auto &kv : seeds) seedIds.insert(kv.first);
        auto isConnectedToSeeds = [&](const std::string &memeId) {
            if (seedIds.empty() || seedIds.count(memeId)) return true;
            for (auto &n : graph_.neighborsOf(memeId)) {
                if (seedIds.count(n.first)) return true;
            }
            for (auto &sid : seedIds) {
                for (auto &n : graph_.neighborsOf(sid)) {
                    if (n.first == memeId) return true;
                }
            }
            return false;
        };

        std::unordered_map<std::string, double> candidateScores;
        for (size_t i = 0; i < result.windowInfo.ids.size(); i++) {
            if (i >= result.activation.size()) break;
            double score = result.activation[i];
            if (!std::isfinite(score) || score <= 0) continue;
            const auto &memeId = result.windowInfo.ids[i];
            if (!isConnectedToSeeds(memeId)) continue;
            for (auto &w : kvm_.getMemeWords(memeId)) {
                if (!w.empty()) candidateScores[w] = std::max(candidateScores[w], score);
            }
        }
        std::vector<std::string> ordered;
        for (auto &kv : candidateScores) ordered.push_back(kv.first);
        std::sort(ordered.begin(), ordered.end(), [&](auto &a, auto &b) { return candidateScores[a] > candidateScores[b]; });

        std::vector<std::string> finalWords;
        std::unordered_set<std::string> seen;
        auto pushWord = [&](const std::string &w) { if (!w.empty() && !seen.count(w)) { seen.insert(w); finalWords.push_back(w); } };
        for (auto &w : words) pushWord(w);
        for (auto &w : ordered) { pushWord(w); if (finalWords.size() >= 30) break; }
        if (finalWords.empty()) return "I need more context to respond.";
        std::ostringstream oss;
        for (size_t i = 0; i < finalWords.size() && i < 30; i++) { if (i) oss << " "; oss << finalWords[i]; }
        return oss.str();
    }
};

// ------------------ MemeBarrier ------------------

class MemeBarrier {
public:
    struct Stats {
        int scans{0};
        int isolated{0};
        int64_t lastScanTime{0};
        std::vector<json> lastIsolated;
        int evaluated{0};
        int candidates{0};
        double avgScore{0.0};
        double maxScore{0.0};
        double p90Score{0.0};
        double lastThreshold{0.0};
        double avgDegree{0.0};
        double stdDegree{0.0};
        double avgOutDegree{0.0};
        double stdOutDegree{0.0};
        std::vector<int> scoreHistogram;
    };

    MemeBarrier(std::shared_ptr<RuntimeState> runtime,
                int scanIntervalMs = 10000,
                double maliciousThreshold = 0.7,
                int maxIsolatePerScan = 5)
        : runtime_(std::move(runtime)), scanIntervalMs_(scanIntervalMs),
                    maliciousThreshold_(maliciousThreshold), baseThreshold_(maliciousThreshold),
                    maxIsolatePerScan_(maxIsolatePerScan) {}

    void start() {
        if (running_) return;
        running_ = true;
        worker_ = std::thread([this]() {
            while (running_) {
                try { scanNetwork(); } catch (...) {}
                std::this_thread::sleep_for(std::chrono::milliseconds(scanIntervalMs_));
            }
        });
    }

    void stop() {
        running_ = false;
        if (worker_.joinable()) worker_.join();
    }

    bool running() const { return running_; }

    void setThreshold(double t) {
        baseThreshold_ = t;
        maliciousThreshold_ = t;
    }
    double threshold() const { return maliciousThreshold_; }

    Stats getStats() const { return stats_; }

private:
    struct NodeStats {
        int degree{0};
        int outDegree{0};
        int selfLoops{0};
    };

    std::shared_ptr<RuntimeState> runtime_;
    int scanIntervalMs_{10000};
    double maliciousThreshold_{0.7};
    double baseThreshold_{0.7};
    double minThreshold_{0.35};
    double maxThreshold_{0.95};
    double thresholdMomentum_{0.15};
    double minIsolationMargin_{0.05};
    int minConsecutiveHits_{2};
    bool adaptiveEnabled_{true};
    bool anomalyEnabled_{true};
    int maxIsolatePerScan_{5};
    std::atomic<bool> running_{false};
    std::thread worker_;
    Stats stats_;
    std::deque<float> scoreWindow_;
    size_t scoreWindowSize_{512};
    std::unordered_map<std::string, int> hitCounts_;

    double evaluateMaliciousness(const std::string &memeId, const NodeStats &ns, double avgConn,
                                 double avgOut, double stdDeg, double stdOut) {
        if (!runtime_) return 0.0;
        double outSkew = avgConn > 0 ? std::min(1.0, ns.outDegree / (avgConn * 3.0)) : 0;
        double selfSkew = ns.degree > 0 ? std::min(1.0, (double)ns.selfLoops / ns.degree) : 0;
        double zDeg = (stdDeg > 1e-6) ? (ns.degree - avgConn) / stdDeg : 0.0;
        double zOut = (stdOut > 1e-6) ? (ns.outDegree - avgOut) / stdOut : 0.0;
        double anomaly = anomalyEnabled_ ? clamp01((std::max(0.0, zDeg) + std::max(0.0, zOut)) / 6.0) : 0.0;
        double growth = 0; // 暂无 access log，对齐 JS 的可选逻辑
        double score = 0.5 * growth + 0.25 * outSkew + 0.15 * selfSkew + 0.10 * anomaly;
        return clamp01(score);
    }

    double getAverageConnections() {
        if (!runtime_) return 0.0;
        auto points = runtime_->graph().getAllPoints();
        if (points.empty()) return 0;
        double sum = 0;
        for (auto &id : points) sum += runtime_->graph().degreeOf(id);
        return sum / points.size();
    }

    std::string generateReason(const std::string &memeId, double score) {
        if (!runtime_) return "score=0.000 conn=0";
        int degree = runtime_->graph().degreeOf(memeId);
        std::ostringstream oss;
        oss << "score=" << std::fixed << std::setprecision(3) << score << " conn=" << degree;
        return oss.str();
    }

    bool isolateMeme(const std::string &memeId, double score, const std::string &reason) {
        if (!runtime_) return false;
        int cut = runtime_->graph().removeOutgoingEdges(memeId);
        if (cut > 0) {
            stats_.isolated++;
            json item{{"memeID", memeId}, {"score", score}, {"reason", reason}, {"cut", cut}};
            stats_.lastIsolated.insert(stats_.lastIsolated.begin(), item);
            if (stats_.lastIsolated.size() > 20) stats_.lastIsolated.resize(20);
            return true;
        }
        return false;
    }

    void scanNetwork() {
        if (!runtime_) return;
        auto points = runtime_->graph().getAllPoints();
        stats_.scans++;
        stats_.lastScanTime = nowEpochMs();
        if (points.empty()) return;
        struct Scored { std::string id; double score; std::string reason; };
        std::vector<Scored> scored;
        scored.reserve(points.size());
        std::unordered_map<std::string, NodeStats> nodeStats;
        nodeStats.reserve(points.size());
        double sumDeg = 0.0;
        double sumOut = 0.0;
        double sumDeg2 = 0.0;
        double sumOut2 = 0.0;
        for (auto &id : points) {
            auto neighbors = runtime_->graph().neighborsOf(id);
            NodeStats ns;
            ns.degree = (int)neighbors.size();
            for (auto &n : neighbors) {
                if (n.first == id) ns.selfLoops++;
                if (n.second.second == 2) ns.outDegree++;
            }
            nodeStats.emplace(id, ns);
            sumDeg += ns.degree;
            sumOut += ns.outDegree;
            sumDeg2 += (double)ns.degree * ns.degree;
            sumOut2 += (double)ns.outDegree * ns.outDegree;
        }
        double avgDeg = sumDeg / points.size();
        double avgOut = sumOut / points.size();
        double varDeg = sumDeg2 / points.size() - avgDeg * avgDeg;
        double varOut = sumOut2 / points.size() - avgOut * avgOut;
        double stdDeg = std::sqrt(std::max(0.0, varDeg));
        double stdOut = std::sqrt(std::max(0.0, varOut));
        stats_.avgDegree = avgDeg;
        stats_.stdDegree = stdDeg;
        stats_.avgOutDegree = avgOut;
        stats_.stdOutDegree = stdOut;

        std::vector<double> scoreValues;
        scoreValues.reserve(points.size());
        for (auto &id : points) {
            const auto &ns = nodeStats[id];
            double s = evaluateMaliciousness(id, ns, avgDeg, avgOut, stdDeg, stdOut);
            scored.push_back(Scored{id, s, generateReason(id, s)});
            scoreValues.push_back(s);
        }

        std::sort(scoreValues.begin(), scoreValues.end());
        stats_.evaluated = (int)scoreValues.size();
        stats_.avgScore = 0.0;
        stats_.maxScore = scoreValues.empty() ? 0.0 : scoreValues.back();
        for (double v : scoreValues) stats_.avgScore += v;
        if (!scoreValues.empty()) stats_.avgScore /= scoreValues.size();
        if (!scoreValues.empty()) {
            size_t p90 = (size_t)std::floor(0.9 * (scoreValues.size() - 1));
            stats_.p90Score = scoreValues[p90];
        } else {
            stats_.p90Score = 0.0;
        }

        stats_.scoreHistogram.assign(10, 0);
        for (double v : scoreValues) {
            int bucket = (int)std::floor(std::min(0.999, std::max(0.0, v)) * 10.0);
            stats_.scoreHistogram[bucket]++;
        }

        double dynamicTarget = clamp01(0.7 * baseThreshold_ + 0.3 * stats_.p90Score);
        std::sort(scored.begin(), scored.end(), [](const Scored &a, const Scored &b){ return a.score > b.score; });
        int isolated = 0;
        stats_.candidates = 0;
        for (auto &item : scored) {
            if (item.score >= maliciousThreshold_) {
                stats_.candidates++;
                auto &hits = hitCounts_[item.id];
                if (item.score >= maliciousThreshold_ + minIsolationMargin_) hits = minConsecutiveHits_;
                else hits = std::min(minConsecutiveHits_, hits + 1);
                if (hits >= minConsecutiveHits_) {
                    if (isolateMeme(item.id, item.score, item.reason)) {
                        isolated++;
                        if (isolated >= maxIsolatePerScan_) break;
                    }
                }
            } else {
                hitCounts_[item.id] = 0;
                break;
            }
        }
        if (adaptiveEnabled_ && !scoreValues.empty()) {
            double isolatedRate = stats_.evaluated > 0 ? (double)isolated / stats_.evaluated : 0.0;
            if (isolatedRate > 0.02) dynamicTarget = clamp01(dynamicTarget + 0.05);
            if (isolatedRate < 0.002) dynamicTarget = clamp01(dynamicTarget - 0.02);
            double blended = maliciousThreshold_ * (1.0 - thresholdMomentum_) + dynamicTarget * thresholdMomentum_;
            maliciousThreshold_ = std::min(maxThreshold_, std::max(minThreshold_, blended));
        }
        stats_.lastThreshold = maliciousThreshold_;
    }
};

std::string OnlineResearcher::normalizeKey(const std::vector<std::string> &words) const {
    std::ostringstream oss;
    int limit = std::min<int>(32, (int)words.size());
    for (int i = 0; i < limit; i++) {
        if (i) oss << " ";
        std::string w = words[i];
        for (auto &c : w) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        oss << w;
    }
    return oss.str();
}

void OnlineResearcher::pruneCache() {
    if ((int)cache_.size() <= cacheSize_) return;
    std::vector<std::pair<std::string, CacheEntry>> items(cache_.begin(), cache_.end());
    std::sort(items.begin(), items.end(), [](auto &a, auto &b) { return a.second.ts < b.second.ts; });
    while ((int)items.size() > cacheSize_) {
        cache_.erase(items.front().first);
        items.erase(items.begin());
    }
}

void OnlineResearcher::remember(const std::string &key, const json &payload) {
    cache_[key] = CacheEntry{(int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count(), payload};
    pruneCache();
}

std::optional<json> OnlineResearcher::fromCache(const std::string &key) {
    auto it = cache_.find(key);
    if (it == cache_.end()) return std::nullopt;
    int64_t nowMs = (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    if (nowMs - it->second.ts > cooldownMs_) {
        cache_.erase(it);
        return std::nullopt;
    }
    return it->second.payload;
}

json OnlineResearcher::fallback(const std::vector<std::string> &words) {
    if (!runtime_) return json{{"ok", false}, {"source", "local"}, {"reason", "runtime-missing"}};
    if (words.empty()) return json{{"ok", false}, {"source", "local"}, {"reason", "empty-query"}};
    auto mapping = runtime_->mapWordsToMemesOrdered(words);
    auto seeds = mapping.seeds;
    if (seeds.empty()) {
        return json{{"ok", false}, {"source", "local"}, {"reason", "no-memes"}};
    }
    auto result = runtime_->runPropagation(seeds, json{{"seedOrder", mapping.order}});
    json suggestions = json::array();
    int limit = std::min<int>(10, (int)result.windowInfo.ids.size());
    for (int i = 0; i < limit; i++) {
        std::string memeId = result.windowInfo.ids[i];
        double strength = (i < (int)result.activation.size() && std::isfinite(result.activation[i])) ? result.activation[i] : 0.0;
        std::vector<std::string> wordsForMeme;
        for (auto &w : runtime_->kvm().getMemeWords(memeId)) {
            wordsForMeme.push_back(w);
            if (wordsForMeme.size() >= 6) break;
        }
        suggestions.push_back(json{{"memeId", memeId}, {"strength", strength}, {"words", wordsForMeme}});
    }
    std::ostringstream oss;
    for (size_t i = 0; i < words.size(); i++) { if (i) oss << " "; oss << words[i]; }
    return json{{"ok", true}, {"source", "local"}, {"query", oss.str()}, {"suggestions", suggestions}};
}

json OnlineResearcher::crawl(const std::string &startUrl, const json &crawlReq, const json &options) {
    json out;
    out["ok"] = true;
    out["source"] = "crawl";
    out["pages"] = json::array();
    if (startUrl.empty()) return out;
#ifdef HAVE_CURL
    int timeoutMs = clampInt(crawlReq.value("timeoutMs", json()), 1000, 60000, 12000);
    int maxPages = clampInt(crawlReq.value("maxPages", json()), 1, 500, 60);
    int maxDepth = clampInt(crawlReq.value("maxDepth", json()), 0, 10, 3);
    bool includePdf = crawlReq.value("includePdf", true);
    bool sameSite = crawlReq.value("sameSite", true);
    int maxBytesPerPage = clampInt(crawlReq.value("maxBytesPerPage", json()), 8 * 1024, 10 * 1024 * 1024, 2 * 1024 * 1024);
    int maxPdfBytes = clampInt(crawlReq.value("maxPdfBytes", json()), 64 * 1024, 40 * 1024 * 1024, 20 * 1024 * 1024);
    (void)maxPdfBytes;
    std::string userAgent = crawlReq.value("userAgent", "079ProjectCrawler/1.0");
    json errors = json::array();
    std::queue<std::pair<std::string, int>> q;
    std::unordered_set<std::string> visited;
    std::string normalizedStart = normalizeUrlSimple(startUrl);
    q.push({normalizedStart, 0});
    while (!q.empty() && (int)out["pages"].size() < maxPages) {
        auto [url, depth] = q.front(); q.pop();
        if (url.empty() || visited.count(url)) continue;
        visited.insert(url);
        if (sameSite && !sameSiteSimple(startUrl, url)) continue;

        auto res = fetchUrl(url, timeoutMs, userAgent, (size_t)maxBytesPerPage);
        std::string text;
        std::vector<std::string> links;
#ifdef HAVE_GUMBO
        if (res.contentType.find("text/html") != std::string::npos || res.body.find("<html") != std::string::npos) {
            text = extractTextFromHtml(res.body);
            std::regex hrefRe(R"(\b(?:href|src)\s*=\s*['"]([^'"#]+)['"])" , std::regex::icase);
            std::smatch m;
            std::string::const_iterator searchStart(res.body.cbegin());
            while (std::regex_search(searchStart, res.body.cend(), m, hrefRe)) {
                std::string link = m[1].str();
                if (link.rfind("javascript:", 0) == 0 || link.rfind("mailto:", 0) == 0) { searchStart = m.suffix().first; continue; }
                if (link.rfind("http", 0) != 0 && link.rfind("//", 0) != 0) {
                    std::string base = url;
                    if (!base.empty() && base.back() != '/') base += '/';
                    link = base + link;
                } else if (link.rfind("//", 0) == 0) {
                    link = std::string("https:") + link;
                }
                link = normalizeUrlSimple(link);
                if (!link.empty()) links.push_back(link);
                searchStart = m.suffix().first;
            }
        }
#endif
#ifdef HAVE_POPPLER
        if (text.empty() && includePdf && (res.contentType.find("application/pdf") != std::string::npos || url.find(".pdf") != std::string::npos)) {
            fs::path tmp = runtime_ ? (runtime_->config().baseDir / "_tmp_fetch.pdf") : fs::path("_tmp_fetch.pdf");
            std::ofstream ofs(tmp, std::ios::binary); ofs << res.body; ofs.close();
            text = extractTextFromPdfFile(tmp);
        }
#endif
        if (!text.empty()) {
            std::string clipped = text.size() > 20000 ? text.substr(0, 20000) : text;
            out["pages"].push_back(json{{"url", url}, {"status", res.status}, {"contentType", res.contentType}, {"bytes", (int64_t)res.bytes}, {"linksFound", (int)links.size()}, {"text", clipped}});
        } else if (res.status <= 0) {
            errors.push_back(json{{"url", url}, {"error", "fetch-failed"}});
        }

        if (depth < maxDepth) {
            for (auto &link : links) {
                if (link.empty() || visited.count(link)) continue;
                if (sameSite && !sameSiteSimple(startUrl, link)) continue;
                q.push({link, depth + 1});
            }
        }
    }

    json chunks = json::array();
    std::string aggregated;
    for (auto &p : out["pages"]) {
        std::string url = p.value("url", "");
        std::string text = p.value("text", "");
        if (text.empty()) continue;
        std::string block = "URL: " + url + "\n" + text;
        if (aggregated.size() + block.size() + 2 > 60000) break;
        if (!aggregated.empty()) aggregated += "\n\n";
        aggregated += block;
    }
    out["startUrl"] = normalizedStart;
    out["stats"] = json{{"visited", (int)visited.size()}, {"returned", (int)out["pages"].size()}, {"queued", (int)q.size()}, {"errors", (int)errors.size()}};
    out["text"] = aggregated;
    json errOut = json::array();
    for (size_t i = 0; i < errors.size() && i < 20; i++) errOut.push_back(errors[i]);
    out["errors"] = errOut;
#endif
    (void)options;
    return out;
}

json OnlineResearcher::lookup(const json &input, const json &options) {
    std::string rawText;
    std::vector<std::string> words;
    std::function<std::string(const json &)> jsStringForJoin;
    jsStringForJoin = [&](const json &v) -> std::string {
        if (v.is_null()) return "";
        if (v.is_string()) return v.get<std::string>();
        if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
        if (v.is_number()) return v.dump();
        if (v.is_array()) {
            std::string out;
            bool first = true;
            for (auto &item : v) {
                if (!first) out += ",";
                first = false;
                out += jsStringForJoin(item);
            }
            return out;
        }
        return "[object Object]";
    };
    auto jsStringOrEmpty = [&](const json &v) -> std::string {
        if (v.is_null()) return "";
        if (v.is_boolean()) return v.get<bool>() ? "true" : "";
        if (v.is_number()) {
            double n = v.get<double>();
            if (n == 0.0) return "";
            return v.dump();
        }
        if (v.is_string()) return v.get<std::string>();
        if (v.is_array()) {
            std::string out;
            bool first = true;
            for (auto &item : v) {
                if (!first) out += ",";
                first = false;
                out += jsStringForJoin(item);
            }
            return out;
        }
        return "[object Object]";
    };

    if (input.is_array()) {
        for (auto &t : input) {
            words.push_back(jsStringForJoin(t));
        }
        for (size_t i = 0; i < words.size(); i++) { if (i) rawText += " "; rawText += words[i]; }
    } else {
        rawText = jsStringOrEmpty(input);
        words = tokenize(rawText);
    }

    if (!enabled_ || options.value("disableRemote", false)) {
        return fallback(words);
    }

    auto crawlReq = options.value("crawl", json::object());
    std::string urlFromInput = extractFirstUrl(rawText);
    bool shouldCrawl = options.value("mode", "search") == "crawl" || (!urlFromInput.empty()) || (crawlReq.contains("startUrl") && crawlReq["startUrl"].is_string());
    if (shouldCrawl) {
        std::string startUrl = crawlReq.value("startUrl", urlFromInput);
        if (startUrl.empty()) return json{{"ok", false}, {"source", "crawl"}, {"reason", "startUrl-required"}};
        auto payload = crawl(startUrl, crawlReq, options);
        if (!options.value("forceRemote", false)) {
            auto key = normalizeKey(tokenize("crawl " + startUrl));
            remember(key, payload);
        }
        return payload;
    }

    if (words.empty()) {
        return json{{"ok", false}, {"source", "local"}, {"reason", "empty-query"}};
    }
    std::string key = normalizeKey(words);
    if (!options.value("forceRemote", false)) {
        auto cached = fromCache(key);
        if (cached) {
            json payload = *cached;
            payload["cached"] = true;
            return payload;
        }
    }

    json payload;
    bool remoteOk = false;
#ifdef HAVE_CURL
    if (!endpoint_.empty() && !options.value("skipRemote", false)) {
        std::string url = endpoint_;
        if (url.find('?') == std::string::npos) url += "?q=" + urlEncode(rawText);
        else url += "&q=" + urlEncode(rawText);
        int timeoutMs = options.value("timeout", 5000);
        auto res = fetchUrl(url, timeoutMs);
        if (!res.body.empty()) {
            json raw;
            try { raw = json::parse(res.body); } catch (...) { raw = json{{"raw", res.body}}; }
            std::string snippet;
            if (raw.is_string()) snippet = raw.get<std::string>().substr(0, 280);
            else snippet = raw.dump().substr(0, 280);
            payload = json{{"ok", true}, {"source", "remote"}, {"query", rawText}, {"snippet", snippet}};
            if (options.value("includeRaw", false)) payload["raw"] = raw;
            remoteOk = true;
        }
    }
#endif
    if (!remoteOk) {
        payload = fallback(words);
    }

    remember(key, payload);
    return payload;
}

// ------------------ Controller / Pool ------------------

class Controller;

class ControllerPoolBase {
public:
    virtual ~ControllerPoolBase() = default;
    virtual std::shared_ptr<Controller> getActive() = 0;
    virtual std::shared_ptr<Controller> getStandby() = 0;
    virtual std::shared_ptr<Controller> getValidation() = 0;
    virtual std::shared_ptr<Controller> getByName(const std::string &name) = 0;
    virtual std::vector<std::string> listControllerNames() const = 0;
    virtual std::vector<std::string> listGroupIds() const = 0;
    virtual std::vector<std::string> listControllersInGroup(const std::string &gid) = 0;
    virtual std::vector<json> listMetrics() const = 0;
    virtual json ingestDocument(const json &doc) = 0;
    virtual json ingestDocumentToGroup(const std::string &gid, const json &doc) = 0;
    virtual json forgetMemes(const json &payload) = 0;
    virtual json onlineResearch(const json &queryOrTokens, const json &options) = 0;
    virtual void hotSwap(const json &snapshot) = 0;
    virtual const Config &config() const = 0;
    virtual json respondByName(const std::string &controllerName, const json &payload) = 0;
};

class Controller {
public:
    virtual ~Controller() = default;
    virtual json respond(const json &payload) = 0;
    virtual json metrics() const = 0;
    virtual void applyParams(const json &p) = 0;
    virtual json snapshot() const = 0;
    virtual void applySnapshot(const json &snap) = 0;
    virtual const std::string &name() const = 0;
    virtual std::shared_ptr<RuntimeBase> runtime() const = 0;
};

class ShardDescriptor {
public:
    explicit ShardDescriptor(std::string name)
        : name_(std::move(name)), label_("AI-" + name_), embedding_(64, 0.0f) {}

    const std::string &name() const { return name_; }
    const std::string &label() const { return label_; }
    const std::vector<float> &embedding() const { return embedding_; }

    void updateEmbedding(const std::vector<float> &vec) {
        double alpha = 0.2;
        if (vec.size() != embedding_.size()) return;
        for (size_t i = 0; i < embedding_.size(); i++) {
            embedding_[i] = (float)((1 - alpha) * embedding_[i] + alpha * vec[i]);
        }
    }

    void record(const json &entry) {
        history_.push_back(entry);
        if (history_.size() > 50) history_.erase(history_.begin());
    }

    json toJson() const {
        return json{{"name", name_}, {"label", label_}, {"offline", offline_}, {"history", history_}};
    }

private:
    std::string name_;
    std::string label_;
    std::vector<float> embedding_;
    std::vector<json> history_;
    bool offline_{false};
};

class ShardManager {
public:
    explicit ShardManager(std::shared_ptr<ControllerPoolBase> pool) : pool_(std::move(pool)) {
        auto names = pool_->listControllerNames();
        for (auto &name : names) {
            shards_.emplace(name, ShardDescriptor(name));
        }
        lastEmbedding_ = std::vector<float>(64, 0.0f);
    }

    std::shared_ptr<Controller> chooseShard(const std::string &text) {
        auto requestEmbedding = textToMiniEmbedding(text, 64);
        lastEmbedding_ = requestEmbedding;
        auto best = pool_->getActive();
        double bestScore = -1e9;
        for (auto &kv : shards_) {
            auto score = cosineSim(requestEmbedding, kv.second.embedding());
            if (score > bestScore) {
                auto ctrl = pool_->getByName(kv.first);
                if (ctrl) {
                    bestScore = score;
                    best = ctrl;
                }
            }
        }
        return best;
    }

    json metrics() const {
        json out = json::array();
        for (auto &kv : shards_) {
            out.push_back({{"name", kv.first}, {"shard", kv.second.toJson()}});
        }
        return out;
    }

    void record(const std::string &controllerName, const std::vector<float> &requestEmbedding, const std::string &replyText, int latency) {
        auto it = shards_.find(controllerName);
        if (it == shards_.end()) return;
        auto replyEmbedding = textToMiniEmbedding(replyText, (int)requestEmbedding.size());
        it->second.updateEmbedding(replyEmbedding);
        double affinity = cosineSim(requestEmbedding, replyEmbedding);
        it->second.record(json{{"latency", latency}, {"affinity", affinity}, {"ts", (int64_t)std::chrono::system_clock::now().time_since_epoch().count()}});
    }

private:
    std::shared_ptr<ControllerPoolBase> pool_;
    std::unordered_map<std::string, ShardDescriptor> shards_;
    std::vector<float> lastEmbedding_;
};

class ControllerPool;

class InferenceProcessPool {
public:
    InferenceProcessPool(ControllerPool *pool, int workers, int timeoutMs);
    ~InferenceProcessPool();

    json respond(const std::string &controllerName, const json &payload);
    void stop();

private:
    struct Task {
        std::string controllerName;
        json payload;
        std::shared_ptr<std::promise<json>> promise;
    };

    ControllerPool *pool_{nullptr};
    int timeoutMs_{12000};
    std::atomic<bool> running_{false};
    std::vector<std::thread> workers_;
    std::queue<std::shared_ptr<Task>> queue_;
    std::mutex mu_;
    std::condition_variable cv_;

    void workerLoop();
};

class GroupProcessPool;

class ProxyRuntime : public RuntimeBase {
public:
    ProxyRuntime(GroupProcessPool *pool, std::string controllerName)
        : pool_(pool), controllerName_(std::move(controllerName)) {}

    json processInput(const json &payload) override;
    json cloneParams() const override;
    void setParams(const json &patch) override;
    json toSnapshot() override;
    void fromSnapshot(const json &snapshot) override;
    json ingestDocument(const json &doc) override;
    json forgetMemes(const json &payload) override;
    json onlineLookup(const std::string &query, const json &options) override;
    json onlineLookup(const json &input, const json &options) override;
    json getSearchConfig() const override;
    json setSearchConfig(const json &patch) override;
    std::vector<RobotsDocument> collectRobotsDocuments(const json &options) const override;
    std::vector<std::string> listRobotsFiles() const override;
    fs::path exportGraphToFile(const json &options) override;
    json learnFromDialog(const json &payload, const json &result) override;
    std::unordered_map<std::string, double> mapWordsToMemes(const std::vector<std::string> &words) override;
    std::vector<std::string> getMemeWords(const std::string &memeId) override;
    void graphLink(const std::string &fromId, const std::string &toId, double weight, int direction) override;
    const Config &config() const override;

private:
    GroupProcessPool *pool_{nullptr};
    std::string controllerName_;
};

class ProxyController : public Controller {
public:
    ProxyController(GroupProcessPool *pool, std::string name)
        : pool_(pool), name_(std::move(name)), runtime_(std::make_shared<ProxyRuntime>(pool, name_)) {}

    json respond(const json &payload) override;
    json metrics() const override;
    void applyParams(const json &p) override;
    json snapshot() const override;
    void applySnapshot(const json &snap) override;
    const std::string &name() const override { return name_; }
    std::shared_ptr<RuntimeBase> runtime() const override { return runtime_; }

private:
    GroupProcessPool *pool_{nullptr};
    std::string name_;
    std::shared_ptr<ProxyRuntime> runtime_;
};

class LocalController : public Controller {
public:
    LocalController(std::string name, std::shared_ptr<RuntimeState> runtime)
        : name_(std::move(name)), runtime_(std::move(runtime)), since_(nowEpochMs()) {}

    json respond(const json &payload) override { return runtime_->processInput(payload); }
    json metrics() const override {
        return json{{"name", name_}, {"online", true}, {"health", json{{"status", "ok"}, {"since", since_}, {"failures", 0}}}, {"metrics", runtime_->metrics()}, {"params", runtime_->cloneParams()}};
    }
    void applyParams(const json &p) override { runtime_->setParams(p); }
    json snapshot() const override { return runtime_->toSnapshot(); }
    void applySnapshot(const json &snap) override { runtime_->fromSnapshot(snap); }
    const std::string &name() const override { return name_; }
    std::shared_ptr<RuntimeBase> runtime() const override { return runtime_; }

private:
    std::string name_;
    std::shared_ptr<RuntimeState> runtime_;
    int64_t since_{0};
};

class ControllerPool : public ControllerPoolBase {
public:
    ControllerPool(std::shared_ptr<KeyValueStore> kvmStore,
                   std::shared_ptr<KeyValueStore> memeStore,
                   std::shared_ptr<KeyValueStore> sessionStore,
                   const Config &config)
        : config_(config) {
#ifdef HAVE_LMDB
        auto lmdbK = std::make_shared<LmdbStore>("kvm", config_.lmdbRoot, config_.lmdbMapSizeBytes);
        auto lmdbM = std::make_shared<LmdbStore>("meme_graph", config_.lmdbRoot, config_.lmdbMapSizeBytes);
        auto lmdbS = std::make_shared<LmdbStore>("session", config_.lmdbRoot, config_.lmdbMapSizeBytes);
        if (lmdbK->ok()) kvmStore = lmdbK;
        if (lmdbM->ok()) memeStore = lmdbM;
        if (lmdbS->ok()) sessionStore = lmdbS;
#endif
        for (int i = 0; i < config_.groupCount; i++) {
            std::string gid = "G" + std::to_string(i + 1);
            groupIds_.push_back(gid);
            std::vector<std::string> names;
            auto kvmNs = std::make_shared<NamespacedStore>(kvmStore, gid + ":kvm");
            auto memeNs = std::make_shared<NamespacedStore>(memeStore, gid + ":graph");
            auto sessNs = std::make_shared<NamespacedStore>(sessionStore, gid + ":session");
            for (int j = 0; j < config_.groupSize; j++) {
                std::string name = gid + "_AI" + std::to_string(j + 1);
                controllers_[name] = std::make_shared<LocalController>(name, std::make_shared<RuntimeState>(kvmNs, memeNs, sessNs, config_));
                names.push_back(name);
            }
            groups_[gid] = names;
        }
        auto g1 = groupIds_.empty() ? "G1" : groupIds_[0];
        auto list = groups_[g1];
        serving_ = controllers_[list[0]];
        standby_ = controllers_[list.size() > 1 ? list[1] : list[0]];
        validation_ = controllers_[list.size() > 2 ? list[2] : list[0]];
        if (config_.inferMP && !config_.groupProc) {
            inferencePool_ = std::make_unique<InferenceProcessPool>(this, config_.inferWorkers, config_.groupProcTimeoutMs);
        }
    }

    std::shared_ptr<Controller> getActive() override { return serving_; }
    std::shared_ptr<Controller> getStandby() override { return standby_; }
    std::shared_ptr<Controller> getValidation() override { return validation_; }

    std::shared_ptr<Controller> getByName(const std::string &name) override {
        if (name == "A" || name == "B" || name == "C") {
            auto g1 = groupIds_.empty() ? "G1" : groupIds_[0];
            auto list = groups_[g1];
            if (name == "A" && !list.empty()) return controllers_[list[0]];
            if (name == "B" && list.size() > 1) return controllers_[list[1]];
            if (name == "C" && list.size() > 2) return controllers_[list[2]];
        }
        auto it = controllers_.find(name);
        return it == controllers_.end() ? nullptr : it->second;
    }

    std::vector<std::string> listControllerNames() const override {
        std::vector<std::string> out;
        for (auto &gid : groupIds_) {
            auto it = groups_.find(gid);
            if (it == groups_.end()) continue;
            for (auto &name : it->second) out.push_back(name);
        }
        return out;
    }

    std::vector<std::string> listGroupIds() const override { return groupIds_; }

    std::vector<std::string> listControllersInGroup(const std::string &gid) override {
        return groups_.count(gid) ? groups_.at(gid) : std::vector<std::string>{};
    }

    std::vector<json> listMetrics() const override {
        std::vector<json> out;
        for (auto &name : listControllerNames()) {
            auto it = controllers_.find(name);
            if (it != controllers_.end()) out.push_back(it->second->metrics());
        }
        return out;
    }

    json ingestDocument(const json &doc) override {
        json results = json::array();
        for (auto &name : listControllerNames()) {
            auto ctrl = getByName(name);
            if (!ctrl) {
                results.push_back(json{{"ok", false}, {"reason", "controller-not-found"}});
                continue;
            }
            results.push_back(ctrl->runtime()->ingestDocument(doc));
        }
        return results;
    }

    json ingestDocumentToGroup(const std::string &gid, const json &doc) override {
        auto list = listControllersInGroup(gid);
        if (list.empty()) return json{{"ok", false}, {"reason", "group-not-found"}};
        json results = json::array();
        for (auto &name : list) {
            auto ctrl = getByName(name);
            if (!ctrl) {
                results.push_back(json{{"ok", false}, {"reason", "controller-not-found"}});
                continue;
            }
            results.push_back(ctrl->runtime()->ingestDocument(doc));
        }
        return results;
    }

    json forgetMemes(const json &payload) override {
        json results = json::array();
        for (auto &name : listControllerNames()) {
            auto ctrl = getByName(name);
            if (!ctrl) {
                results.push_back(json{{"ok", false}, {"reason", "controller-not-found"}});
                continue;
            }
            results.push_back(ctrl->runtime()->forgetMemes(payload));
        }
        return results;
    }

    json onlineResearch(const json &queryOrTokens, const json &options) override {
        return serving_->runtime()->onlineLookup(queryOrTokens, options);
    }

    void hotSwap(const json &snapshot) override {
        standby_->applySnapshot(snapshot);
        std::swap(serving_, standby_);
    }

    const Config &config() const override { return config_; }

    json respondByName(const std::string &controllerName, const json &payload) override {
        if (inferencePool_) {
            return inferencePool_->respond(controllerName, payload);
        }
        auto ctrl = getByName(controllerName);
        if (!ctrl) throw std::runtime_error("controller-offline");
        return ctrl->respond(payload);
    }

private:
    Config config_;
    std::unordered_map<std::string, std::shared_ptr<Controller>> controllers_;
    std::unordered_map<std::string, std::vector<std::string>> groups_;
    std::vector<std::string> groupIds_;
    std::shared_ptr<Controller> serving_;
    std::shared_ptr<Controller> standby_;
    std::shared_ptr<Controller> validation_;
    std::unique_ptr<InferenceProcessPool> inferencePool_;
};

InferenceProcessPool::InferenceProcessPool(ControllerPool *pool, int workers, int timeoutMs)
    : pool_(pool), timeoutMs_(std::max(200, timeoutMs)) {
    int count = std::max(1, workers);
    running_ = true;
    workers_.reserve(count);
    for (int i = 0; i < count; i++) {
        workers_.emplace_back([this]() { workerLoop(); });
    }
}

InferenceProcessPool::~InferenceProcessPool() {
    stop();
}

void InferenceProcessPool::stop() {
    if (!running_) return;
    running_ = false;
    cv_.notify_all();
    for (auto &t : workers_) {
        if (t.joinable()) t.join();
    }
    workers_.clear();
}

json InferenceProcessPool::respond(const std::string &controllerName, const json &payload) {
    if (!running_) throw std::runtime_error("inference-pool-stopped");
    auto task = std::make_shared<Task>();
    task->controllerName = controllerName;
    task->payload = payload;
    task->promise = std::make_shared<std::promise<json>>();
    auto fut = task->promise->get_future();
    {
        std::lock_guard<std::mutex> lock(mu_);
        queue_.push(task);
    }
    cv_.notify_one();
    if (fut.wait_for(std::chrono::milliseconds(timeoutMs_)) == std::future_status::timeout) {
        throw std::runtime_error("inference-timeout");
    }
    return fut.get();
}

void InferenceProcessPool::workerLoop() {
    while (running_) {
        std::shared_ptr<Task> task;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [&]() { return !running_ || !queue_.empty(); });
            if (!running_ && queue_.empty()) return;
            task = queue_.front();
            queue_.pop();
        }
        try {
            if (!pool_) throw std::runtime_error("pool-null");
            auto ctrl = pool_->getByName(task->controllerName);
            if (!ctrl) throw std::runtime_error("controller-offline");
            auto out = ctrl->respond(task->payload);
            task->promise->set_value(out);
        } catch (const std::exception &e) {
            try {
                task->promise->set_exception(std::make_exception_ptr(std::runtime_error(e.what())));
            } catch (...) {}
        }
    }
}

class GroupProcessPool : public ControllerPoolBase {
public:
    GroupProcessPool(std::shared_ptr<KeyValueStore> kvmStore,
                     std::shared_ptr<KeyValueStore> memeStore,
                     std::shared_ptr<KeyValueStore> sessionStore,
                     const Config &config)
        : config_(config), kvmStore_(std::move(kvmStore)), memeStore_(std::move(memeStore)), sessionStore_(std::move(sessionStore)) {
        for (int i = 0; i < config_.groupCount; i++) {
            std::string gid = "G" + std::to_string(i + 1);
            groupIds_.push_back(gid);
            std::vector<std::string> names;
            for (int j = 0; j < config_.groupSize; j++) {
                std::string name = gid + "_AI" + std::to_string(j + 1);
                names.push_back(name);
                controllerToGroup_[name] = gid;
                controllers_[name] = std::make_shared<ProxyController>(this, name);
            }
            groups_[gid] = names;
        }
        auto g1 = groupIds_.empty() ? "G1" : groupIds_[0];
        auto list = groups_[g1];
        servingName_ = list.empty() ? "" : list[0];
        standbyName_ = list.size() > 1 ? list[1] : servingName_;
        validationName_ = list.size() > 2 ? list[2] : servingName_;
        spawnWorkers();
    }

    ~GroupProcessPool() override {
        shutdownWorkers();
    }

    std::shared_ptr<Controller> getActive() override { return getByName(servingName_); }
    std::shared_ptr<Controller> getStandby() override { return getByName(standbyName_); }
    std::shared_ptr<Controller> getValidation() override { return getByName(validationName_); }

    std::shared_ptr<Controller> getByName(const std::string &name) override {
        auto it = controllers_.find(name);
        return it == controllers_.end() ? nullptr : it->second;
    }

    std::vector<std::string> listControllerNames() const override {
        std::vector<std::string> out;
        for (auto &gid : groupIds_) {
            auto it = groups_.find(gid);
            if (it == groups_.end()) continue;
            for (auto &name : it->second) out.push_back(name);
        }
        return out;
    }

    std::vector<std::string> listGroupIds() const override { return groupIds_; }

    std::vector<std::string> listControllersInGroup(const std::string &gid) override {
        return groups_.count(gid) ? groups_.at(gid) : std::vector<std::string>{};
    }

    std::vector<json> listMetrics() const override {
        std::vector<json> out;
        for (auto &gid : groupIds_) {
            try {
                auto result = rpc(gid, json{{"cmd", "listMetrics"}});
                if (result.is_array()) {
                    for (auto &item : result) out.push_back(item);
                }
            } catch (...) {
                // ignore failed groups
            }
        }
        return out;
    }

    json ingestDocument(const json &doc) override {
        json results = json::array();
        for (auto &gid : groupIds_) {
            auto part = rpc(gid, json{{"cmd", "ingestDocument"}, {"doc", doc}});
            if (part.is_array()) {
                for (auto &item : part) results.push_back(item);
            } else {
                results.push_back(part);
            }
        }
        return results;
    }

    json ingestDocumentToGroup(const std::string &gid, const json &doc) override {
        if (!groups_.count(gid)) return json{{"ok", false}, {"reason", "group-not-found"}};
        return rpc(gid, json{{"cmd", "ingestDocumentToGroup"}, {"doc", doc}});
    }

    json forgetMemes(const json &payload) override {
        json results = json::array();
        for (auto &gid : groupIds_) {
            auto part = rpc(gid, json{{"cmd", "forgetMemes"}, {"criteria", payload}});
            if (part.is_array()) {
                for (auto &item : part) results.push_back(item);
            } else {
                results.push_back(part);
            }
        }
        return results;
    }

    json onlineResearch(const json &queryOrTokens, const json &options) override {
        auto ctrl = getActive();
        if (!ctrl) throw std::runtime_error("no-active-controller");
        return rpc(controllerToGroup_[ctrl->name()], json{{"cmd", "runtimeCall"}, {"controllerName", ctrl->name()}, {"method", "onlineLookup"}, {"args", json::array({queryOrTokens, options})}});
    }

    void hotSwap(const json &snapshot) override {
        for (auto &gid : groupIds_) {
            rpc(gid, json{{"cmd", "applySnapshotAll"}, {"snapshot", snapshot}});
        }
        std::swap(servingName_, standbyName_);
    }

    void applySnapshotAll(const json &snapshot) {
        for (auto &gid : groupIds_) {
            rpc(gid, json{{"cmd", "applySnapshotAll"}, {"snapshot", snapshot}});
        }
    }

    const Config &config() const override { return config_; }

    json respondByName(const std::string &controllerName, const json &payload) override {
        auto gid = controllerToGroup_.count(controllerName) ? controllerToGroup_[controllerName] : std::string();
        if (gid.empty()) throw std::runtime_error("group-not-found");
        return rpc(gid, json{{"cmd", "respond"}, {"controllerName", controllerName}, {"payload", payload}});
    }

    json rpc(const std::string &groupId, const json &payload) const {
        auto it = workers_.find(groupId);
        if (it == workers_.end()) throw std::runtime_error("group-worker-unavailable:" + groupId);
        auto &worker = *it->second;
        std::lock_guard<std::mutex> lock(worker.mu);
        json msg = payload;
        msg["id"] = std::to_string(worker.seq++);
        std::string line = msg.dump() + "\n";
        if (worker.sock == kInvalidSocket) {
            worker.sock = connectWorker(worker.port);
        }
        if (worker.sock == kInvalidSocket || !sendAll(worker.sock, line)) {
            throw std::runtime_error("group-worker-unavailable:" + groupId);
        }
        std::string replyLine;
        if (!recvLine(worker.sock, replyLine, config_.groupProcTimeoutMs)) {
            std::string cmd = payload.value("cmd", "");
            throw std::runtime_error("group-worker-timeout:" + groupId + ":" + cmd);
        }
        json reply = json::parse(replyLine, nullptr, false);
        if (reply.is_discarded()) throw std::runtime_error("group-worker-error:" + groupId);
        if (reply.value("ok", false)) return reply.value("result", json::object());
        std::string err = reply.value("error", "");
        if (err.empty()) err = "group-worker-error:" + groupId;
        throw std::runtime_error(err);
    }

    json runtimeCall(const std::string &controllerName, const std::string &method, const json &args) const {
        auto gid = controllerToGroup_.count(controllerName) ? controllerToGroup_.at(controllerName) : std::string();
        if (gid.empty()) throw std::runtime_error("group-not-found");
        return rpc(gid, json{{"cmd", "runtimeCall"}, {"controllerName", controllerName}, {"method", method}, {"args", args}});
    }

    json memebarrierStart(double threshold) const {
        auto gid = controllerToGroup_.count(servingName_) ? controllerToGroup_.at(servingName_) : std::string();
        if (gid.empty()) throw std::runtime_error("group-not-found");
        return rpc(gid, json{{"cmd", "memebarrierStart"}, {"maliciousThreshold", threshold}});
    }

    json memebarrierStop() const {
        auto gid = controllerToGroup_.count(servingName_) ? controllerToGroup_.at(servingName_) : std::string();
        if (gid.empty()) throw std::runtime_error("group-not-found");
        return rpc(gid, json{{"cmd", "memebarrierStop"}});
    }

    json memebarrierStats() const {
        auto gid = controllerToGroup_.count(servingName_) ? controllerToGroup_.at(servingName_) : std::string();
        if (gid.empty()) throw std::runtime_error("group-not-found");
        return rpc(gid, json{{"cmd", "memebarrierStats"}});
    }

    json memebarrierSetThreshold(double threshold) const {
        auto gid = controllerToGroup_.count(servingName_) ? controllerToGroup_.at(servingName_) : std::string();
        if (gid.empty()) throw std::runtime_error("group-not-found");
        return rpc(gid, json{{"cmd", "memebarrierSetThreshold"}, {"maliciousThreshold", threshold}});
    }

    std::string groupForController(const std::string &name) const {
        auto it = controllerToGroup_.find(name);
        return it == controllerToGroup_.end() ? std::string() : it->second;
    }

private:
    struct WorkerInfo {
        int port{0};
        mutable SocketHandle sock{kInvalidSocket};
        mutable std::mutex mu;
        mutable uint64_t seq{1};
#ifdef _WIN32
        PROCESS_INFORMATION proc{0};
#else
        pid_t pid{-1};
#endif
    };

    Config config_;
    std::shared_ptr<KeyValueStore> kvmStore_;
    std::shared_ptr<KeyValueStore> memeStore_;
    std::shared_ptr<KeyValueStore> sessionStore_;
    std::vector<std::string> groupIds_;
    std::unordered_map<std::string, std::vector<std::string>> groups_;
    std::unordered_map<std::string, std::string> controllerToGroup_;
    std::unordered_map<std::string, std::shared_ptr<Controller>> controllers_;
    mutable std::unordered_map<std::string, std::shared_ptr<WorkerInfo>> workers_;
    std::string servingName_;
    std::string standbyName_;
    std::string validationName_;

    void spawnWorkers() {
        int basePort = std::max(1, config_.portStudy + 100);
        for (size_t i = 0; i < groupIds_.size(); i++) {
            std::string gid = groupIds_[i];
            int port = basePort + (int)i;
            auto info = std::make_shared<WorkerInfo>();
            info->port = port;
            spawnWorkerProcess(gid, port, *info);
            info->sock = connectWorker(port);
            workers_[gid] = info;
        }
    }

    void shutdownWorkers() {
        for (auto &kv : workers_) {
            if (!kv.second) continue;
            auto &worker = *kv.second;
            closeSocket(worker.sock);
#ifdef _WIN32
            if (worker.proc.hProcess) {
                TerminateProcess(worker.proc.hProcess, 0);
                CloseHandle(worker.proc.hProcess);
                CloseHandle(worker.proc.hThread);
            }
#else
            if (worker.pid > 0) {
                kill(worker.pid, SIGTERM);
            }
#endif
        }
    }

    void spawnWorkerProcess(const std::string &groupId, int port, WorkerInfo &info) const {
        std::string portStr = std::to_string(port);
        std::string groupSize = std::to_string(config_.groupSize);
#ifdef _WIN32
        STARTUPINFOA si{};
        PROCESS_INFORMATION pi{};
        si.cb = sizeof(si);
        std::ostringstream cmd;
        cmd << '"' << g_selfPath << '"' << " --group-worker=1";
        std::string cmdLine = cmd.str();

        std::string envBlock;
        auto addEnv = [&](const std::string &k, const std::string &v) {
            envBlock += k + "=" + v + "\0";
        };
        addEnv("AI_GROUP_ID", groupId);
        addEnv("AI_GROUP_SIZE", groupSize);
        addEnv("AI_GROUP_COUNT", std::to_string(config_.groupCount));
        addEnv("AI_LMDB_ROOT", config_.lmdbRoot.string());
        addEnv("AI_LMDB_MAP_BYTES", std::to_string(config_.lmdbMapSizeBytes));
        addEnv("AI_BASE_DIR", config_.baseDir.string());
        addEnv("AI_GROUP_WORKER_PORT", portStr);
        envBlock.push_back('\0');

        BOOL ok = CreateProcessA(nullptr, cmdLine.data(), nullptr, nullptr, FALSE, 0, (LPVOID)envBlock.c_str(), nullptr, &si, &pi);
        if (!ok) {
            throw std::runtime_error("failed-to-spawn-group-worker");
        }
        info.proc = pi;
        CloseHandle(pi.hThread);
#else
        pid_t pid = fork();
        if (pid == 0) {
            setenv("AI_GROUP_ID", groupId.c_str(), 1);
            setenv("AI_GROUP_SIZE", groupSize.c_str(), 1);
            setenv("AI_GROUP_COUNT", std::to_string(config_.groupCount).c_str(), 1);
            setenv("AI_LMDB_ROOT", config_.lmdbRoot.string().c_str(), 1);
            setenv("AI_LMDB_MAP_BYTES", std::to_string(config_.lmdbMapSizeBytes).c_str(), 1);
            setenv("AI_BASE_DIR", config_.baseDir.string().c_str(), 1);
            setenv("AI_GROUP_WORKER_PORT", portStr.c_str(), 1);
            execl(g_selfPath.c_str(), g_selfPath.c_str(), "--group-worker=1", nullptr);
            _exit(1);
        }
        info.pid = pid;
#endif
    }

    SocketHandle connectWorker(int port) const {
#ifdef _WIN32
        static std::atomic<bool> wsaReady{false};
        if (!wsaReady) {
            WSADATA wsaData{};
            WSAStartup(MAKEWORD(2, 2), &wsaData);
            wsaReady = true;
        }
#endif
        SocketHandle sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock == kInvalidSocket) return kInvalidSocket;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        for (int i = 0; i < 50; i++) {
            if (connect(sock, (sockaddr *)&addr, sizeof(addr)) == 0) {
                return sock;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        closeSocket(sock);
        return kInvalidSocket;
    }
};

json ProxyRuntime::processInput(const json &payload) {
    return pool_->respondByName(controllerName_, payload);
}

json ProxyRuntime::cloneParams() const {
    return pool_->rpc(pool_->groupForController(controllerName_), json{{"cmd", "cloneParams"}, {"controllerName", controllerName_}});
}

void ProxyRuntime::setParams(const json &patch) {
    pool_->rpc(pool_->groupForController(controllerName_), json{{"cmd", "applyParams"}, {"controllerName", controllerName_}, {"params", patch}});
}

json ProxyRuntime::toSnapshot() {
    return pool_->rpc(pool_->groupForController(controllerName_), json{{"cmd", "snapshot"}, {"controllerName", controllerName_}});
}

void ProxyRuntime::fromSnapshot(const json &snapshot) {
    pool_->rpc(pool_->groupForController(controllerName_), json{{"cmd", "applySnapshot"}, {"controllerName", controllerName_}, {"snapshot", snapshot}});
}

json ProxyRuntime::ingestDocument(const json &doc) {
    return pool_->rpc(pool_->groupForController(controllerName_), json{{"cmd", "ingestDocumentTo"}, {"controllerName", controllerName_}, {"doc", doc}});
}

json ProxyRuntime::forgetMemes(const json &payload) {
    return pool_->forgetMemes(payload);
}

json ProxyRuntime::onlineLookup(const std::string &query, const json &options) {
    return pool_->runtimeCall(controllerName_, "onlineLookup", json::array({query, options}));
}

json ProxyRuntime::onlineLookup(const json &input, const json &options) {
    return pool_->runtimeCall(controllerName_, "onlineLookup", json::array({input, options}));
}

json ProxyRuntime::getSearchConfig() const {
    return pool_->runtimeCall(controllerName_, "getSearchConfig", json::array());
}

json ProxyRuntime::setSearchConfig(const json &patch) {
    return pool_->runtimeCall(controllerName_, "setSearchConfig", json::array({patch}));
}

std::vector<RobotsDocument> ProxyRuntime::collectRobotsDocuments(const json &options) const {
    auto out = pool_->runtimeCall(controllerName_, "collectRobotsDocuments", json::array({options}));
    std::vector<RobotsDocument> docs;
    if (out.is_array()) {
        for (auto &item : out) {
            RobotsDocument d;
            d.id = item.value("id", "");
            d.text = item.value("text", "");
            d.tokens = item.value("tokens", std::vector<std::string>{});
            d.source = item.value("source", "");
            d.file = item.value("file", "");
            docs.push_back(std::move(d));
        }
    }
    return docs;
}

std::vector<std::string> ProxyRuntime::listRobotsFiles() const {
    auto out = pool_->runtimeCall(controllerName_, "listRobotsFiles", json::array());
    std::vector<std::string> files;
    if (out.is_array()) {
        for (auto &v : out) if (v.is_string()) files.push_back(v.get<std::string>());
    }
    return files;
}

fs::path ProxyRuntime::exportGraphToFile(const json &options) {
    auto out = pool_->runtimeCall(controllerName_, "exportGraphToFile", json::array({options}));
    if (out.is_string()) return fs::path(out.get<std::string>());
    if (out.is_object() && out.contains("path")) return fs::path(out["path"].get<std::string>());
    return {};
}

json ProxyRuntime::learnFromDialog(const json &payload, const json &result) {
    return pool_->runtimeCall(controllerName_, "learnFromDialog", json::array({json{{"payload", payload}, {"result", result}}}));
}

std::unordered_map<std::string, double> ProxyRuntime::mapWordsToMemes(const std::vector<std::string> &words) {
    auto out = pool_->runtimeCall(controllerName_, "mapWordsToMemes", json::array({words}));
    std::unordered_map<std::string, double> res;
    if (out.is_object()) {
        for (auto it = out.begin(); it != out.end(); ++it) {
            if (it.value().is_number()) res[it.key()] = it.value().get<double>();
        }
    }
    return res;
}

std::vector<std::string> ProxyRuntime::getMemeWords(const std::string &memeId) {
    auto out = pool_->runtimeCall(controllerName_, "getMemeWords", json::array({memeId}));
    std::vector<std::string> res;
    if (out.is_array()) {
        for (auto &v : out) if (v.is_string()) res.push_back(v.get<std::string>());
    }
    return res;
}

void ProxyRuntime::graphLink(const std::string &fromId, const std::string &toId, double weight, int direction) {
    pool_->runtimeCall(controllerName_, "graphLink", json::array({fromId, toId, weight, direction}));
}

const Config &ProxyRuntime::config() const {
    return pool_->config();
}

json ProxyController::respond(const json &payload) {
    return pool_->respondByName(name_, payload);
}

json ProxyController::metrics() const {
    return pool_->rpc(pool_->groupForController(name_), json{{"cmd", "metrics"}, {"controllerName", name_}});
}

void ProxyController::applyParams(const json &p) {
    pool_->rpc(pool_->groupForController(name_), json{{"cmd", "applyParams"}, {"controllerName", name_}, {"params", p}});
}

json ProxyController::snapshot() const {
    return pool_->rpc(pool_->groupForController(name_), json{{"cmd", "snapshot"}, {"controllerName", name_}});
}

void ProxyController::applySnapshot(const json &snap) {
    pool_->rpc(pool_->groupForController(name_), json{{"cmd", "applySnapshot"}, {"controllerName", name_}, {"snapshot", snap}});
}

// ------------------ Spark Arrays ------------------

class PersonaForestAverager {
public:
    explicit PersonaForestAverager(const json &options = json::object()) {
        enabled_ = options.value("enabled", true);
        trees_ = std::max(8, options.value("trees", 32));
        featureSubspace_ = std::max(2, options.value("featureSubspace", 4));
        sampleRate_ = clamp01(options.value("sampleRate", 0.85));
        personaMomentum_ = clamp01(options.value("personaMomentum", 0.92));
        targetReplyLen_ = std::max(20, options.value("targetReplyLen", 220));
        maxHistory_ = std::max(10, options.value("maxHistory", 60));
    }

    int maxHistory() const { return maxHistory_; }

    std::optional<json> pick(const json &payload, const json &layerResults, const std::vector<float> &requestEmbedding, const std::vector<json> &history) {
        if (!enabled_) return std::nullopt;
        int dim = requestEmbedding.empty() ? 64 : (int)requestEmbedding.size();
        std::string inputText = payload.value("text", "");
        auto inputTokens = tokenize(inputText);

        std::vector<Candidate> candidates;
        if (layerResults.is_array()) {
            for (auto &layer : layerResults) {
                auto controllers = layer.value("controllers", json::array());
                for (auto &controller : controllers) {
                    auto base = controller.value("base", json::object());
                    if (!base.contains("reply")) continue;
                    std::string reply = base.value("reply", "");
                    if (reply.empty()) continue;
                    Candidate c;
                    c.layer = layer.value("layer", "");
                    c.controller = controller.value("controller", "");
                    c.reply = reply;
                    c.latency = base.value("latency", 0);
                    c.sessionId = base.value("sessionId", "");
                    c.emb = textToMiniEmbedding(reply, dim);
                    if (controller.contains("affinity") && controller["affinity"].is_number()) {
                        c.affinity = clamp11(controller["affinity"].get<double>());
                    } else {
                        c.affinity = clamp11(cosineSim(requestEmbedding, c.emb));
                    }
                    c.controllerRef = controller;
                    candidates.push_back(std::move(c));
                }
            }
        }

        if (candidates.empty()) return std::nullopt;
        if (candidates.size() == 1) {
            updatePersonas(candidates[0]);
            return json{{"layer", candidates[0].layer}, {"controller", candidates[0].controller}, {"affinity", candidates[0].affinity}, {"reply", candidates[0].reply}, {"latency", candidates[0].latency}, {"sessionId", candidates[0].sessionId}, {"score", 0}, {"method", "forest-single"}};
        }

        auto centroid = centroidEmbedding(candidates, dim);
        json lastAgg = history.empty() ? json::object() : history.back().value("aggregate", json::object());
        std::vector<float> lastReplyEmb;
        if (lastAgg.contains("reply")) lastReplyEmb = textToMiniEmbedding(lastAgg.value("reply", ""), dim);
        int lastReplyLen = lastAgg.contains("reply") ? (int)lastAgg.value("reply", "").size() : 0;
        int targetLen = (lastReplyLen > 20) ? lastReplyLen : targetReplyLen_;

        for (auto &c : candidates) {
            auto replyTokens = tokenize(c.reply);
            double overlap = tokenOverlapRatio(inputTokens, replyTokens);
            double consensus = centroid.empty() ? 0.0 : clamp11(cosineSim(c.emb, centroid));
            double persona = globalPersona_.empty() ? 0.0 : clamp11(cosineSim(c.emb, globalPersona_));
            auto it = controllerPersona_.find(c.controller);
            double controllerPersona = (it == controllerPersona_.end() || it->second.empty()) ? 0.0 : clamp11(cosineSim(c.emb, it->second));
            double novelty = lastReplyEmb.empty() ? 0.5 : clamp01(1 - ((clamp11(cosineSim(c.emb, lastReplyEmb)) + 1) / 2));
            double lenPenalty = -clamp01(std::abs((int)c.reply.size() - targetLen) / (double)std::max(40, targetLen));
            double stability = variantStability(c.controllerRef);
            double latencyNorm = c.latency > 0 ? clamp01(1 - (c.latency / 2000.0)) : 0.5;
            c.features = json{{"affinity", clamp01((c.affinity + 1) / 2)}, {"overlap", overlap}, {"consensus", clamp01((consensus + 1) / 2)}, {"persona", clamp01((persona + 1) / 2)}, {"controllerPersona", clamp01((controllerPersona + 1) / 2)}, {"novelty", novelty}, {"lenPenalty", clamp01(1 + lenPenalty)}, {"stability", stability}, {"latency", latencyNorm}};
        }

        auto featureNames = json::array({"affinity", "overlap", "consensus", "persona", "controllerPersona", "novelty", "lenPenalty", "stability", "latency"});
        std::string seedText = payload.value("sessionId", "") + "|" + std::to_string(hashStrSimple(inputText)) + "|" + std::to_string(candidates.size());
        Rng32 rng(hashStrSimple(seedText));

        std::unordered_map<int, double> scores;
        std::unordered_map<int, int> votes;
        for (size_t i = 0; i < candidates.size(); i++) { scores[(int)i] = 0; votes[(int)i] = 0; }

        auto pickSubspace = [&]() {
            std::unordered_set<int> chosen;
            int maxPick = std::min(featureSubspace_, (int)featureNames.size());
            while ((int)chosen.size() < maxPick) {
                int idx = (int)std::floor(rng.next() * featureNames.size());
                chosen.insert(idx);
            }
            std::vector<int> out(chosen.begin(), chosen.end());
            return out;
        };

        for (int t = 0; t < trees_; t++) {
            auto subspace = pickSubspace();
            std::unordered_map<std::string, double> weights;
            for (auto idx : subspace) {
                std::string f = featureNames[idx].get<std::string>();
                double w = (rng.next() * 2 - 1);
                if (f == "affinity" || f == "consensus" || f == "persona") w *= 1.4;
                if (f == "controllerPersona" || f == "stability") w *= 1.1;
                if (f == "novelty") w *= 0.6;
                weights[f] = w;
            }

            std::vector<int> bag;
            for (size_t i = 0; i < candidates.size(); i++) {
                if (rng.next() <= sampleRate_) bag.push_back((int)i);
            }
            if (bag.size() < 2) {
                bag.clear();
                for (size_t i = 0; i < candidates.size(); i++) bag.push_back((int)i);
            }

            int bestIdx = -1;
            double bestScore = -1e9;
            for (auto idx : bag) {
                double s = 0.0;
                for (auto &kv : weights) {
                    double v = candidates[idx].features.value(kv.first, 0.0);
                    s += kv.second * v;
                }
                if (s > bestScore) { bestScore = s; bestIdx = idx; }
            }
            if (bestIdx >= 0) votes[bestIdx] += 1;
            for (size_t i = 0; i < candidates.size(); i++) {
                double s = 0.0;
                for (auto &kv : weights) {
                    double v = candidates[i].features.value(kv.first, 0.0);
                    s += kv.second * v;
                }
                scores[(int)i] += s;
            }
        }

        int winner = -1;
        for (size_t i = 0; i < candidates.size(); i++) {
            if (winner == -1) { winner = (int)i; continue; }
            int vi = votes[(int)i];
            int vw = votes[winner];
            double si = scores[(int)i];
            double sw = scores[winner];
            double ci = candidates[i].features.value("consensus", 0.0);
            double cw = candidates[winner].features.value("consensus", 0.0);
            double ai = candidates[i].features.value("affinity", 0.0);
            double aw = candidates[winner].features.value("affinity", 0.0);
            if (vi > vw || (vi == vw && (si > sw || (si == sw && (ci > cw || (ci == cw && ai > aw)))))) {
                winner = (int)i;
            }
        }

        if (winner < 0) return std::nullopt;
        updatePersonas(candidates[winner]);
        return json{{"layer", candidates[winner].layer}, {"controller", candidates[winner].controller}, {"affinity", candidates[winner].affinity}, {"reply", candidates[winner].reply}, {"latency", candidates[winner].latency}, {"sessionId", candidates[winner].sessionId}, {"score", scores[winner] / std::max(1, trees_)}, {"votes", votes[winner]}, {"method", "persona-forest"}};
    }

private:
    struct Candidate {
        std::string layer;
        std::string controller;
        std::string reply;
        int latency{0};
        std::string sessionId;
        double affinity{0};
        std::vector<float> emb;
        json controllerRef;
        json features;
    };
    bool enabled_{true};
    int trees_{32};
    int featureSubspace_{4};
    double sampleRate_{0.85};
    double personaMomentum_{0.92};
    int targetReplyLen_{220};
    int maxHistory_{60};
    std::vector<float> globalPersona_;
    std::unordered_map<std::string, std::vector<float>> controllerPersona_;

    static std::vector<float> centroidEmbedding(const std::vector<Candidate> &cands, int dim) {
        if (cands.empty()) return {};
        std::vector<float> out(dim, 0.0f);
        int count = 0;
        for (auto &c : cands) {
            if ((int)c.emb.size() != dim) continue;
            for (int i = 0; i < dim; i++) out[i] += c.emb[i];
            count++;
        }
        if (!count) return {};
        for (int i = 0; i < dim; i++) out[i] /= count;
        return out;
    }

    static double tokenOverlapRatio(const std::vector<std::string> &a, const std::vector<std::string> &b) {
        if (a.empty() || b.empty()) return 0.0;
        std::unordered_set<std::string> setB(b.begin(), b.end());
        double hit = 0.0;
        for (auto &t : a) if (setB.count(t)) hit += 1.0;
        return hit / std::max(1.0, (double)a.size());
    }

    double variantStability(const json &controllerRef) const {
        auto variants = controllerRef.value("variants", json::array());
        if (!variants.is_array() || variants.empty()) return 0.5;
        double sum = 0.0; int n = 0;
        for (auto &v : variants) {
            if (!v.is_object() || !v.contains("affinity")) continue;
            if (!v["affinity"].is_number()) continue;
            double a = v["affinity"].get<double>();
            if (!std::isfinite(a)) continue;
            sum += clamp11(a);
            n++;
        }
        if (!n) return 0.5;
        return clamp01((sum / n + 1) / 2);
    }

    void updatePersonas(const Candidate &winner) {
        if (winner.emb.empty()) return;
        auto blend = [&](const std::vector<float> &prev, const std::vector<float> &next) {
            if (next.empty()) return prev;
            if (prev.empty()) return next;
            std::vector<float> out(prev.size(), 0.0f);
            for (size_t i = 0; i < prev.size(); i++) {
                double v = personaMomentum_ * prev[i] + (1 - personaMomentum_) * next[i];
                out[i] = std::isfinite(v) ? (float)v : 0.0f;
            }
            return out;
        };
        globalPersona_ = blend(globalPersona_, winner.emb);
        auto it = controllerPersona_.find(winner.controller);
        if (it == controllerPersona_.end()) controllerPersona_[winner.controller] = blend({}, winner.emb);
        else it->second = blend(it->second, winner.emb);

        if (controllerPersona_.size() > 64) {
            std::vector<std::string> keys;
            keys.reserve(controllerPersona_.size());
            for (auto &kv : controllerPersona_) keys.push_back(kv.first);
            while (controllerPersona_.size() > 64) {
                size_t idx = (size_t)(std::rand() % keys.size());
                controllerPersona_.erase(keys[idx]);
            }
        }
    }
};

class SparkArray {
public:
    SparkArray(std::shared_ptr<ControllerPoolBase> pool, std::shared_ptr<ShardManager> shards, const json &options = json::object())
        : pool_(std::move(pool)), shards_(std::move(shards)), personaForest_(options.value("personaForest", json::object())) {
        groupId_ = options.value("groupId", std::string("G1"));
        auto available = pool_->listControllersInGroup(groupId_);
        auto wantedRaw = options.contains("numAI") ? options["numAI"].get<int>() : (options.contains("groupSize") ? options["groupSize"].get<int>() : 7);
        int wanted = std::max(1, wantedRaw);
        int numAI = std::max(1, std::min((int)available.size() ? (int)available.size() : wanted, wanted));
        layers_ = json::array();
        for (int i = 0; i < numAI; i++) {
            if (i >= (int)available.size()) break;
            layers_.push_back(json{{"name", groupId_ + ":a" + std::to_string(i + 1)}, {"controllers", json::array({available[i]})}, {"strategy", "max"}});
        }
        if (options.contains("layers") && options["layers"].is_array() && !options["layers"].empty()) {
            layers_ = json::array();
            for (auto &layer : options["layers"]) {
                json next = layer;
                if (!next.contains("strategy")) next["strategy"] = "max";
                layers_.push_back(next);
            }
        }
        defaultBudget_ = normalizeBudget(options.value("budget", json()));
    }

    json getLayers() const { return layers_; }

    void updateLayers(const json &layers) {
        if (!layers.is_array() || layers.empty()) throw std::runtime_error("layers must be a non-empty array");
        for (auto &layer : layers) {
            if (!layer.contains("name") || !layer.contains("controllers") || !layer["controllers"].is_array()) {
                throw std::runtime_error("invalid layer specification");
            }
        }
        layers_ = json::array();
        for (auto &layer : layers) {
            json next = layer;
            if (!next.contains("strategy")) next["strategy"] = "max";
            layers_.push_back(next);
        }
    }

    json dispatch(const json &payload, const json &options = json::object()) {
        std::string text = payload.value("text", "");
        auto requestEmbedding = textToMiniEmbedding(text, 64);
        auto variants = buildVariants(text, options.value("perturbations", 0));
        json layers = options.value("multiLayer", true) ? layers_ : json::array({layers_.empty() ? json::object() : layers_[0]});
        if (options.contains("numAI") && options["numAI"].is_number()) {
            int cap = std::max(1, options["numAI"].get<int>());
            json trimmed = json::array();
            for (int i = 0; i < (int)layers.size() && i < cap; i++) trimmed.push_back(layers[i]);
            layers = trimmed;
        }
        auto budget = mergeBudgets(defaultBudget_, normalizeBudget(options.value("budget", payload.value("budget", json()))));

        json layerResults = json::array();

        auto callRespond = [&](const std::string &controllerName, const json &nextPayload) {
            return pool_->respondByName(controllerName, nextPayload);
        };

        for (auto &layer : layers) {
            json controllers = json::array();
            for (auto &controllerSpec : layer.value("controllers", json::array())) {
                std::string controllerName = controllerSpec.is_string() ? controllerSpec.get<std::string>() : controllerSpec.value("name", controllerSpec.value("controller", ""));
                if (controllerName.empty()) {
                    controllers.push_back(json{{"controller", ""}, {"error", "invalid-controller"}});
                    continue;
                }
                int weight = 1;
                if (controllerSpec.is_object()) {
                    auto w = controllerSpec.value("weight", controllerSpec.value("w", 1));
                    weight = std::max(1, std::min(8, w));
                }
                std::string weightedText = weight <= 1 ? text : std::string();
                if (weight > 1) {
                    for (int i = 0; i < weight; i++) {
                        if (i) weightedText += " ";
                        weightedText += text;
                    }
                }

                json nextPayload = payload;
                nextPayload["text"] = weightedText;
                if (budget) nextPayload["budget"] = *budget;

                json baseResult;
                try {
                    baseResult = callRespond(controllerName, nextPayload);
                } catch (const std::exception &e) {
                    controllers.push_back(json{{"controller", controllerName}, {"error", e.what()}});
                    continue;
                }
                double affinity = cosineSim(requestEmbedding, textToMiniEmbedding(baseResult.value("reply", ""), 64));
                shards_->record(controllerName, requestEmbedding, baseResult.value("reply", ""), baseResult.value("latency", 0));
                json variantResults = json::array();
                for (auto &variant : variants) {
                    json vrPayload = payload;
                    vrPayload["text"] = variant;
                    if (budget) vrPayload["budget"] = *budget;
                    try {
                        auto vr = callRespond(controllerName, vrPayload);
                        double vAffinity = cosineSim(textToMiniEmbedding(variant, 64), textToMiniEmbedding(vr.value("reply", ""), 64));
                        variantResults.push_back(json{{"text", variant}, {"response", vr}, {"affinity", vAffinity}});
                    } catch (const std::exception &e) {
                        variantResults.push_back(json{{"text", variant}, {"error", e.what()}});
                    }
                }
                controllers.push_back(json{{"controller", controllerName}, {"base", baseResult}, {"affinity", affinity}, {"variants", variantResults}});
            }
            layerResults.push_back(json{{"layer", layer.value("name", "")}, {"controllers", controllers}});
        }

        auto aggregate = aggregateResults(layerResults, payload, requestEmbedding);
        recordHistory(payload, aggregate, layerResults);
        return json{{"aggregate", aggregate}, {"layers", layerResults}};
    }

    json dispatchBig(const json &payload, const json &options) {
        int rounds = std::max(1, std::min(10, options.value("bigRounds", options.value("bigSparkRounds", 1))));
        json history = json::array();
        json current = payload;
        std::string originalText = payload.value("text", "");
        for (int i = 0; i < rounds; i++) {
            auto r = dispatch(current, options);
            history.push_back(r);
            std::string reply = r.value("aggregate", json::object()).value("reply", "");
            if (reply.empty()) break;
            std::string combined = originalText + "\n" + reply;
            if (combined.size() > 4000) combined = combined.substr(combined.size() - 4000);
            current["text"] = combined;
        }
        json last = history.empty() ? json{{"aggregate", nullptr}, {"layers", json::array()}} : history.back();
        last["rounds"] = history;
        return last;
    }

    const std::vector<json> &history() const { return history_; }

private:
    std::shared_ptr<ControllerPoolBase> pool_;
    std::shared_ptr<ShardManager> shards_;
    std::string groupId_{"G1"};
    json layers_ = json::array();
    PersonaForestAverager personaForest_;
    std::optional<json> defaultBudget_;
    std::vector<json> history_;

    json aggregateResults(const json &layerResults, const json &payload, const std::vector<float> &requestEmbedding) {
        json best;
        double bestAffinity = -1e9;
        for (auto &layer : layerResults) {
            for (auto &controller : layer.value("controllers", json::array())) {
                if (!controller.contains("base")) continue;
                auto base = controller["base"];
                if (!base.contains("reply")) continue;
                double affinity = controller.value("affinity", 0.0);
                if (!best.is_object() || affinity > bestAffinity) {
                    bestAffinity = affinity;
                    best = json{{"layer", layer.value("layer", "")}, {"controller", controller.value("controller", "")}, {"affinity", affinity}, {"reply", base.value("reply", "")}, {"latency", base.value("latency", 0)}, {"sessionId", base.value("sessionId", "")}, {"method", "max-affinity"}};
                }
            }
        }
        auto picked = personaForest_.pick(payload, layerResults, requestEmbedding, history_);
        if (picked) return *picked;
        return best;
    }

    void recordHistory(const json &payload, const json &aggregate, const json &layerResults) {
        history_.push_back(json{{"ts", (int64_t)std::chrono::system_clock::now().time_since_epoch().count()}, {"request", json{{"text", payload.value("text", "")}, {"sessionId", payload.value("sessionId", "")}}}, {"aggregate", aggregate}, {"layers", layerResults}});
        if (history_.size() > 100) history_.erase(history_.begin());
    }
};

class BigSparkArray {
public:
    BigSparkArray(std::shared_ptr<ControllerPoolBase> pool, std::shared_ptr<ShardManager> shards, const json &options = json::object())
        : pool_(std::move(pool)), shards_(std::move(shards)), personaForest_(options.value("personaForest", json::object())) {
        auto groupIds = options.value("groupIds", json::array());
        if (!groupIds.is_array() || groupIds.empty()) {
            groupIds = json::array();
            for (auto &gid : pool_->listGroupIds()) groupIds.push_back(gid);
        }
        for (auto &gid : groupIds) {
            std::string groupId = gid.get<std::string>();
            int weight = 1;
            if (options.contains("groupWeights") && options["groupWeights"].contains(groupId)) {
                weight = std::max(1, std::min(8, (int)std::round(options["groupWeights"][groupId].get<double>())));
            }
            json groupOptions = options.value("groupOptions", json::object());
            groupOptions["groupId"] = groupId;
            groups_.push_back(GroupEntry{groupId, weight, std::make_shared<SparkArray>(pool_, shards_, groupOptions)});
        }
    }

    json dispatch(const json &payload, const json &options) {
        auto requestEmbedding = textToMiniEmbedding(payload.value("text", ""), 64);
        json groupResults = json::array();
        for (auto &g : groups_) {
            std::string gid = g.groupId;
            int w = std::max(1, g.weight);
            json groupPayload = payload;
            if (w > 1) {
                std::string t = payload.value("text", "");
                std::string repeated;
                for (int i = 0; i < w; i++) { if (i) repeated += " "; repeated += t; }
                groupPayload["text"] = repeated;
            }
            auto result = g.spark->dispatch(groupPayload, options);
            groupResults.push_back(json{{"groupId", gid}, {"result", result}});
        }

        json layerResults = json::array({json{{"layer", "groups"}, {"controllers", json::array()}}});
        for (auto &gr : groupResults) {
            auto agg = gr.value("result", json::object()).value("aggregate", json::object());
            layerResults[0]["controllers"].push_back(json{{"controller", gr.value("groupId", "")}, {"base", agg}, {"affinity", agg.value("affinity", 0.0)}, {"variants", json::array()}});
        }

        json aggregate;
        auto picked = personaForest_.pick(payload, layerResults, requestEmbedding, history_);
        if (picked) {
            aggregate = *picked;
        } else {
            double bestAffinity = -1e9;
            for (auto &gr : groupResults) {
                auto agg = gr.value("result", json::object()).value("aggregate", json::object());
                if (!agg.contains("reply")) continue;
                double a = agg.value("affinity", 0.0);
                if (aggregate.is_null() || a > bestAffinity) {
                    bestAffinity = a;
                    aggregate = agg;
                    aggregate["controller"] = gr.value("groupId", "");
                    aggregate["layer"] = "groups";
                }
            }
        }

        json out{{"aggregate", aggregate}, {"groups", groupResults}};
        history_.push_back(json{{"ts", (int64_t)std::chrono::system_clock::now().time_since_epoch().count()}, {"request", json{{"text", payload.value("text", "")}, {"sessionId", payload.value("sessionId", "")}}}, {"aggregate", aggregate}, {"groups", groupResults}});
        if (history_.size() > 100) history_.erase(history_.begin());
        return out;
    }

    json dispatchBig(const json &payload, const json &options) {
        int rounds = std::max(1, std::min(10, options.value("bigRounds", options.value("bigSparkRounds", 1))));
        json history = json::array();
        json current = payload;
        std::string originalText = payload.value("text", "");
        for (int i = 0; i < rounds; i++) {
            auto r = dispatch(current, options);
            history.push_back(r);
            std::string reply = r.value("aggregate", json::object()).value("reply", "");
            if (reply.empty()) break;
            std::string combined = originalText + "\n" + reply;
            if (combined.size() > 4000) combined = combined.substr(combined.size() - 4000);
            current["text"] = combined;
        }
        json last = history.empty() ? json{{"aggregate", nullptr}, {"groups", json::array()}} : history.back();
        last["rounds"] = history;
        return last;
    }

    json getLayers() const {
        json out = json::array();
        for (auto &g : groups_) {
            out.push_back(json{{"groupId", g.groupId}, {"weight", g.weight}, {"layers", g.spark->getLayers()}});
        }
        return out;
    }

    void updateLayers(const json &patch) {
        if (patch.is_array()) {
            if (!groups_.empty()) groups_[0].spark->updateLayers(patch);
            return;
        }
        std::string groupId = patch.value("groupId", "");
        auto layers = patch.value("layers", json::array());
        if (!groupId.empty() && layers.is_array()) {
            for (auto &g : groups_) {
                if (g.groupId == groupId) {
                    g.spark->updateLayers(layers);
                }
            }
        }
    }

    const std::vector<json> &history() const { return history_; }

private:
    struct GroupEntry {
        std::string groupId;
        int weight{1};
        std::shared_ptr<SparkArray> spark;
    };
    std::shared_ptr<ControllerPoolBase> pool_;
    std::shared_ptr<ShardManager> shards_;
    PersonaForestAverager personaForest_;
    std::vector<GroupEntry> groups_;
    std::vector<json> history_;
};

// ------------------ Learning ------------------

class ReinforcementLearner {
public:
    ReinforcementLearner(std::shared_ptr<ControllerPoolBase> pool, fs::path testsDir)
        : pool_(std::move(pool)), testsDir_(std::move(testsDir)) {}

    json learn(int cycles) {
        (void)cycles;
        auto files = listTestFiles();
        std::vector<json> evals;
        for (auto &file : files) {
            evals.push_back(evaluateOnce(file));
        }
        std::vector<std::vector<float>> features;
        for (auto &e : evals) {
            features.push_back(buildFeatureForDoc(e.value("reply", "")));
        }
        auto coords = reduceFeatures(features);
        auto clusters = cluster(coords, std::min(4, std::max(2, (int)std::sqrt((double)files.size()))));
        std::unordered_map<int, std::vector<json>> grouped;
        for (size_t i = 0; i < evals.size(); i++) grouped[clusters[i]].push_back(evals[i]);
        json clusterStats = json::array();
        for (auto &kv : grouped) {
            double sum = 0; for (auto &e : kv.second) sum += e.value("score", 0.0);
            clusterStats.push_back(json{{"gid", kv.first}, {"avg", kv.second.empty() ? 0.0 : sum / kv.second.size()}, {"n", (int)kv.second.size()}});
        }
        double baseAvg = 0; for (auto &e : evals) baseAvg += e.value("score", 0.0);
        baseAvg = evals.empty() ? 0.0 : baseAvg / evals.size();
        double learnedAvg = 0; for (auto &c : clusterStats) learnedAvg += c.value("avg", 0.0);
        learnedAvg = clusterStats.empty() ? 0.0 : learnedAvg / clusterStats.size();
        json evalStats{{"baseAvg", baseAvg}, {"learnedAvg", learnedAvg}, {"clusters", clusterStats}};
        auto params = adjustParams(evalStats);
        latest_ = json{{"ok", true}, {"evalStats", evalStats}, {"params", params}, {"evals", evals}, {"ts", (int64_t)std::chrono::system_clock::now().time_since_epoch().count()}};
        history_.push_back(latest_);
        return latest_;
    }

    json latest() const { return history_.empty() ? json() : history_.back(); }

    json refreshTests(const fs::path &testsDir) {
        testsDir_ = testsDir;
        return json{{"ok", true}, {"testsDir", testsDir_.string()}};
    }

    json setTestsDir(const fs::path &testsDir) { return refreshTests(testsDir); }

private:
    std::shared_ptr<ControllerPoolBase> pool_;
    fs::path testsDir_;
    int maxDocs_{64};
    int topKWords_{30};
    double improvementThreshold_{0.01};
    int iterations_{5};
    bool useUmap_{true};
    std::vector<json> history_;
    json latest_ = json::object();

    std::vector<std::string> listTestFiles() const {
        std::vector<std::string> files;
        if (!fs::exists(testsDir_)) return files;
        for (auto &e : fs::directory_iterator(testsDir_)) {
            if (!e.is_regular_file()) continue;
            auto ext = e.path().extension().string();
            std::string lower;
            for (char c : ext) lower.push_back((char)std::tolower((unsigned char)c));
            if (lower == ".txt") files.push_back(e.path().filename().string());
        }
        std::sort(files.begin(), files.end());
        if ((int)files.size() > maxDocs_) files.resize(maxDocs_);
        return files;
    }

    std::string readText(const std::string &file) const {
        return readTextFile(testsDir_ / file);
    }

    double scoreReplyQuality(const std::string &reply, const std::unordered_map<std::string, double> &seeds) const {
        auto words = tokenize(reply);
        if (words.empty()) return 0.0;
        double coverage = 0.0;
        int count = 0;
        for (auto &kv : seeds) {
            auto linked = pool_->getActive()->runtime()->getMemeWords(kv.first);
            if (linked.empty()) continue;
            std::unordered_set<std::string> linkedSet(linked.begin(), linked.end());
            int hit = 0;
            for (auto &w : words) if (linkedSet.count(w)) hit++;
            coverage += (double)hit / std::max(1.0, (double)linkedSet.size());
            count++;
        }
        coverage = count ? coverage / count : 0.0;
        double uniq = (double)std::unordered_set<std::string>(words.begin(), words.end()).size() / std::max(1.0, (double)words.size());
        return 0.7 * coverage + 0.3 * uniq;
    }

    std::vector<float> buildFeatureForDoc(const std::string &text) const {
        return textToMiniEmbedding(text, 128);
    }

    std::vector<std::array<double,2>> reduceFeatures(const std::vector<std::vector<float>> &features) const {
        if (features.empty()) return {};
        int nRows = (int)features.size();
        int nCols = (int)features[0].size();
        std::vector<double> mean(nCols, 0.0);
        for (auto &row : features) for (int j = 0; j < nCols; j++) mean[j] += row[j];
        for (int j = 0; j < nCols; j++) mean[j] /= std::max(1, nRows);
        std::vector<double> centered(nRows * nCols, 0.0);
        for (int i = 0; i < nRows; i++) for (int j = 0; j < nCols; j++) centered[i * nCols + j] = features[i][j] - mean[j];
        std::vector<std::vector<double>> cov(nCols, std::vector<double>(nCols, 0.0));
        for (int i = 0; i < nRows; i++) {
            for (int a = 0; a < nCols; a++) {
                for (int b = a; b < nCols; b++) {
                    double v = centered[i * nCols + a] * centered[i * nCols + b];
                    cov[a][b] += v;
                    if (a != b) cov[b][a] += v;
                }
            }
        }
        auto powerIter = [&](std::vector<double> vec) {
            std::vector<double> next(nCols, 0.0);
            for (int i = 0; i < nCols; i++) {
                double acc = 0.0;
                for (int j = 0; j < nCols; j++) acc += cov[i][j] * vec[j];
                next[i] = acc;
            }
            double norm = 0.0; for (double v : next) norm += v * v;
            norm = std::sqrt(norm) ? std::sqrt(norm) : 1.0;
            for (double &v : next) v /= norm;
            return next;
        };
        std::vector<double> v1(nCols, 0.0), v2(nCols, 0.0);
        for (int j = 0; j < nCols; j++) v1[j] = ((double)std::rand() / RAND_MAX) - 0.5;
        for (int i = 0; i < 8; i++) v1 = powerIter(v1);
        for (int j = 0; j < nCols; j++) v2[j] = ((double)std::rand() / RAND_MAX) - 0.5;
        for (int i = 0; i < 8; i++) {
            double proj = 0; for (int j = 0; j < nCols; j++) proj += v2[j] * v1[j];
            for (int j = 0; j < nCols; j++) v2[j] -= proj * v1[j];
            v2 = powerIter(v2);
        }
        std::vector<std::array<double,2>> coords;
        coords.reserve(nRows);
        for (int i = 0; i < nRows; i++) {
            double x = 0, y = 0;
            for (int j = 0; j < nCols; j++) {
                double v = centered[i * nCols + j];
                x += v * v1[j];
                y += v * v2[j];
            }
            coords.push_back({x, y});
        }
        return coords;
    }

    std::vector<int> cluster(const std::vector<std::array<double,2>> &coords, int k) const {
        auto out = clusterKMeans(coords, k);
        if (!out.empty()) return out;
        if (coords.empty()) return {};
        double minX = coords[0][0], maxX = coords[0][0];
        for (auto &c : coords) { minX = std::min(minX, c[0]); maxX = std::max(maxX, c[0]); }
        double step = (maxX - minX) / std::max(1, k);
        std::vector<int> fallback;
        fallback.reserve(coords.size());
        for (auto &c : coords) {
            int idx = (step <= 1e-6) ? 0 : (int)std::floor((c[0] - minX) / step);
            if (idx < 0) idx = 0; if (idx >= k) idx = k - 1;
            fallback.push_back(idx);
        }
        return fallback;
    }

    std::vector<int> clusterKMeans(const std::vector<std::array<double,2>> &coords, int k) const {
        if (coords.empty() || k <= 0) return {};
        int n = (int)coords.size();
        k = std::min(k, n);
        std::mt19937 rng(0xC0FFEEu);
        std::uniform_int_distribution<int> pick(0, n - 1);

        std::vector<std::array<double,2>> centroids;
        centroids.reserve(k);
        centroids.push_back(coords[pick(rng)]);
        for (int c = 1; c < k; c++) {
            std::vector<double> dist(n, 0.0);
            double total = 0.0;
            for (int i = 0; i < n; i++) {
                double best = std::numeric_limits<double>::max();
                for (auto &ct : centroids) {
                    double dx = coords[i][0] - ct[0];
                    double dy = coords[i][1] - ct[1];
                    double d = dx * dx + dy * dy;
                    if (d < best) best = d;
                }
                dist[i] = best;
                total += best;
            }
            if (total <= 1e-9) { centroids.push_back(coords[pick(rng)]); continue; }
            std::uniform_real_distribution<double> ur(0.0, total);
            double r = ur(rng);
            double acc = 0.0;
            int idx = 0;
            for (int i = 0; i < n; i++) { acc += dist[i]; if (acc >= r) { idx = i; break; } }
            centroids.push_back(coords[idx]);
        }

        std::vector<int> assign(n, 0);
        for (int iter = 0; iter < 25; iter++) {
            bool changed = false;
            for (int i = 0; i < n; i++) {
                int bestK = 0;
                double best = std::numeric_limits<double>::max();
                for (int c = 0; c < k; c++) {
                    double dx = coords[i][0] - centroids[c][0];
                    double dy = coords[i][1] - centroids[c][1];
                    double d = dx * dx + dy * dy;
                    if (d < best) { best = d; bestK = c; }
                }
                if (assign[i] != bestK) { assign[i] = bestK; changed = true; }
            }
            std::vector<std::array<double,2>> next(k, {0.0, 0.0});
            std::vector<int> counts(k, 0);
            for (int i = 0; i < n; i++) {
                next[assign[i]][0] += coords[i][0];
                next[assign[i]][1] += coords[i][1];
                counts[assign[i]]++;
            }
            for (int c = 0; c < k; c++) {
                if (counts[c] == 0) {
                    next[c] = coords[pick(rng)];
                } else {
                    next[c][0] /= counts[c];
                    next[c][1] /= counts[c];
                }
            }
            centroids.swap(next);
            if (!changed) break;
        }
        return assign;
    }

    json adjustParams(const json &evalStats) {
        auto active = pool_->getActive();
        auto params = active->runtime()->cloneParams();
        double baseScore = evalStats.value("baseAvg", 0.0);
        double learnedScore = evalStats.value("learnedAvg", 0.0);
        if (learnedScore - baseScore >= improvementThreshold_) {
            params["iteration"] = std::min(12, params.value("iteration", 5) + 1);
            params["decayK"] = std::max(0.5, params.value("decayK", 1.0) - 0.05);
        } else {
            params["iteration"] = std::max(3, params.value("iteration", 5) - 1);
            params["decayK"] = std::min(2.0, params.value("decayK", 1.0) + 0.05);
        }
        active->applyParams(params);
        return params;
    }

    json evaluateOnce(const std::string &file) {
        std::string text = readText(file);
        auto ctrl = pool_->getActive();
        auto response = ctrl->respond(json{{"text", text}});
        std::unordered_map<std::string, double> seeds;
        for (auto &s : response.value("seeds", json::array())) {
            if (s.is_array() && s.size() >= 2) seeds[s[0].get<std::string>()] = s[1].get<double>();
        }
        double score = scoreReplyQuality(response.value("reply", ""), seeds);
        return json{{"file", file}, {"score", score}, {"words", tokenize(text)}, {"reply", response.value("reply", "")}};
    }
};

class AdversarialLearner {
public:
    explicit AdversarialLearner(std::shared_ptr<ControllerPoolBase> pool)
        : pool_(std::move(pool)), rng_(0x12345678u) {}

    json attackAndDefend(const json &samples) {
        auto bench = samples.is_array() ? samples : json::array();
        if ((int)bench.size() > benchLimit_) bench.erase(bench.begin() + benchLimit_, bench.end());
        json rounds = json::array();
        for (int r = 0; r < attackRounds_; r++) {
            json results = json::array();
            for (auto &s : bench) {
                auto advs = generateAdversaries(s.get<std::string>());
                for (auto &a : advs) {
                    results.push_back(probe(a));
                }
            }
            rounds.push_back(json{{"phase", "attack"}, {"results", results}});
        }
        json fixes = json::array();
        for (int d = 0; d < defenseRounds_; d++) {
            for (auto &s : bench) {
                auto res = probe(s.get<std::string>());
                if (res.value("quality", 0.0) < 0.25) {
                    auto words = tokenize(res.value("reply", ""));
                    auto seeds = pool_->getActive()->runtime()->mapWordsToMemes(words);
                    std::vector<std::string> ids;
                    for (auto &kv : seeds) ids.push_back(kv.first);
                    for (size_t i = 0; i + 1 < ids.size(); i++) {
                        pool_->getActive()->runtime()->graphLink(ids[i], ids[i + 1], 0.5, 0);
                    }
                    fixes.push_back(json{{"sample", s}, {"addedEdges", (int)std::max<int>(0, (int)ids.size() - 1)}});
                }
            }
            rounds.push_back(json{{"phase", "defense"}, {"fixes", fixes}});
        }
        auto summary = summarize(rounds);
        latest_ = json{{"ok", true}, {"summary", summary}, {"rounds", rounds}};
        history_.push_back(latest_);
        return latest_;
    }

    json latest() const { return history_.empty() ? json() : history_.back(); }

private:
    std::shared_ptr<ControllerPoolBase> pool_;
    int maxAdversaries_{64};
    double noiseLevel_{0.2};
    int attackRounds_{3};
    int defenseRounds_{3};
    int benchLimit_{50};
    Rng32 rng_;
    std::vector<json> history_;
    json latest_ = json::object();

    std::vector<std::string> perturbTokens(const std::vector<std::string> &tokens) {
        std::vector<std::string> out;
        for (auto &t : tokens) {
            double r = rng_.next();
            if (r < noiseLevel_ / 2) continue;
            out.push_back(t);
        }
        if (rng_.next() < noiseLevel_) out.push_back("noise_token_" + std::to_string((int)(rng_.next() * 1000)));
        return out;
    }

    std::vector<std::string> generateAdversaries(const std::string &text) {
        auto base = tokenize(text);
        std::vector<std::string> adversaries;
        int times = std::min(8, std::max(2, (int)(base.size() / 3)));
        for (int i = 0; i < times; i++) {
            auto mut = perturbTokens(base);
            std::ostringstream oss;
            for (size_t j = 0; j < mut.size(); j++) { if (j) oss << " "; oss << mut[j]; }
            adversaries.push_back(oss.str());
        }
        return adversaries;
    }

    json probe(const std::string &text) {
        auto ctrl = pool_->getActive();
        auto result = ctrl->respond(json{{"text", text}});
        std::unordered_map<std::string, double> seeds;
        for (auto &s : result.value("seeds", json::array())) {
            if (s.is_array() && s.size() >= 2) seeds[s[0].get<std::string>()] = s[1].get<double>();
        }
        double quality = qualityMetric(result.value("reply", ""), seeds);
        return json{{"text", text}, {"reply", result.value("reply", "")}, {"quality", quality}, {"latency", result.value("latency", 0)}, {"seeds", result.value("seeds", json::array())}};
    }

    double qualityMetric(const std::string &reply, const std::unordered_map<std::string, double> &seeds) {
        auto base = tokenize(reply);
        if (base.empty()) return 0.0;
        double linkedHit = linkedCoverage(base, seeds);
        double uniq = (double)std::unordered_set<std::string>(base.begin(), base.end()).size() / std::max(1.0, (double)base.size());
        double lenPenalty = base.size() > 50 ? 0.9 : 1.0;
        return (0.6 * linkedHit + 0.4 * uniq) * lenPenalty;
    }

    double linkedCoverage(const std::vector<std::string> &words, const std::unordered_map<std::string, double> &seeds) {
        double coverage = 0.0;
        int count = 0;
        for (auto &kv : seeds) {
            auto linked = pool_->getActive()->runtime()->getMemeWords(kv.first);
            if (linked.empty()) continue;
            std::unordered_set<std::string> linkedSet(linked.begin(), linked.end());
            int hit = 0;
            for (auto &w : words) if (linkedSet.count(w)) hit++;
            coverage += (double)hit / std::max(1.0, (double)linked.size());
            count++;
        }
        return count ? coverage / count : 0.0;
    }

    json summarize(const json &rounds) {
        json stats{{"attack", json{{"n", 0}, {"avgQuality", 0.0}}}, {"defense", json{{"fixes", 0}}}};
        double qsum = 0.0; int qcnt = 0;
        for (auto &r : rounds) {
            if (r.value("phase", "") == "attack") {
                auto results = r.value("results", json::array());
                stats["attack"]["n"] = stats["attack"]["n"].get<int>() + (int)results.size();
                for (auto &item : results) { qsum += item.value("quality", 0.0); qcnt++; }
            } else if (r.value("phase", "") == "defense") {
                stats["defense"]["fixes"] = stats["defense"]["fixes"].get<int>() + (int)r.value("fixes", json::array()).size();
            }
        }
        stats["attack"]["avgQuality"] = qcnt ? (qsum / qcnt) : 0.0;
        return stats;
    }
};

// ------------------ Redis Sync ------------------

class RedisSynchronizer {
public:
    RedisSynchronizer(std::shared_ptr<ControllerPoolBase> pool, const std::string &url, const std::string &channel)
        : pool_(std::move(pool)), url_(url), channel_(channel) {}

    void start() {
#ifdef HAVE_REDIS
        sw::redis::ConnectionOptions opts(url_);
        pub_ = std::make_unique<sw::redis::Redis>(opts);
        sub_ = std::make_unique<sw::redis::Redis>(opts);
        auto sub = sub_->subscriber();
        sub.subscribe(channel_);
        subThread_ = std::thread([this, sub = std::move(sub)]() mutable {
            while (running_) {
                sub.consume([&](std::string, std::string msg) {
                    try {
                        auto snapshot = json::parse(msg);
                        pool_->hotSwap(snapshot);
                    } catch (...) {}
                });
            }
        });
#endif
    }

    void publish(const json &snapshot) {
#ifdef HAVE_REDIS
        if (pub_) pub_->publish(channel_, snapshot.dump());
#endif
    }

    const std::string &channel() const { return channel_; }

private:
    std::shared_ptr<ControllerPoolBase> pool_;
    std::string url_;
    std::string channel_;
#ifdef HAVE_REDIS
    std::unique_ptr<sw::redis::Redis> pub_;
    std::unique_ptr<sw::redis::Redis> sub_;
#endif
    std::thread subThread_;
    std::atomic<bool> running_{true};
};

class RotationManager {
public:
    RotationManager(std::shared_ptr<ControllerPoolBase> pool, int cycleMs = 5 * 60 * 1000, int learnIters = 3, double minImprove = 0.05)
        : pool_(std::move(pool)), cycleMs_(cycleMs), learnIters_(learnIters), minImprove_(minImprove) {}

    void start() {
        if (running_) return;
        running_ = true;
        worker_ = std::thread([this]() {
            while (running_) {
                std::this_thread::sleep_for(std::chrono::milliseconds(cycleMs_));
                if (!running_) break;
                try { runCycle(); } catch (...) {}
            }
        });
    }

    void stop() {
        running_ = false;
        if (worker_.joinable()) worker_.join();
    }

    bool running() const { return running_; }
    int cycleMs() const { return cycleMs_; }

private:
    void runCycle() {
        auto ctrlB = pool_->getStandby();
        auto ctrlC = pool_->getValidation();
        if (!ctrlB || !ctrlC) return;
        auto params = ctrlB->runtime()->cloneParams();
        int iteration = params.value("iteration", MODEL_DEFAULTS.iteration);
        iteration += std::max(1, learnIters_);
        params["iteration"] = iteration;
        ctrlB->applyParams(params);

        std::uniform_real_distribution<double> dist(0.0, 1.0);
        double evalScore = dist(rng_);
        double baseScore = dist(rng_);
        if (evalScore - baseScore >= minImprove_) {
            pool_->hotSwap(ctrlB->snapshot());
        }
        ctrlC->applySnapshot(ctrlB->snapshot());
    }

    std::shared_ptr<ControllerPoolBase> pool_;
    int cycleMs_{5 * 60 * 1000};
    int learnIters_{3};
    double minImprove_{0.05};
    std::atomic<bool> running_{false};
    std::thread worker_;
    std::mt19937 rng_{std::random_device{}()};
};

StudyEngine::StudyEngine(std::shared_ptr<ControllerPoolBase> pool, std::shared_ptr<RedisSynchronizer> redis)
    : pool_(std::move(pool)), redis_(std::move(redis)) {}

void StudyEngine::start() {
    if (running_) return;
    running_ = true;
    worker_ = std::thread([this]() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::seconds(60));
            if (!running_) break;
            try { tick(); } catch (...) {}
        }
    });
}

void StudyEngine::enqueueDocument(const json &doc) {
    std::lock_guard<std::mutex> lock(mu_);
    queue_.push_back(doc);
    metrics_["enqueued"] = metrics_.value("enqueued", 0) + 1;
}

json StudyEngine::status() const {
    std::lock_guard<std::mutex> lock(mu_);
    return json{{"running", running_.load()}, {"queue", (int)queue_.size()}, {"metrics", metrics_}};
}

std::shared_ptr<LemmaWorkerPool> StudyEngine::ensureWorkerPool() {
    std::lock_guard<std::mutex> lock(workerMu_);
    if (workerPool_) return workerPool_;
    int workers = 1;
    try {
        auto cfg = pool_ ? pool_->config() : Config{};
        workers = std::max(1, cfg.maxWorkers);
    } catch (...) {
        workers = 1;
    }
    workerPool_ = std::make_shared<LemmaWorkerPool>(workers);
    return workerPool_;
}

void StudyEngine::tick() {
    json doc;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_ || queue_.empty()) return;
        doc = queue_.front();
        queue_.pop_front();
    }
    try {
        {
            std::lock_guard<std::mutex> lock(mu_);
            metrics_["lastTickAt"] = (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        }
        if (!doc.is_null()) {
            pool_->ingestDocument(doc);
        }
        try {
            if (!doc.is_null()) {
                std::string text = doc.value("text", "");
                auto tokens = tokenize(text);
                std::string lemmaCsv;
                if (pool_) {
                    auto active = pool_->getActive();
                    if (active) {
                        try { lemmaCsv = active->runtime()->config().lemmaCsv.string(); } catch (...) {}
                    }
                }
                auto wp = ensureWorkerPool();
                if (wp) {
                    wp->execBatchLemmatize(tokens, lemmaCsv);
                }
            }
        } catch (...) {
            // ignore worker errors
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            metrics_["processed"] = metrics_.value("processed", 0) + 1;
            metrics_["lastError"] = nullptr;
        }
    } catch (const std::exception &e) {
        std::lock_guard<std::mutex> lock(mu_);
        metrics_["lastError"] = std::string(e.what());
    }
    if (redis_) {
        try {
            auto snap = pool_->getStandby()->snapshot();
            redis_->publish(snap);
        } catch (...) {}
    }
}

// ------------------ Snapshot ------------------

class SnapshotManager {
public:
    SnapshotManager(std::shared_ptr<RuntimeBase> runtime, fs::path dir)
        : runtime_(std::move(runtime)), dir_(std::move(dir)) {
        ensureDir(dir_);
    }

    std::vector<std::string> list() const {
        std::vector<std::string> out;
        if (!fs::exists(dir_)) return out;
        for (auto &e : fs::directory_iterator(dir_)) {
            if (e.path().extension() == ".json") out.push_back(e.path().filename().string());
        }
        return out;
    }

    fs::path create(const std::string &name = "auto") {
        json snap = runtime_->toSnapshot();
        auto ts = nowIso();
        std::string file = ts;
        std::replace(file.begin(), file.end(), ':', '-');
        std::replace(file.begin(), file.end(), '.', '-');
        file += "_" + name + ".json";
        fs::path out = dir_ / file;
        std::ofstream o(out);
        o << snap.dump();
        return out;
    }

    json restore(const std::string &id) {
        fs::path file = dir_ / id;
        if (!fs::exists(file)) throw std::runtime_error("snapshot not found");
        std::ifstream in(file);
        json snap; in >> snap;
        runtime_->fromSnapshot(snap);
        return snap;
    }

    void remove(const std::string &id) {
        fs::path file = dir_ / id;
        if (fs::exists(file)) {
            fs::remove(file);
        }
    }

private:
    std::shared_ptr<RuntimeBase> runtime_;
    fs::path dir_;
};

// ------------------ Gateway ------------------

class GatewayServer {
public:
    GatewayServer(std::shared_ptr<ControllerPoolBase> pool,
                  std::shared_ptr<SnapshotManager> snapshots,
                  std::shared_ptr<RedisSynchronizer> redisSync,
                  std::shared_ptr<RotationManager> rotation,
                  std::shared_ptr<StudyEngine> study,
                  const Config &config)
    : pool_(std::move(pool)), snapshots_(std::move(snapshots)), redis_(std::move(redisSync)), rotation_(std::move(rotation)), study_(std::move(study)), config_(config) {
    startedAt_ = std::chrono::steady_clock::now();
        shards_ = std::make_shared<ShardManager>(pool_);
        json sparkOptions;
        sparkOptions["groupIds"] = pool_->listGroupIds();
        sparkOptions["groupOptions"] = json{{"numAI", config_.sparkNumAI}, {"budget", config_.sparkBudget}};
        spark_ = std::make_shared<BigSparkArray>(pool_, shards_, sparkOptions);
        {
            std::string authFlag = getEnv("AI_AUTH_ENABLED", "true");
            std::transform(authFlag.begin(), authFlag.end(), authFlag.begin(), ::tolower);
            authEnabled_ = (authFlag != "false");
        }
        jwtSecret_ = getEnv("AI_AUTH_JWT_SECRET", getEnv("AUTH_JWT_SECRET", "dev-secret-change-me"));
        rlDisabled_ = config_.disableLearning || config_.disableRL;
        advDisabled_ = config_.disableLearning || config_.disableADV;
        dialogLearningEnabled_ = !(config_.disableLearning == true);
        if (!config_.disableBarrier) {
            try {
                auto rt = std::dynamic_pointer_cast<RuntimeState>(pool_->getActive()->runtime());
                if (rt) {
                    barrier_ = std::make_shared<MemeBarrier>(rt, 10000, MODEL_DEFAULTS.maliciousThreshold, 5);
                    barrier_->start();
                }
            } catch (...) {
                barrier_.reset();
            }
        }
        setupRoutes();
    }

    void listen() {
        drogon::app().addListener(config_.gatewayHost, config_.portGateway);
        drogon::app().run();
    }

    void warmupLearning() {
        if (!config_.disableLearning && !config_.disableRL) {
            try { ensureRL()->learn(1); } catch (...) { rlDisabled_ = true; }
        }
        if (!config_.disableLearning && !config_.disableADV) {
            try {
                auto docs = pool_->getActive()->runtime()->collectRobotsDocuments(json{{"limit", 3}, {"shuffle", true}});
                json samples = json::array();
                for (auto &d : docs) { if (!d.text.empty()) samples.push_back(d.text); if (samples.size() >= 3) break; }
                if (!samples.empty()) ensureADV()->attackAndDefend(samples);
            } catch (...) {}
        }
    }

private:
    std::shared_ptr<ControllerPoolBase> pool_;
    std::shared_ptr<SnapshotManager> snapshots_;
    std::shared_ptr<RedisSynchronizer> redis_;
    std::shared_ptr<RotationManager> rotation_;
    std::shared_ptr<StudyEngine> study_;
    std::shared_ptr<ShardManager> shards_;
    std::shared_ptr<BigSparkArray> spark_;
    std::shared_ptr<ReinforcementLearner> rl_;
    std::shared_ptr<AdversarialLearner> adv_;
    transformer::TransformerService transformer_;
    std::mutex transformerMu_;
    Config config_;
    std::chrono::steady_clock::time_point startedAt_;
    bool authEnabled_{true};
    std::string jwtSecret_;
    std::shared_ptr<MemeBarrier> barrier_;
    bool rlDisabled_{false};
    bool advDisabled_{false};
    bool dialogLearningEnabled_{true};
    struct DialogCounters { int total{0}; int lastRL{0}; int lastADV{0}; } dialogCounters_;
    struct DialogThresholds { double rlEvery{20}; double advEvery{30}; } dialogThresholds_;
    struct AuthError { std::string error; std::string message; drogon::HttpStatusCode code{drogon::k401Unauthorized}; };
    static thread_local AuthError lastAuthError_;
    static thread_local bool hasAuthError_;

    json parseRequestBody(const drogon::HttpRequestPtr &req, bool &ok) const {
        ok = true;
        auto payload = req->getJsonObject();
        if (payload) return fromJsoncpp(*payload);
        auto body = req->getBody();
        if (body.empty()) return json::object();
        auto contentType = req->getHeader("content-type");
        std::string lowered = contentType;
        std::transform(lowered.begin(), lowered.end(), lowered.begin(), ::tolower);
        if (lowered.find("application/x-www-form-urlencoded") != std::string::npos) {
            return parseFormUrlEncoded(std::string(body));
        }
        ok = false;
        return json();
    }

    bool jsTruthy(const json &v) const {
        if (v.is_null()) return false;
        if (v.is_boolean()) return v.get<bool>();
        if (v.is_number()) return v.get<double>() != 0.0;
        if (v.is_string()) return !v.get<std::string>().empty();
        return true;
    }

    bool isGroupProc() const { return dynamic_cast<GroupProcessPool *>(pool_.get()) != nullptr; }

    bool authOK(const drogon::HttpRequestPtr &req) {
        hasAuthError_ = false;
        if (!authEnabled_) return true;
        if (req->method() == drogon::Options) return true;
        auto path = req->path();
        if (path == "/api/system/status") return true;
        if (path.rfind("/api/", 0) != 0) return true;
        auto auth = req->getHeader("authorization");
        std::string token;
        try {
            std::regex re("^Bearer\\s+(.+)$", std::regex::icase);
            std::smatch m;
            if (std::regex_match(auth, m, re) && m.size() >= 2) {
                token = m[1].str();
            }
        } catch (...) {
            token.clear();
        }
        if (token.empty()) {
            lastAuthError_ = {"unauthorized", "", drogon::k401Unauthorized};
            hasAuthError_ = true;
            return false;
        }
        try {
            auto dec = jwt::decode<jwt::traits::nlohmann_json>(token);
            jwt::verify<jwt::traits::nlohmann_json>()
                .allow_algorithm(jwt::algorithm::hs256{jwtSecret_})
                .verify(dec);
            return true;
        } catch (const std::exception &e) {
            lastAuthError_ = {"invalid-token", e.what(), drogon::k401Unauthorized};
            hasAuthError_ = true;
            return false;
        } catch (...) {
            lastAuthError_ = {"invalid-token", "invalid token", drogon::k401Unauthorized};
            hasAuthError_ = true;
            return false;
        }
    }

    std::string buildGraphContext(const json &graphResult, const json &payload) {
        auto rt = std::dynamic_pointer_cast<RuntimeState>(pool_->getActive()->runtime());
        if (!rt) return "";
        auto weightBucket = [](double w) {
            if (!std::isfinite(w)) return std::string("w_unknown");
            if (w < 0.5) return std::string("w_low");
            if (w < 1.5) return std::string("w_mid");
            return std::string("w_high");
        };
        auto actBucket = [](double a) {
            if (!std::isfinite(a)) return std::string("act_unknown");
            if (a < 0.2) return std::string("act_low");
            if (a < 0.8) return std::string("act_mid");
            return std::string("act_high");
        };
        auto degBucket = [](int d) {
            if (d < 2) return std::string("deg_low");
            if (d < 6) return std::string("deg_mid");
            return std::string("deg_high");
        };
        auto dirToken = [](int dir) {
            if (dir == 1) return std::string("dir_in");
            if (dir == 2) return std::string("dir_out");
            return std::string("dir_bi");
        };
        std::vector<std::string> words;
        if (payload.contains("tokens") && payload["tokens"].is_array()) {
            for (auto &t : payload["tokens"]) { if (t.is_string()) words.push_back(t.get<std::string>()); }
        } else if (payload.contains("text") && payload["text"].is_string()) {
            words = tokenize(payload["text"].get<std::string>());
        }

        std::vector<std::pair<std::string, double>> memes;
        std::unordered_set<std::string> seedIds;
        if (graphResult.contains("seeds") && graphResult["seeds"].is_array()) {
            for (auto &s : graphResult["seeds"]) {
                if (s.is_array() && s.size() >= 1 && s[0].is_string()) seedIds.insert(s[0].get<std::string>());
            }
        }
        if (graphResult.contains("memes") && graphResult["memes"].is_array()) {
            auto mem = graphResult["memes"];
            auto act = graphResult.value("activation", json::array());
            for (size_t i = 0; i < mem.size(); i++) {
                if (!mem[i].is_string()) continue;
                double score = 0.0;
                if (act.is_array() && i < act.size() && act[i].is_number()) score = act[i].get<double>();
                memes.push_back({mem[i].get<std::string>(), score});
            }
        }
        std::sort(memes.begin(), memes.end(), [](auto &a, auto &b) { return a.second > b.second; });
        if (memes.size() > 16) memes.resize(16);
        std::unordered_set<std::string> memeSet;
        for (auto &m : memes) memeSet.insert(m.first);

        std::vector<std::string> ctxTokens;
        auto pushToken = [&](const std::string &t) {
            if (t.empty()) return;
            if (ctxTokens.size() >= 512) return;
            ctxTokens.push_back(t);
        };
        auto pushTokens = [&](const std::vector<std::string> &ts) {
            for (const auto &t : ts) pushToken(t);
        };

        if (!words.empty()) {
            pushToken("input");
            for (size_t i = 0; i < words.size() && i < 32; i++) pushToken(words[i]);
        }

        for (size_t i = 0; i < memes.size(); i++) {
            std::string memeId = memes[i].first;
            pushToken("node");
            pushToken(seedIds.count(memeId) ? "seed" : "cand");
            pushToken(actBucket(memes[i].second));
            pushToken(degBucket(rt->graph().degreeOf(memeId)));
            pushToken("meme");
            pushTokens(tokenize(memeId));
            auto ws = rt->getMemeWords(memeId);
            int wlim = 0;
            for (auto &w : ws) {
                if (w.empty()) continue;
                pushTokens(tokenize(w));
                if (++wlim >= 6) break;
            }
        }

        int edgeCount = 0;
        for (auto &m : memes) {
            for (auto &n : rt->graph().neighborsOf(m.first)) {
                if (!memeSet.count(n.first)) continue;
                pushToken("edge");
                pushTokens(tokenize(m.first));
                pushToken(weightBucket(n.second.first));
                pushToken(dirToken(n.second.second));
                pushToken("to");
                pushTokens(tokenize(n.first));
                if (++edgeCount >= 32) break;
            }
            if (edgeCount >= 32) break;
        }

        std::ostringstream oss;
        for (size_t i = 0; i < ctxTokens.size(); i++) {
            if (i) oss << " ";
            oss << ctxTokens[i];
        }
        std::string out = oss.str();
        if (out.size() > 2048) out.resize(2048);
        return out;
    }

    std::vector<std::vector<float>> buildGraphEmbeddings(const json &graphResult, int dim, int rounds = 1) {
        auto rt = std::dynamic_pointer_cast<RuntimeState>(pool_->getActive()->runtime());
        if (!rt) return {};
        std::vector<std::string> ids;
        if (graphResult.contains("memes") && graphResult["memes"].is_array()) {
            for (auto &m : graphResult["memes"]) {
                if (!m.is_string()) continue;
                ids.push_back(m.get<std::string>());
                if (ids.size() >= 64) break;
            }
        }
        if (ids.empty()) return {};
        GraphTensorBridge bridge(&rt->kvm(), nullptr);
        auto base = bridge.computeNodeEmbeddings(ids, dim);
        return rt->graph().messagePassingEmbeddings(ids, base, rounds, 0.65f, 0.35f);
    }

    std::vector<transformer::TrainSample> parseTrainSamples(const json &payload) {
        std::vector<transformer::TrainSample> samples;
        if (!payload.contains("samples") || !payload["samples"].is_array()) return samples;
        for (auto &s : payload["samples"]) {
            if (!s.is_object()) continue;
            transformer::TrainSample sample;
            sample.input = s.value("input", "");
            sample.target = s.value("target", "");
            sample.graph = s.value("graph", "");
            samples.push_back(std::move(sample));
        }
        return samples;
    }

    json getRuntimeFeatureState() {
        json out;
        out["memebarrier"] = {
            {"enabled", barrier_ ? barrier_->running() : false},
            {"available", !config_.disableBarrier},
            {"threshold", barrier_ ? barrier_->threshold() : MODEL_DEFAULTS.maliciousThreshold}
        };
        out["learning"] = {
            {"enabled", !(rlDisabled_ && advDisabled_)},
            {"cliDisabled", config_.disableLearning}
        };
        out["rl"] = { {"enabled", !rlDisabled_}, {"cliDisabled", (config_.disableLearning || config_.disableRL)} };
        out["adv"] = { {"enabled", !advDisabled_}, {"cliDisabled", (config_.disableLearning || config_.disableADV)} };
        out["dialogLearning"] = { {"enabled", dialogLearningEnabled_} };
        out["dialogThresholds"] = { {"rlEvery", dialogThresholds_.rlEvery}, {"advEvery", dialogThresholds_.advEvery} };
        out["dialogCounters"] = { {"total", dialogCounters_.total}, {"lastRL", dialogCounters_.lastRL}, {"lastADV", dialogCounters_.lastADV} };
        return out;
    }

    json applyRuntimeFeaturePatch(const json &patch) {
        json out{{"applied", json::object()}, {"warnings", json::array()}};
        if (patch.contains("memebarrierEnabled") && patch["memebarrierEnabled"].is_boolean()) {
            bool on = patch["memebarrierEnabled"].get<bool>();
            if (on) {
                auto rt = std::dynamic_pointer_cast<RuntimeState>(pool_->getActive()->runtime());
                if (!barrier_ && rt) barrier_ = std::make_shared<MemeBarrier>(rt, 10000, MODEL_DEFAULTS.maliciousThreshold, 5);
                if (barrier_) barrier_->start();
            } else if (barrier_) {
                barrier_->stop();
            }
            out["applied"]["memebarrierEnabled"] = on;
        }
        if (patch.contains("maliciousThreshold") && patch["maliciousThreshold"].is_number()) {
            double v = patch["maliciousThreshold"].get<double>();
            auto rt = std::dynamic_pointer_cast<RuntimeState>(pool_->getActive()->runtime());
            if (!barrier_ && rt) barrier_ = std::make_shared<MemeBarrier>(rt, 10000, v, 5);
            if (barrier_) barrier_->setThreshold(v);
            out["applied"]["maliciousThreshold"] = v;
        }
        if (patch.contains("learningEnabled") && patch["learningEnabled"].is_boolean()) {
            bool on = patch["learningEnabled"].get<bool>();
            if (!on) {
                rlDisabled_ = true;
                advDisabled_ = true;
                dialogLearningEnabled_ = false;
            } else {
                if (config_.disableLearning) {
                    out["warnings"].push_back("learning was CLI-disabled; runtime override enabled");
                }
            }
            out["applied"]["learningEnabled"] = on;
        }
        if (patch.contains("rlEnabled") && patch["rlEnabled"].is_boolean()) {
            bool on = patch["rlEnabled"].get<bool>();
            if (on && (config_.disableLearning || config_.disableRL)) {
                out["warnings"].push_back("rl was CLI-disabled; runtime override enabled");
            }
            rlDisabled_ = !on;
            out["applied"]["rlEnabled"] = on;
        }
        if (patch.contains("advEnabled") && patch["advEnabled"].is_boolean()) {
            bool on = patch["advEnabled"].get<bool>();
            if (on && (config_.disableLearning || config_.disableADV)) {
                out["warnings"].push_back("adv was CLI-disabled; runtime override enabled");
            }
            advDisabled_ = !on;
            out["applied"]["advEnabled"] = on;
        }
        if (patch.contains("dialogLearningEnabled") && patch["dialogLearningEnabled"].is_boolean()) {
            dialogLearningEnabled_ = patch["dialogLearningEnabled"].get<bool>();
            out["applied"]["dialogLearningEnabled"] = dialogLearningEnabled_;
        }
        if (patch.contains("rlEvery") && patch["rlEvery"].is_number()) {
            double v = patch["rlEvery"].get<double>();
            if (std::isfinite(v) && v > 0) {
                dialogThresholds_.rlEvery = v;
                out["applied"]["rlEvery"] = dialogThresholds_.rlEvery;
            }
        }
        if (patch.contains("advEvery") && patch["advEvery"].is_number()) {
            double v = patch["advEvery"].get<double>();
            if (std::isfinite(v) && v > 0) {
                dialogThresholds_.advEvery = v;
                out["applied"]["advEvery"] = dialogThresholds_.advEvery;
            }
        }
        return out;
    }

    std::shared_ptr<ReinforcementLearner> ensureRL() {
        if (!rl_) rl_ = std::make_shared<ReinforcementLearner>(pool_, getProjectRoot() / "tests");
        return rl_;
    }

    std::shared_ptr<AdversarialLearner> ensureADV() {
        if (!adv_) adv_ = std::make_shared<AdversarialLearner>(pool_);
        return adv_;
    }

    void setupRoutes() {
        drogon::app().registerHandler("/", [](const drogon::HttpRequestPtr &, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            auto resp = drogon::HttpResponse::newHttpResponse();
            resp->setStatusCode(drogon::k404NotFound);
            resp->setBody("UI moved to auth_frontend_server.cjs (default :5081)");
            cb(resp);
        });

        drogon::app().registerHandler("/api/graph/chat", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                auto controller = pool_->getActive();
                json result = controller->respond(body);
                respondJson(cb, json{{"ok", true}, {"result", result}});
                onDialogCompleted(result, body);
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/chat", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                auto controller = pool_->getActive();
                json graphResult = controller->respond(body);
                if (graphResult.contains("addon") && graphResult["addon"].is_object()) {
                    json result = graphResult;
                    result["graph"] = graphResult;
                    result["graphReply"] = graphResult.value("reply", "");
                    result["reply"] = graphResult.value("reply", "");
                    result["transformerReply"] = graphResult.value("reply", "");
                    result["graphContext"] = "";
                    respondJson(cb, json{{"ok", true}, {"result", result}});
                    onDialogCompleted(result, body);
                    return;
                }
                std::string graphContext = buildGraphContext(graphResult, body);
                int maxTokens = graphResult.value("maxTokens", 128);
                if (body.contains("maxTokens") && body["maxTokens"].is_number_integer()) maxTokens = body["maxTokens"].get<int>();
                std::string text = body.value("text", body.value("message", ""));
                json tReply;
                {
                    std::lock_guard<std::mutex> lock(transformerMu_);
                    int dim = transformer_.params().value("dModel", 128);
                    auto graphEmb = buildGraphEmbeddings(graphResult, dim, 2);
                    tReply = transformer_.chat(text, graphContext, graphEmb, maxTokens);
                }
                std::string reply = tReply.value("reply", "");
                json verify;
                {
                    std::lock_guard<std::mutex> lock(transformerMu_);
                    verify = transformer_.verify(text, graphContext, reply);
                }
                if (!verify.value("ok", true)) {
                    reply = graphResult.value("reply", reply);
                }
                json result = graphResult;
                result["graph"] = graphResult;
                result["graphReply"] = graphResult.value("reply", "");
                result["reply"] = reply;
                result["transformerReply"] = reply;
                result["graphContext"] = graphContext;
                result["verify"] = verify;
                respondJson(cb, json{{"ok", true}, {"result", result}});
                onDialogCompleted(result, body);
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/transformer/chat", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                std::string text = body.value("text", body.value("message", ""));
                std::string graphContext = body.value("graphContext", "");
                json graphResult;
                if (graphContext.empty()) {
                    auto controller = pool_->getActive();
                    graphResult = controller->respond(body);
                    graphContext = buildGraphContext(graphResult, body);
                }
                int maxTokens = body.value("maxTokens", 128);
                json tReply;
                {
                    std::lock_guard<std::mutex> lock(transformerMu_);
                    int dim = transformer_.params().value("dModel", 128);
                    auto graphEmb = buildGraphEmbeddings(graphResult, dim, 2);
                    tReply = transformer_.chat(text, graphContext, graphEmb, maxTokens);
                }
                json verify;
                {
                    std::lock_guard<std::mutex> lock(transformerMu_);
                    verify = transformer_.verify(text, graphContext, tReply.value("reply", ""));
                }
                json result = tReply;
                result["graphContext"] = graphContext;
                result["verify"] = verify;
                if (!graphResult.is_null()) result["graph"] = graphResult;
                respondJson(cb, json{{"ok", true}, {"result", result}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/transformer/pretrain", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json payload = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            int epochs = payload.value("epochs", 1);
            float lr = payload.value("lr", 0.001f);
            auto samples = parseTrainSamples(payload);
            json out;
            {
                std::lock_guard<std::mutex> lock(transformerMu_);
                out = transformer_.pretrain(samples, epochs, lr);
            }
            respondJson(cb, json{{"ok", true}, {"result", out}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/transformer/joint_train", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json payload = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            int epochs = payload.value("epochs", 1);
            float lr = payload.value("lr", 0.001f);
            float gw = payload.value("graphWeight", 0.35f);
            auto samples = parseTrainSamples(payload);
            json out;
            {
                std::lock_guard<std::mutex> lock(transformerMu_);
                out = transformer_.jointTrain(samples, epochs, lr, gw);
            }
            respondJson(cb, json{{"ok", true}, {"result", out}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/transformer/ga_optimize", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json payload = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            int generations = payload.value("generations", 3);
            int population = payload.value("population", 6);
            auto samples = parseTrainSamples(payload);
            json out;
            {
                std::lock_guard<std::mutex> lock(transformerMu_);
                out = transformer_.optimizeGA(samples, generations, population);
            }
            respondJson(cb, json{{"ok", true}, {"result", out}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/transformer/verify", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json payload = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            std::string text = payload.value("text", "");
            std::string graphContext = payload.value("graphContext", "");
            std::string reply = payload.value("reply", "");
            json out;
            {
                std::lock_guard<std::mutex> lock(transformerMu_);
                out = transformer_.verify(text, graphContext, reply);
            }
            respondJson(cb, json{{"ok", true}, {"result", out}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/transformer/params", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            if (req->method() == drogon::Get) {
                json out;
                {
                    std::lock_guard<std::mutex> lock(transformerMu_);
                    out = transformer_.params();
                }
                respondJson(cb, json{{"ok", true}, {"params", out}});
                return;
            }
            bool ok = true;
            json payload = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            {
                std::lock_guard<std::mutex> lock(transformerMu_);
                transformer_.applyParams(payload);
            }
            json out;
            {
                std::lock_guard<std::mutex> lock(transformerMu_);
                out = transformer_.params();
            }
            respondJson(cb, json{{"ok", true}, {"params", out}});
        }, {drogon::Get, drogon::Post});

        drogon::app().registerHandler("/api/runtime/features", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            respondJson(cb, json{{"ok", true}, {"features", getRuntimeFeatureState()}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/runtime/features", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                auto result = applyRuntimeFeaturePatch(body);
                respondJson(cb, json{{"ok", true}, {"result", result}, {"features", getRuntimeFeatureState()}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Patch});

        drogon::app().registerHandler("/api/addons", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            auto controller = pool_->getActive();
            if (!controller) return badRequest(cb, "controller not available");
            auto runtime = controller->runtime();
            auto local = std::dynamic_pointer_cast<RuntimeState>(runtime);
            if (!local) return badRequest(cb, "runtime not supported");
            respondJson(cb, json{{"ok", true}, {"addons", local->listAddons()}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/addons/add", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json body = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            auto controller = pool_->getActive();
            if (!controller) return badRequest(cb, "controller not available");
            auto runtime = controller->runtime();
            auto local = std::dynamic_pointer_cast<RuntimeState>(runtime);
            if (!local) return badRequest(cb, "runtime not supported");
            std::string path = body.value("path", body.value("libraryPath", ""));
            std::string type = body.value("type", "");
            std::string name = body.value("name", "");
            std::string error;
            bool okAdd = false;
            if (!path.empty()) {
                okAdd = local->addAddon(path, error);
            } else {
                okAdd = local->addAddon(type, name, error);
            }
            if (!okAdd) {
                respondJson(cb, json{{"ok", false}, {"error", error.empty() ? "addon add failed" : error}}, drogon::k400BadRequest);
                return;
            }
            respondJson(cb, json{{"ok", true}, {"addons", local->listAddons()}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/addons/remove", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json body = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            auto controller = pool_->getActive();
            if (!controller) return badRequest(cb, "controller not available");
            auto runtime = controller->runtime();
            auto local = std::dynamic_pointer_cast<RuntimeState>(runtime);
            if (!local) return badRequest(cb, "runtime not supported");
            std::string name = body.value("name", "");
            std::string error;
            if (!local->removeAddon(name, error)) {
                respondJson(cb, json{{"ok", false}, {"error", error.empty() ? "addon remove failed" : error}}, drogon::k400BadRequest);
                return;
            }
            respondJson(cb, json{{"ok", true}, {"addons", local->listAddons()}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/study/status", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            if (!study_) {
                respondJson(cb, json{{"ok", true}, {"running", false}, {"queue", 0}, {"metrics", nullptr}});
                return;
            }
            auto status = study_->status();
            json metrics = status.contains("metrics") ? status["metrics"] : json();
            respondJson(cb, json{{"ok", true}, {"running", status.value("running", false)}, {"queue", status.value("queue", 0)}, {"metrics", metrics}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/learn/dialog/reset", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            dialogCounters_ = {};
            respondJson(cb, json{{"ok", true}, {"dialogCounters", json{{"total", dialogCounters_.total}, {"lastRL", dialogCounters_.lastRL}, {"lastADV", dialogCounters_.lastADV}}}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/array/chat", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                auto payload = std::make_shared<json>(body);
                if (!payload || !payload->contains("text")) return badRequest(cb, "text required");
                auto &textVal = (*payload)["text"];
                bool truthy = true;
                if (textVal.is_null()) truthy = false;
                else if (textVal.is_boolean()) truthy = textVal.get<bool>();
                else if (textVal.is_number()) truthy = (textVal.get<double>() != 0.0);
                else if (textVal.is_string()) truthy = !textVal.get<std::string>().empty();
                if (!truthy) return badRequest(cb, "text required");
                json opts = payload->value("options", json::object());
                auto jsNumber = [&](const json &v, double fallback) -> double {
                    if (v.is_number()) return v.get<double>();
                    if (v.is_boolean()) return v.get<bool>() ? 1.0 : 0.0;
                    if (v.is_string()) {
                        double out = 0.0;
                        if (parseNumberLikeJs(v.get<std::string>(), out)) return out;
                        return fallback;
                    }
                    return fallback;
                };
                int bigRounds = 1;
                if (opts.contains("bigRounds")) {
                    double v = jsNumber(opts["bigRounds"], 1.0);
                    if (std::isfinite(v)) {
                        bigRounds = (v == 0.0) ? 1 : (int)v;
                    }
                } else if (opts.contains("bigSparkRounds")) {
                    double v = jsNumber(opts["bigSparkRounds"], 1.0);
                    if (std::isfinite(v)) {
                        bigRounds = (v == 0.0) ? 1 : (int)v;
                    }
                }
                json result = bigRounds > 1 ? spark_->dispatchBig(*payload, opts) : spark_->dispatch(*payload, opts);
                respondJson(cb, json{{"ok", true}, {"result", result}});
                json dialog = json();
                if (result.contains("aggregate")) {
                    json agg = result["aggregate"];
                    dialog = json::object();
                    if (agg.is_object()) {
                        if (agg.contains("reply")) dialog["reply"] = agg["reply"];
                        if (agg.contains("latency")) dialog["latency"] = agg["latency"];
                        if (agg.contains("sessionId")) dialog["sessionId"] = agg["sessionId"];
                    }
                    dialog["seeds"] = json::array();
                }
                onDialogCompleted(dialog, *payload);
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/learn/reinforce", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            if (rlDisabled_) return respondJson(cb, json{{"ok", false}, {"error", "rl-disabled"}}, drogon::k503ServiceUnavailable);
            try {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                auto payload = std::make_shared<json>(body);
                int cycles = 3;
                if (payload && payload->contains("cycles")) {
                    auto jsNumber = [&](const json &v, double fallback) -> double {
                        if (v.is_number()) return v.get<double>();
                        if (v.is_boolean()) return v.get<bool>() ? 1.0 : 0.0;
                        if (v.is_string()) {
                            double out = 0.0;
                            if (parseNumberLikeJs(v.get<std::string>(), out)) return out;
                            return fallback;
                        }
                        return fallback;
                    };
                    double v = jsNumber((*payload)["cycles"], 3.0);
                    if (std::isfinite(v)) cycles = (v == 0.0) ? 3 : (int)v;
                }
                auto out = ensureRL()->learn(cycles);
                respondJson(cb, json{{"ok", true}, {"result", out}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/learn/reinforce/latest", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            respondJson(cb, json{{"ok", true}, {"latest", rl_ ? rl_->latest() : json()}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/learn/adversarial", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            if (advDisabled_) return respondJson(cb, json{{"ok", false}, {"error", "adv-disabled"}}, drogon::k503ServiceUnavailable);
            try {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                auto payload = std::make_shared<json>(body);
                if (!payload || !payload->contains("samples") || !(*payload)["samples"].is_array()) return badRequest(cb, "samples required");
                if ((*payload)["samples"].empty()) return badRequest(cb, "samples required");
                auto out = ensureADV()->attackAndDefend((*payload)["samples"]);
                respondJson(cb, json{{"ok", true}, {"result", out}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/learn/adversarial/latest", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            respondJson(cb, json{{"ok", true}, {"latest", adv_ ? adv_->latest() : json()}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/learn/thresholds", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json body = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            if (body.is_object()) {
                if (body.contains("rlEvery") && body["rlEvery"].is_number()) {
                    double v = body["rlEvery"].get<double>();
                    if (std::isfinite(v) && v > 0) dialogThresholds_.rlEvery = v;
                }
                if (body.contains("advEvery") && body["advEvery"].is_number()) {
                    double v = body["advEvery"].get<double>();
                    if (std::isfinite(v) && v > 0) dialogThresholds_.advEvery = v;
                }
            }
            respondJson(cb, json{{"ok", true}, {"thresholds", json{{"rlEvery", dialogThresholds_.rlEvery}, {"advEvery", dialogThresholds_.advEvery}}}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/memebarrier/start", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                double th = MODEL_DEFAULTS.maliciousThreshold;
                if (body.contains("maliciousThreshold") && body["maliciousThreshold"].is_number()) {
                    th = body["maliciousThreshold"].get<double>();
                }
                auto rt = std::dynamic_pointer_cast<RuntimeState>(pool_->getActive()->runtime());
                if (!barrier_) barrier_ = std::make_shared<MemeBarrier>(rt, 10000, th, 5);
                barrier_->setThreshold(th);
                barrier_->start();
                respondJson(cb, json{{"ok", true}, {"running", true}, {"threshold", barrier_->threshold()}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/memebarrier/stop", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                if (barrier_) barrier_->stop();
                respondJson(cb, json{{"ok", true}, {"running", false}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/memebarrier/stats", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                json stats;
                if (barrier_) {
                    auto s = barrier_->getStats();
                    stats = json{{"scans", s.scans}, {"isolated", s.isolated}, {"lastScanTime", s.lastScanTime}, {"lastIsolated", s.lastIsolated}};
                }
                respondJson(cb, json{{"ok", true}, {"stats", stats}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Get});
        drogon::app().registerHandler("/api/model/params", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            if (req->method() == drogon::Get) {
                respondJson(cb, json{{"ok", true}, {"params", pool_->getActive()->runtime()->cloneParams()}});
            } else if (req->method() == drogon::Post) {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                pool_->getActive()->applyParams(body);
                respondJson(cb, json{{"ok", true}, {"params", pool_->getActive()->runtime()->cloneParams()}});
            }
        }, {drogon::Get, drogon::Post});

        drogon::app().registerHandler("/api/model/params/reset", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            json defaults = json{
                {"decayFactor", MODEL_DEFAULTS.decayFactor},
                {"maxMemeWords", MODEL_DEFAULTS.maxMemeWords},
                {"minOverlapThreshold", MODEL_DEFAULTS.minOverlapThreshold},
                {"memeNgramMin", MODEL_DEFAULTS.memeNgramMin},
                {"memeNgramMax", MODEL_DEFAULTS.memeNgramMax},
                {"maliciousThreshold", MODEL_DEFAULTS.maliciousThreshold},
                {"learningIterations", MODEL_DEFAULTS.learningIterations},
                {"iteration", MODEL_DEFAULTS.iteration},
                {"threshold", MODEL_DEFAULTS.threshold},
                {"decay", MODEL_DEFAULTS.decay},
                {"decayK", MODEL_DEFAULTS.decayK},
                {"maxLen", MODEL_DEFAULTS.maxLen},
                {"edgeWeight", MODEL_DEFAULTS.edgeWeight},
                {"activationType", MODEL_DEFAULTS.activationType},
                {"transferType", MODEL_DEFAULTS.transferType},
                {"activationCustom", MODEL_DEFAULTS.activationCustom},
                {"transferCustom", MODEL_DEFAULTS.transferCustom},
                {"mappingDepth", MODEL_DEFAULTS.mappingDepth},
                {"reflectionTopMemes", MODEL_DEFAULTS.reflectionTopMemes},
                {"reflectionTopWords", MODEL_DEFAULTS.reflectionTopWords},
                {"reflectionMinScore", MODEL_DEFAULTS.reflectionMinScore}
            };
            pool_->getActive()->applyParams(defaults);
            respondJson(cb, json{{"ok", true}, {"params", pool_->getActive()->runtime()->cloneParams()}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/array/layers", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            json history = json::array();
            auto &h = spark_->history();
            int start = (int)h.size() > 20 ? (int)h.size() - 20 : 0;
            for (int i = start; i < (int)h.size(); i++) history.push_back(h[i]);
            respondJson(cb, json{{"ok", true}, {"layers", spark_->getLayers()}, {"history", history}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/array/layers", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json body = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            try {
                spark_->updateLayers(body.value("layers", json::array()));
                respondJson(cb, json{{"ok", true}, {"layers", spark_->getLayers()}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k400BadRequest);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/array/history", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            json history = json::array();
            auto &h = spark_->history();
            int start = (int)h.size() > 20 ? (int)h.size() - 20 : 0;
            for (int i = start; i < (int)h.size(); i++) history.push_back(h[i]);
            respondJson(cb, json{{"ok", true}, {"history", history}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/system/status", [this](const drogon::HttpRequestPtr &, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            json out;
            const auto now = std::chrono::steady_clock::now();
            const auto uptime = std::chrono::duration_cast<std::chrono::duration<double>>(now - startedAt_).count();
            out["ok"] = true;
            out["uptime"] = uptime;
            out["load"] = getLoadAvg();
            out["memory"] = getMemoryUsage();
            out["controllers"] = pool_->listMetrics();
            out["groups"] = json::array();
            for (auto &gid : pool_->listGroupIds()) {
                out["groups"].push_back({{"gid", gid}, {"controllers", pool_->listControllersInGroup(gid)}});
            }
            out["rotation"] = json{{"running", rotation_ ? rotation_->running() : false}, {"cycleMs", rotation_ ? rotation_->cycleMs() : 0}};
            out["redis"] = json{{"channel", redis_ ? redis_->channel() : std::string("" )}};
            respondJson(cb, out);
        }, {drogon::Get});

        drogon::app().registerHandler("/api/system/config", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            json cfg;
            cfg["groupCount"] = config_.groupCount;
            cfg["groupSize"] = config_.groupSize;
            cfg["sparkNumAI"] = config_.sparkNumAI;
            cfg["sparkBudget"] = config_.sparkBudget;
            cfg["groupIds"] = pool_->listGroupIds();
            cfg["gatewayHost"] = config_.gatewayHost;
            cfg["portGateway"] = config_.portGateway;
            cfg["portStudy"] = config_.portStudy;
            respondJson(cb, json{{"ok", true}, {"config", cfg}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/groups", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            json out;
            out["ok"] = true;
            out["groupSize"] = config_.groupSize;
            out["groupCount"] = config_.groupCount;
            out["groups"] = json::array();
            for (auto &gid : pool_->listGroupIds()) {
                out["groups"].push_back({{"gid", gid}, {"controllers", pool_->listControllersInGroup(gid)}});
            }
            respondJson(cb, out);
        }, {drogon::Get});

        drogon::app().registerHandler("/api/groups/{1}/metrics", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb, const std::string &gid) {
            if (!authOK(req)) return unauthorized(cb);
            auto list = pool_->listControllersInGroup(gid);
            if (list.empty()) return respondJson(cb, json{{"ok", false}, {"error", "group-not-found"}}, drogon::k404NotFound);
            json metrics = json::array();
            if (isGroupProc()) {
                for (auto &name : list) {
                    (void)name;
                    metrics.push_back(json::object());
                }
            } else {
                for (auto &name : list) {
                    auto ctrl = pool_->getByName(name);
                    if (ctrl) metrics.push_back(ctrl->metrics());
                }
            }
            respondJson(cb, json{{"ok", true}, {"gid", gid}, {"controllers", metrics}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/system/ai/{1}", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb, const std::string &name) {
            if (!authOK(req)) return unauthorized(cb);
            auto ctrl = pool_->getByName(name);
            if (!ctrl) return respondJson(cb, json{{"ok", false}, {"error", "Controller not found"}}, drogon::k404NotFound);
            if (isGroupProc()) {
                respondJson(cb, json{{"ok", true}, {"controller", json::object()}});
            } else {
                respondJson(cb, json{{"ok", true}, {"controller", ctrl->metrics()}});
            }
        }, {drogon::Get});

        drogon::app().registerHandler("/api/shards", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            respondJson(cb, json{{"ok", true}, {"shards", shards_->metrics()}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/study/enqueue", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json body = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            if (!body.contains("text") || !jsTruthy(body["text"])) return badRequest(cb, "text required");
            json doc{{"text", body["text"]}, {"queuedAt", nowEpochMs()}};
            if (study_) study_->enqueueDocument(doc);
            respondJson(cb, json{{"ok", true}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/corpus/ingest", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json payload = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            if (!payload.contains("text") || !jsTruthy(payload["text"])) return badRequest(cb, "text required");
            try {
                auto results = pool_->ingestDocument(payload);
                if (payload.value("enqueueStudy", false) && study_) {
                    study_->enqueueDocument(payload);
                }
                respondJson(cb, json{{"ok", true}, {"results", results}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/corpus/forget", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json payload = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                auto results = pool_->forgetMemes(payload);
                respondJson(cb, json{{"ok", true}, {"results", results}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/corpus/online", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json payload = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                std::string query = payload.value("query", "");
                json tokens = payload.value("tokens", json::array());
                if (query.empty() && (!tokens.is_array() || tokens.empty())) return badRequest(cb, "query or tokens required");
                auto result = pool_->onlineResearch(!query.empty() ? json(query) : tokens, payload.value("options", json::object()));
                respondJson(cb, json{{"ok", true}, {"result", result}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/corpus/crawl", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json payload = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            std::string url = trimString(payload.value("startUrl", ""));
            if (url.empty()) return badRequest(cb, "startUrl required");
            json options = payload.value("options", json::object());
            json crawl = json{{"startUrl", url}};
            if (options.is_object()) {
                for (auto it = options.begin(); it != options.end(); ++it) {
                    crawl[it.key()] = it.value();
                }
            }
            json result = pool_->getActive()->runtime()->onlineLookup(url, json{{"mode", "crawl"}, {"crawl", crawl}});
            json ingested = nullptr;
            if (payload.value("ingest", false)) {
                json details = json::array();
                int docsCount = 0;
                for (auto &p : result.value("pages", json::array())) {
                    std::string text = p.value("text", "");
                    if (text.empty()) continue;
                    std::string source = payload.value("source", p.value("url", "crawl"));
                    json doc{{"text", text}, {"source", source}};
                    std::string gid = payload.value("groupId", "");
                    json r = gid.empty() ? pool_->ingestDocument(doc) : pool_->ingestDocumentToGroup(gid, doc);
                    details.push_back(json{{"source", source}, {"result", r}});
                    docsCount++;
                    if (docsCount >= 20) break;
                }
                ingested = json{{"docs", docsCount}, {"details", details}};
            }
            respondJson(cb, json{{"ok", true}, {"result", result}, {"ingested", ingested}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/search/config", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                auto runtime = pool_->getActive()->runtime();
                respondJson(cb, json{{"ok", true}, {"config", runtime->getSearchConfig()}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Get});

        drogon::app().registerHandler("/api/search/config", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json payload = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                auto runtime = pool_->getActive()->runtime();
                auto next = runtime->setSearchConfig(payload);
                respondJson(cb, json{{"ok", true}, {"config", next}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Put});

        drogon::app().registerHandler("/api/search/endpoints/add", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json payload = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                std::string url = trimString(payload.value("url", ""));
                if (url.empty()) return badRequest(cb, "url required");
                auto runtime = pool_->getActive()->runtime();
                auto cfg = runtime->getSearchConfig();
                std::unordered_set<std::string> uniq;
                json endpoints = json::array();
                for (auto &e : cfg.value("endpoints", json::array())) {
                    if (e.is_string() && uniq.insert(e.get<std::string>()).second) endpoints.push_back(e);
                }
                if (uniq.insert(url).second) endpoints.push_back(url);
                auto next = runtime->setSearchConfig(json{{"endpoints", endpoints}});
                respondJson(cb, json{{"ok", true}, {"config", next}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/search/endpoints/remove", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json payload = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                std::string url = trimString(payload.value("url", ""));
                if (url.empty()) return badRequest(cb, "url required");
                auto runtime = pool_->getActive()->runtime();
                auto cfg = runtime->getSearchConfig();
                json endpoints = json::array();
                for (auto &e : cfg.value("endpoints", json::array())) {
                    if (e.is_string() && e.get<std::string>() != url) endpoints.push_back(e);
                }
                json patch{{"endpoints", endpoints}};
                if (cfg.value("active", "") == url) patch["active"] = endpoints.empty() ? "" : endpoints[0];
                auto next = runtime->setSearchConfig(patch);
                respondJson(cb, json{{"ok", true}, {"config", next}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/export/graph", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json body = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                json opts{{"seeds", body.value("seeds", json())}, {"radius", body.value("radius", json())}, {"file", body.value("file", json())}};
                auto runtime = pool_->getActive()->runtime();
                auto outFile = runtime->exportGraphToFile(opts);
                auto content = readTextFile(outFile);
                respondJson(cb, json{{"ok", true}, {"file", outFile.filename().string()}, {"path", outFile.string()}, {"content", content}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/export/graph/group", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json payload = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                std::string gid = trimString(payload.value("groupId", ""));
                if (gid.empty()) return badRequest(cb, "groupId required");
                auto list = pool_->listControllersInGroup(gid);
                if (list.empty()) return respondJson(cb, json{{"ok", false}, {"error", "group-not-found"}}, drogon::k404NotFound);
                auto ctrl = pool_->getByName(list.front());
                if (!ctrl) return respondJson(cb, json{{"ok", false}, {"error", "controller-not-found"}}, drogon::k404NotFound);
                json opts{{"seeds", payload.value("seeds", json())}, {"radius", payload.value("radius", json())}, {"file", payload.value("file", json())}};
                auto outFile = ctrl->runtime()->exportGraphToFile(opts);
                auto content = readTextFile(outFile);
                respondJson(cb, json{{"ok", true}, {"groupId", gid}, {"file", outFile.filename().string()}, {"path", outFile.string()}, {"content", content}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/robots/retrain", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json options = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                json files = json();
                if (options.contains("files")) {
                    if (options["files"].is_array()) {
                        files = options["files"];
                    } else if (options["files"].is_string()) {
                        std::string f = trimString(options["files"].get<std::string>());
                        if (!f.empty()) files = json::array({f});
                    }
                }
                bool shuffle = options.contains("shuffle") ? jsTruthy(options["shuffle"]) : false;
                json collect = json::object();
                if (options.contains("limit")) collect["limit"] = options["limit"];
                if (options.contains("offset")) collect["offset"] = options["offset"];
                collect["shuffle"] = shuffle;
                if (files.is_array() && !files.empty()) collect["files"] = files;
                auto runtime = pool_->getActive()->runtime();
                auto docs = runtime->collectRobotsDocuments(collect);
                if (docs.empty()) return respondJson(cb, json{{"ok", false}, {"error", "no-documents"}}, drogon::k404NotFound);
                int ingested = 0; int memes = 0; int edges = 0; int failed = 0;
                auto groups = pool_->listGroupIds();
                for (auto &doc : docs) {
                    json j{{"text", doc.text}, {"tokens", doc.tokens}, {"source", doc.source}, {"file", doc.file}, {"id", doc.id}};
                    std::string sourceKey = !doc.source.empty() ? doc.source : (!doc.file.empty() ? doc.file : doc.id);
                    std::string key = sourceKey + "|" + doc.text.substr(0, 256);
                    int gi = groups.empty() ? 0 : (int)(hashStrSimple(key) % groups.size());
                    std::string target = groups.empty() ? "" : groups[gi];
                    json result = target.empty() ? pool_->ingestDocument(j) : pool_->ingestDocumentToGroup(target, j);
                    ingested++;
                    if (result.is_array()) {
                        for (auto &item : result) {
                            bool okItem = item.is_object() ? item.value("ok", true) : jsTruthy(item);
                            if (okItem && item.is_object()) {
                                memes += item.value("memes", 0);
                                edges += item.value("edges", 0);
                            } else if (!okItem) {
                                failed++;
                            }
                        }
                    } else {
                        bool okItem = result.is_object() ? result.value("ok", true) : jsTruthy(result);
                        if (okItem && result.is_object()) {
                            memes += result.value("memes", 0);
                            edges += result.value("edges", 0);
                        } else if (!okItem) {
                            failed++;
                        }
                    }
                    if (options.value("enqueueStudy", false) && study_) study_->enqueueDocument(j);
                }
                respondJson(cb, json{{"ok", true}, {"ingested", ingested}, {"memes", memes}, {"edges", edges}, {"failedControllers", failed}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/robots/list", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            try {
                auto runtime = pool_->getActive()->runtime();
                respondJson(cb, json{{"ok", true}, {"directory", runtime->config().robotsDir.string()}, {"files", runtime->listRobotsFiles()}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Get});

        drogon::app().registerHandler("/robots/ingest", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            try {
                bool ok = true;
                json options = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                json files = json();
                if (options.contains("files")) {
                    if (options["files"].is_array()) {
                        files = options["files"];
                    } else if (options["files"].is_string()) {
                        std::string f = trimString(options["files"].get<std::string>());
                        if (!f.empty()) files = json::array({f});
                    }
                }
                bool shuffle = options.contains("shuffle") ? jsTruthy(options["shuffle"]) : false;
                json collect = json::object();
                if (options.contains("limit")) collect["limit"] = options["limit"];
                if (options.contains("offset")) collect["offset"] = options["offset"];
                collect["shuffle"] = shuffle;
                if (files.is_array() && !files.empty()) collect["files"] = files;
                auto runtime = pool_->getActive()->runtime();
                auto docs = runtime->collectRobotsDocuments(collect);
                if (docs.empty()) return respondJson(cb, json{{"ok", false}, {"error", "no-documents"}}, drogon::k404NotFound);
                json summary = json::array();
                auto groups = pool_->listGroupIds();
                for (auto &doc : docs) {
                    json j{{"text", doc.text}, {"tokens", doc.tokens}, {"source", doc.source}, {"file", doc.file}, {"id", doc.id}};
                    std::string sourceKey = !doc.source.empty() ? doc.source : (!doc.file.empty() ? doc.file : doc.id);
                    std::string key = sourceKey + "|" + doc.text.substr(0, 256);
                    int gi = groups.empty() ? 0 : (int)(hashStrSimple(key) % groups.size());
                    std::string target = groups.empty() ? "" : groups[gi];
                    json result = target.empty() ? pool_->ingestDocument(j) : pool_->ingestDocumentToGroup(target, j);
                    int docMemes = 0;
                    int docEdges = 0;
                    int docFailed = 0;
                    if (result.is_array()) {
                        for (auto &item : result) {
                            bool okItem = item.is_object() ? item.value("ok", true) : jsTruthy(item);
                            if (okItem && item.is_object()) {
                                docMemes += item.value("memes", 0);
                                docEdges += item.value("edges", 0);
                            } else if (!okItem) {
                                docFailed++;
                            }
                        }
                    } else {
                        bool okItem = result.is_object() ? result.value("ok", true) : jsTruthy(result);
                        if (okItem && result.is_object()) {
                            docMemes += result.value("memes", 0);
                            docEdges += result.value("edges", 0);
                        } else if (!okItem) {
                            docFailed++;
                        }
                    }
                    json item{{"id", doc.id}, {"source", doc.source}, {"file", doc.file}, {"tokens", (int)doc.tokens.size()}, {"memes", docMemes}, {"edges", docEdges}, {"failedControllers", docFailed}};
                    summary.push_back(item);
                    if (options.value("enqueueStudy", false) && study_) study_->enqueueDocument(j);
                }
                std::unordered_set<std::string> seen;
                json filesOut = json::array();
                bool nullAdded = false;
                for (auto &item : summary) {
                    if (item.contains("file") && item["file"].is_string()) {
                        std::string f = item["file"].get<std::string>();
                        if (seen.insert(f).second) filesOut.push_back(f);
                    } else if (!nullAdded) {
                        filesOut.push_back(nullptr);
                        nullAdded = true;
                    }
                }
                respondJson(cb, json{{"ok", true}, {"ingested", (int)summary.size()}, {"files", filesOut}, {"summary", summary}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/tests/list", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                fs::path testsDir = pool_->getActive()->runtime()->config().testsDir;
                std::vector<std::string> names;
                if (fs::exists(testsDir)) {
                    for (auto &entry : fs::directory_iterator(testsDir)) {
                        if (!entry.is_regular_file()) continue;
                        auto ext = entry.path().extension().string();
                        std::string lowerExt;
                        lowerExt.reserve(ext.size());
                        for (char c : ext) lowerExt.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
                        if (lowerExt == ".txt") names.push_back(entry.path().filename().string());
                    }
                }
                std::sort(names.begin(), names.end());
                json files = json::array();
                for (auto &n : names) files.push_back(n);
                respondJson(cb, json{{"ok", true}, {"directory", testsDir.string()}, {"files", files}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Get});

        drogon::app().registerHandler("/api/tests/case", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                bool ok = true;
                json payload = parseRequestBody(req, ok);
                if (!ok) return badRequest(cb, "invalid json");
                std::function<std::string(const json &)> jsStringForJoin;
                std::function<std::string(const json &)> jsStringValue;
                jsStringForJoin = [&](const json &v) -> std::string {
                    if (v.is_null()) return "";
                    if (v.is_string()) return v.get<std::string>();
                    if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
                    if (v.is_number()) return v.dump();
                    if (v.is_array()) {
                        std::string out;
                        bool first = true;
                        for (auto &item : v) {
                            if (!first) out += ",";
                            first = false;
                            out += jsStringForJoin(item);
                        }
                        return out;
                    }
                    return "[object Object]";
                };
                jsStringValue = [&](const json &v) -> std::string {
                    if (v.is_null()) return "null";
                    if (v.is_string()) return v.get<std::string>();
                    if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
                    if (v.is_number()) return v.dump();
                    if (v.is_array()) return jsStringForJoin(v);
                    return "[object Object]";
                };
                json nameVal = payload.contains("name") ? payload["name"] : json();
                json contentVal = payload.contains("content") ? payload["content"] : json();
                std::string nameRaw = trimString(jsTruthy(nameVal) ? jsStringValue(nameVal) : "");
                std::string content = jsTruthy(contentVal) ? jsStringValue(contentVal) : "";
                if (nameRaw.empty()) return badRequest(cb, "name required");
                std::string safe;
                safe.reserve(nameRaw.size());
                for (char c : nameRaw) {
                    if (std::isalnum(static_cast<unsigned char>(c)) || c == '.' || c == '_' || c == '-') safe.push_back(c);
                    else safe.push_back('_');
                }
                auto lower = safe;
                std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                if (lower.size() < 4 || lower.substr(lower.size() - 4) != ".txt") safe += ".txt";
                fs::path testsDir = pool_->getActive()->runtime()->config().testsDir;
                ensureDir(testsDir);
                fs::path filePath = testsDir / safe;
                std::ofstream ofs(filePath, std::ios::binary);
                ofs << content;
                respondJson(cb, json{{"ok", true}, {"file", safe}, {"path", filePath.string()}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/tests/refresh", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                fs::path testsDir = pool_->getActive()->runtime()->config().testsDir;
                bool refreshed = false;
                if (rl_) {
                    try {
                        rl_->refreshTests(testsDir);
                        refreshed = true;
                    } catch (...) {
                        try {
                            rl_->setTestsDir(testsDir);
                            refreshed = true;
                        } catch (...) {}
                    }
                } else {
                    rl_ = std::make_shared<ReinforcementLearner>(pool_, testsDir);
                    refreshed = true;
                }
                respondJson(cb, json{{"ok", true}, {"refreshed", refreshed}, {"testsDir", testsDir.string()}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k500InternalServerError);
            } catch (...) {
                respondJson(cb, json{{"ok", false}, {"error", "internal error"}}, drogon::k500InternalServerError);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/snapshots", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            respondJson(cb, json{{"ok", true}, {"list", snapshots_->list()}});
        }, {drogon::Get});

        drogon::app().registerHandler("/api/snapshots/create", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
            if (!authOK(req)) return unauthorized(cb);
            bool ok = true;
            json payload = parseRequestBody(req, ok);
            if (!ok) return badRequest(cb, "invalid json");
            std::string name = payload.value("name", "manual");
            auto file = snapshots_->create(name);
            respondJson(cb, json{{"ok", true}, {"file", file.filename().string()}});
        }, {drogon::Post});

        drogon::app().registerHandler("/api/snapshots/restore/{1}", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb, const std::string &id) {
            if (!authOK(req)) return unauthorized(cb);
            try {
                snapshots_->restore(id);
                respondJson(cb, json{{"ok", true}});
            } catch (const std::exception &e) {
                respondJson(cb, json{{"ok", false}, {"error", e.what()}}, drogon::k404NotFound);
            }
        }, {drogon::Post});

        drogon::app().registerHandler("/api/snapshots/{1}", [this](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb, const std::string &id) {
            if (!authOK(req)) return unauthorized(cb);
            snapshots_->remove(id);
            respondJson(cb, json{{"ok", true}});
        }, {drogon::Delete});
    }

    void onDialogCompleted(const json &result, const json &payload) {
        try {
            dialogCounters_.total += 1;
            if (!dialogLearningEnabled_) return;

            try {
                json wrapped = json{{"payload", payload}, {"result", result}};
                pool_->getActive()->runtime()->learnFromDialog(wrapped, json());
            } catch (...) {
            }

            try {
                std::string text = payload.value("text", payload.value("message", ""));
                std::string reply = result.value("reply", "");
                std::string graphContext = result.value("graphContext", "");
                if (graphContext.empty()) graphContext = buildGraphContext(result, payload);
                if (!text.empty() && !reply.empty()) {
                    transformer::TrainSample sample{ text, reply, graphContext };
                    std::thread([this, sample]() {
                        std::lock_guard<std::mutex> lock(transformerMu_);
                        transformer_.jointTrain(std::vector<transformer::TrainSample>{sample}, 1, 0.001f, 0.35f);
                    }).detach();
                }
            } catch (...) {
            }

            const auto total = dialogCounters_.total;
            if (!rlDisabled_ && (total - dialogCounters_.lastRL >= dialogThresholds_.rlEvery)) {
                dialogCounters_.lastRL = total;
                std::thread([this]() {
                    try { ensureRL()->learn(1); } catch (...) {}
                }).detach();
            }
            if (!advDisabled_ && (total - dialogCounters_.lastADV >= dialogThresholds_.advEvery)) {
                dialogCounters_.lastADV = total;
                std::string text;
                if (payload.contains("text") && payload["text"].is_string()) {
                    text = payload["text"].get<std::string>();
                } else if (payload.contains("tokens") && payload["tokens"].is_array()) {
                    std::ostringstream oss; bool first = true;
                    for (auto &t : payload["tokens"]) { if (!t.is_string()) continue; if (!first) oss << " "; oss << t.get<std::string>(); first = false; }
                    text = oss.str();
                }
                auto trim = [](std::string s) {
                    auto start = s.find_first_not_of(" \n\r\t");
                    if (start == std::string::npos) return std::string();
                    auto end = s.find_last_not_of(" \n\r\t");
                    return s.substr(start, end - start + 1);
                };
                text = trim(text);
                std::vector<std::string> samples;
                if (!text.empty()) samples.push_back(text);
                if (result.contains("reply") && result["reply"].is_string()) {
                    samples.push_back(result["reply"].get<std::string>());
                }
                if (!samples.empty()) {
                    std::thread([this, samples]() {
                        try { ensureADV()->attackAndDefend(samples); } catch (...) {}
                    }).detach();
                }
            }
        } catch (...) {
        }
    }

    void respondJson(const std::function<void(const drogon::HttpResponsePtr &)> &cb, const json &j, drogon::HttpStatusCode code = drogon::k200OK) {
        auto resp = drogon::HttpResponse::newHttpJsonResponse(toJsoncpp(j));
        resp->setStatusCode(code);
        cb(resp);
    }

    void unauthorized(const std::function<void(const drogon::HttpResponsePtr &)> &cb) {
        if (hasAuthError_) {
            json payload{{"ok", false}, {"error", lastAuthError_.error}};
            if (!lastAuthError_.message.empty()) payload["message"] = lastAuthError_.message;
            respondJson(cb, payload, lastAuthError_.code);
            return;
        }
        respondJson(cb, json{{"ok", false}, {"error", "unauthorized"}}, drogon::k401Unauthorized);
    }

    void badRequest(const std::function<void(const drogon::HttpResponsePtr &)> &cb, const std::string &msg) {
        respondJson(cb, json{{"ok", false}, {"error", msg}}, drogon::k400BadRequest);
    }
};

thread_local GatewayServer::AuthError GatewayServer::lastAuthError_{};
thread_local bool GatewayServer::hasAuthError_ = false;

static int runGroupWorker(const Config &config) {
    std::string groupId = trimString(getEnv("AI_GROUP_ID", "G1"));
    if (groupId.empty()) groupId = "G1";
    int groupSize = std::max(1, (int)numberOr(getEnv("AI_GROUP_SIZE", "7"), 7));
    int port = 0;
    try { port = std::stoi(getEnv("AI_GROUP_WORKER_PORT", "0")); } catch (...) { port = 0; }
    if (port <= 0) return 2;

    fs::path lmdbRoot = getProjectRoot() / "lmdb";
    auto lmdbRootEnv = getEnv("AI_LMDB_ROOT", "");
    if (!lmdbRootEnv.empty()) lmdbRoot = fs::path(lmdbRootEnv);

    std::size_t lmdbMapBytes = 512ull * 1024ull * 1024ull;
    auto raw = getEnv("AI_LMDB_MAP_BYTES", "");
    if (!raw.empty()) {
        lmdbMapBytes = (std::size_t)numberOr(raw, (double)(512ull * 1024ull * 1024ull));
    }
    lmdbMapBytes = std::max<std::size_t>(64ull * 1024ull * 1024ull, lmdbMapBytes);

#ifdef _WIN32
    WSADATA wsaData{};
    WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif

    SocketHandle serverSock = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSock == kInvalidSocket) return 3;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int opt = 1;
#ifdef _WIN32
    setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#else
    setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif
    if (bind(serverSock, (sockaddr *)&addr, sizeof(addr)) != 0) return 4;
    if (listen(serverSock, 1) != 0) return 5;

    SocketHandle client = accept(serverSock, nullptr, nullptr);
    if (client == kInvalidSocket) return 6;

    std::shared_ptr<KeyValueStore> kvmStore;
    std::shared_ptr<KeyValueStore> memeStore;
    std::shared_ptr<KeyValueStore> sessionStore;
#ifdef HAVE_LMDB
    auto lmdbK = std::make_shared<LmdbStore>("kvm", lmdbRoot, lmdbMapBytes);
    auto lmdbM = std::make_shared<LmdbStore>("meme_graph", lmdbRoot, lmdbMapBytes);
    auto lmdbS = std::make_shared<LmdbStore>("session", lmdbRoot, lmdbMapBytes);
    if (lmdbK->ok()) kvmStore = lmdbK;
    if (lmdbM->ok()) memeStore = lmdbM;
    if (lmdbS->ok()) sessionStore = lmdbS;
#endif
    if (!kvmStore) {
        kvmStore = std::make_shared<JsonFileStore>("kvm", lmdbRoot);
        memeStore = std::make_shared<JsonFileStore>("meme_graph", lmdbRoot);
        sessionStore = std::make_shared<JsonFileStore>("session", lmdbRoot);
    }

    auto kvmNs = std::make_shared<NamespacedStore>(kvmStore, groupId + ":kvm");
    auto memeNs = std::make_shared<NamespacedStore>(memeStore, groupId + ":graph");
    auto sessNs = std::make_shared<NamespacedStore>(sessionStore, groupId + ":session");

    std::unordered_map<std::string, std::shared_ptr<RuntimeState>> runtimes;
    std::vector<std::string> controllerNames;
    for (int i = 0; i < groupSize; i++) {
        std::string name = groupId + "_AI" + std::to_string(i + 1);
        controllerNames.push_back(name);
        runtimes[name] = std::make_shared<RuntimeState>(kvmNs, memeNs, sessNs, config);
    }

    std::shared_ptr<MemeBarrier> memeBarrier;
    auto getRuntime = [&](const std::string &name) -> std::shared_ptr<RuntimeState> {
        auto it = runtimes.find(name);
        if (it == runtimes.end()) throw std::runtime_error("controller-not-found:" + name);
        return it->second;
    };

    auto safeCall = [&](const std::function<json()> &fn) -> json {
        try {
            return json{{"ok", true}, {"result", fn()}};
        } catch (const std::exception &e) {
            return json{{"ok", false}, {"error", std::string(e.what())}};
        }
    };

    std::string line;
    while (recvLine(client, line, 600000)) {
        json msg = json::parse(line, nullptr, false);
        if (msg.is_discarded()) continue;
        std::string id = msg.value("id", "");
        std::string cmd = msg.value("cmd", "");
        if (id.empty() || cmd.empty()) continue;

        json reply = safeCall([&]() -> json {
            if (cmd == "respond") {
                auto rt = getRuntime(msg.value("controllerName", ""));
                return rt->processInput(msg.value("payload", json::object()));
            }
            if (cmd == "metrics") {
                auto rt = getRuntime(msg.value("controllerName", ""));
                auto name = msg.value("controllerName", "");
                return json{{"name", name}, {"online", true}, {"health", json{{"status", "ok"}, {"since", nowEpochMs()}, {"failures", 0}}}, {"metrics", rt->metrics()}, {"params", rt->cloneParams()}};
            }
            if (cmd == "cloneParams") {
                auto rt = getRuntime(msg.value("controllerName", ""));
                return rt->cloneParams();
            }
            if (cmd == "applyParams") {
                auto rt = getRuntime(msg.value("controllerName", ""));
                rt->setParams(msg.value("params", json::object()));
                return true;
            }
            if (cmd == "snapshot") {
                auto rt = getRuntime(msg.value("controllerName", ""));
                return rt->toSnapshot();
            }
            if (cmd == "applySnapshot") {
                auto rt = getRuntime(msg.value("controllerName", ""));
                rt->fromSnapshot(msg.value("snapshot", json::object()));
                return true;
            }
            if (cmd == "applySnapshotAll") {
                auto snap = msg.value("snapshot", json::object());
                for (auto &name : controllerNames) {
                    getRuntime(name)->fromSnapshot(snap);
                }
                return true;
            }
            if (cmd == "listMetrics") {
                json out = json::array();
                for (auto &name : controllerNames) {
                    auto rt = getRuntime(name);
                    out.push_back(json{{"name", name}, {"online", true}, {"health", json{{"status", "ok"}, {"since", nowEpochMs()}, {"failures", 0}}}, {"metrics", rt->metrics()}, {"params", rt->cloneParams()}});
                }
                return out;
            }
            if (cmd == "ingestDocument") {
                json results = json::array();
                auto doc = msg.value("doc", json::object());
                for (auto &name : controllerNames) {
                    results.push_back(getRuntime(name)->ingestDocument(doc));
                }
                return results;
            }
            if (cmd == "ingestDocumentTo") {
                auto rt = getRuntime(msg.value("controllerName", ""));
                return rt->ingestDocument(msg.value("doc", json::object()));
            }
            if (cmd == "ingestDocumentToGroup") {
                json results = json::array();
                auto doc = msg.value("doc", json::object());
                for (auto &name : controllerNames) {
                    results.push_back(getRuntime(name)->ingestDocument(doc));
                }
                return results;
            }
            if (cmd == "forgetMemes") {
                json results = json::array();
                auto criteria = msg.value("criteria", json::object());
                for (auto &name : controllerNames) {
                    results.push_back(getRuntime(name)->forgetMemes(criteria));
                }
                return results;
            }
            if (cmd == "runtimeCall") {
                auto rt = getRuntime(msg.value("controllerName", ""));
                std::string method = msg.value("method", "");
                auto args = msg.value("args", json::array());
                if (method == "onlineLookup") {
                    auto a0 = (args.is_array() && args.size() > 0) ? args[0] : json::object();
                    auto a1 = (args.is_array() && args.size() > 1) ? args[1] : json::object();
                    return rt->onlineLookup(a0, a1);
                }
                if (method == "collectRobotsDocuments") {
                    json out = json::array();
                    auto a0 = (args.is_array() && args.size() > 0) ? args[0] : json::object();
                    for (auto &d : rt->collectRobotsDocuments(a0)) {
                        out.push_back(json{{"id", d.id}, {"text", d.text}, {"tokens", d.tokens}, {"source", d.source}, {"file", d.file}});
                    }
                    return out;
                }
                if (method == "listRobotsFiles") return rt->listRobotsFiles();
                if (method == "exportGraphToFile") {
                    auto a0 = (args.is_array() && args.size() > 0) ? args[0] : json::object();
                    auto p = rt->exportGraphToFile(a0);
                    return json{{"path", p.string()}};
                }
                if (method == "getSearchConfig") return rt->getSearchConfig();
                if (method == "setSearchConfig") {
                    auto a0 = (args.is_array() && args.size() > 0) ? args[0] : json::object();
                    return rt->setSearchConfig(a0);
                }
                if (method == "learnFromDialog") {
                    if (args.is_array() && args.size() == 1 && args[0].is_object()) {
                        auto obj = args[0];
                        return rt->learnFromDialog(obj.value("payload", json::object()), obj.value("result", json::object()));
                    }
                    auto a0 = (args.is_array() && args.size() > 0) ? args[0] : json::object();
                    auto a1 = (args.is_array() && args.size() > 1) ? args[1] : json::object();
                    return rt->learnFromDialog(a0, a1);
                }
                throw std::runtime_error("runtimeCall not allowed: " + method);
            }
            if (cmd == "memebarrierStart") {
                double th = msg.value("maliciousThreshold", MODEL_DEFAULTS.maliciousThreshold);
                if (!memeBarrier) {
                    memeBarrier = std::make_shared<MemeBarrier>(getRuntime(controllerNames.front()), 10000, th, 5);
                }
                memeBarrier->setThreshold(th);
                memeBarrier->start();
                auto stats = memeBarrier->getStats();
                return json{{"running", true}, {"threshold", memeBarrier->threshold()}, {"stats", json{{"scans", stats.scans}, {"isolated", stats.isolated}, {"lastScanTime", stats.lastScanTime}, {"lastIsolated", stats.lastIsolated}, {"evaluated", stats.evaluated}, {"candidates", stats.candidates}, {"avgScore", stats.avgScore}, {"maxScore", stats.maxScore}, {"p90Score", stats.p90Score}, {"lastThreshold", stats.lastThreshold}, {"avgDegree", stats.avgDegree}, {"stdDegree", stats.stdDegree}, {"avgOutDegree", stats.avgOutDegree}, {"stdOutDegree", stats.stdOutDegree}, {"scoreHistogram", stats.scoreHistogram}}}};
            }
            if (cmd == "memebarrierStop") {
                if (memeBarrier) memeBarrier->stop();
                return json{{"running", false}};
            }
            if (cmd == "memebarrierStats") {
                if (!memeBarrier) return json{{"running", false}, {"threshold", MODEL_DEFAULTS.maliciousThreshold}, {"stats", nullptr}};
                auto stats = memeBarrier->getStats();
                return json{{"running", memeBarrier->running()}, {"threshold", memeBarrier->threshold()}, {"stats", json{{"scans", stats.scans}, {"isolated", stats.isolated}, {"lastScanTime", stats.lastScanTime}, {"lastIsolated", stats.lastIsolated}, {"evaluated", stats.evaluated}, {"candidates", stats.candidates}, {"avgScore", stats.avgScore}, {"maxScore", stats.maxScore}, {"p90Score", stats.p90Score}, {"lastThreshold", stats.lastThreshold}, {"avgDegree", stats.avgDegree}, {"stdDegree", stats.stdDegree}, {"avgOutDegree", stats.avgOutDegree}, {"stdOutDegree", stats.stdOutDegree}, {"scoreHistogram", stats.scoreHistogram}}}};
            }
            if (cmd == "memebarrierSetThreshold") {
                double th = msg.value("maliciousThreshold", MODEL_DEFAULTS.maliciousThreshold);
                if (!memeBarrier) {
                    memeBarrier = std::make_shared<MemeBarrier>(getRuntime(controllerNames.front()), 10000, th, 5);
                }
                memeBarrier->setThreshold(th);
                return json{{"threshold", memeBarrier->threshold()}};
            }
            throw std::runtime_error("unknown-cmd:" + cmd);
        });

        reply["id"] = id;
        std::string outLine = reply.dump() + "\n";
        sendAll(client, outLine);
    }

    closeSocket(client);
    closeSocket(serverSock);
    return 0;
}

int main(int argc, char **argv) {
    g_selfPath = fs::absolute(fs::path(argv[0])).string();
    const auto args = parseArgs(argc, argv);
    const auto config = loadConfig(argc, argv);

    if (args.count("group-worker")) {
        return runGroupWorker(config);
    }

    ensureDir(config.baseDir);
    ensureDir(config.snapshotDir);
    ensureDir(config.lmdbRoot);
    ensureDir(config.robotsDir);

    std::shared_ptr<KeyValueStore> kvmStore;
    std::shared_ptr<KeyValueStore> memeStore;
    std::shared_ptr<KeyValueStore> sessionStore;

#ifdef HAVE_LMDB
    auto lmdbK = std::make_shared<LmdbStore>("kvm", config.lmdbRoot, config.lmdbMapSizeBytes);
    auto lmdbM = std::make_shared<LmdbStore>("meme_graph", config.lmdbRoot, config.lmdbMapSizeBytes);
    auto lmdbS = std::make_shared<LmdbStore>("session", config.lmdbRoot, config.lmdbMapSizeBytes);
    if (lmdbK->ok()) kvmStore = lmdbK;
    if (lmdbM->ok()) memeStore = lmdbM;
    if (lmdbS->ok()) sessionStore = lmdbS;
#endif
    if (!kvmStore) {
        kvmStore = std::make_shared<JsonFileStore>("kvm", config.lmdbRoot);
        memeStore = std::make_shared<JsonFileStore>("meme_graph", config.lmdbRoot);
        sessionStore = std::make_shared<JsonFileStore>("session", config.lmdbRoot);
    }

    std::shared_ptr<ControllerPoolBase> pool;
    std::shared_ptr<RedisSynchronizer> redisSync;
    std::shared_ptr<RotationManager> rotation;
    std::shared_ptr<StudyEngine> study;

    if (config.groupProc) {
        pool = std::make_shared<GroupProcessPool>(kvmStore, memeStore, sessionStore, config);
        redisSync = std::make_shared<RedisSynchronizer>(pool, config.redisUrl, config.redisChannel);
        std::cout << "[Bootstrap] group-proc enabled: one process per group" << std::endl;
    } else {
        pool = std::make_shared<ControllerPool>(kvmStore, memeStore, sessionStore, config);
        rotation = std::make_shared<RotationManager>(pool);
        rotation->start();
        redisSync = std::make_shared<RedisSynchronizer>(pool, config.redisUrl, config.redisChannel);
        try { redisSync->start(); } catch (...) {}
        study = std::make_shared<StudyEngine>(pool, redisSync);
        study->start();
    }

    auto snapshots = std::make_shared<SnapshotManager>(pool->getActive()->runtime(), config.snapshotDir);

    bool restoredFromSnapshot = false;
    try {
        auto list = snapshots->list();
        std::sort(list.begin(), list.end(), std::greater<>());
        if (!list.empty()) {
            auto snap = snapshots->restore(list[0]);
            restoredFromSnapshot = true;
            if (config.groupProc) {
                if (auto gp = dynamic_cast<GroupProcessPool*>(pool.get())) {
                    try { gp->applySnapshotAll(snap); } catch (...) {}
                }
            }
            if (config.syncStandbyOnBoot) {
                try { pool->getStandby()->applySnapshot(snap); } catch (...) {}
                try { pool->getValidation()->applySnapshot(snap); } catch (...) {}
            }
        }
    } catch (...) {}

    if (config.robotsAutoload && config.robotsWarmupLimit > 0 && !restoredFromSnapshot) {
        try {
            auto docs = pool->getActive()->runtime()->collectRobotsDocuments(json{{"limit", config.robotsWarmupLimit}, {"shuffle", config.robotsWarmupShuffle}});
            auto groups = pool->listGroupIds();
            for (auto &doc : docs) {
                std::string key = (doc.source.empty() ? (doc.file.empty() ? doc.id : doc.file) : doc.source);
                key += "|" + doc.text.substr(0, 256);
                int idx = groups.empty() ? 0 : (int)(hashStrSimple(key) % groups.size());
                std::string target = groups.empty() ? "G1" : (groups[idx].empty() ? (groups.empty() ? "G1" : groups[0]) : groups[idx]);
                std::string source = doc.source.empty() ? (doc.file.empty() ? "robots:unknown" : ("robots:" + doc.file)) : doc.source;
                pool->ingestDocumentToGroup(target, json{{"text", doc.text}, {"tokens", doc.tokens}, {"source", source}, {"file", doc.file}, {"id", doc.id}});
            }
        } catch (...) {}
    }

    if (config.testsAutoload) {
        try {
            fs::path testsDir = config.testsDir;
            if (fs::exists(testsDir)) {
                std::vector<std::string> files;
                for (auto &e : fs::directory_iterator(testsDir)) {
                    if (!e.is_regular_file()) continue;
                    auto ext = e.path().extension().string();
                    std::string lowerExt;
                    lowerExt.reserve(ext.size());
                    for (char c : ext) lowerExt.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
                    if (lowerExt == ".txt") files.push_back(e.path().filename().string());
                }
                auto groups = pool->listGroupIds();
                for (auto &f : files) {
                    std::string text = readTextFile(testsDir / f);
                    std::string key = "tests:" + f;
                    int idx = groups.empty() ? 0 : (int)(hashStrSimple(key) % groups.size());
                    std::string target = groups.empty() ? "G1" : (groups[idx].empty() ? (groups.empty() ? "G1" : groups[0]) : groups[idx]);
                    pool->ingestDocumentToGroup(target, json{{"text", text}, {"source", key}});
                }
            }
        } catch (...) {}
    }

    auto gateway = std::make_shared<GatewayServer>(pool, snapshots, redisSync, rotation, study, config);
    if (config.learningWarmup) {
        std::thread([gateway]() {
            try { gateway->warmupLearning(); } catch (...) {}
        }).detach();
    }
    gateway->listen();
    return 0;
}
