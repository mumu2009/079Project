# C++ 版 main.cjs（Drogon）

该目录是 `main.cjs` 的 C++ 迁移版本（单可执行文件）。

## 依赖
- Drogon
- nlohmann/json
- jwt-cpp
- redis-plus-plus（可选）
- LMDB（可选）
- gumbo（可选，HTML 解析）
- poppler-cpp（可选，PDF 解析）
- libcurl（可选，HTTP 抓取）

## 构建
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### 依赖安装与路径提示
- 若 CMake 找不到 Drogon/nlohmann_json/jwt-cpp，请设置 `CMAKE_PREFIX_PATH` 或对应的 `*_DIR`（如 `Drogon_DIR`）。
- Linux 示例（以系统包为主）：
	- 安装后可通过 `cmake -S . -B build -DCMAKE_PREFIX_PATH=/usr/local` 指定前缀路径。

### 可选构建开关（与 CMakeLists.txt 对齐）
- `-DENABLE_REDIS=ON|OFF`
- `-DENABLE_LMDB=ON|OFF`
- `-DENABLE_GUMBO=ON|OFF`
- `-DENABLE_POPPLER=ON|OFF`
- `-DENABLE_CURL=ON|OFF`
- `-DBUILD_STATIC=ON|OFF`（仅 Linux 非 macOS 会尝试 `-static`）

## 运行
```bash
./build/phoenix_main --gateway-host=0.0.0.0 --port=5080
```

## 环境变量
与 `main.cjs` 保持一致，示例：
- AI_BASE_DIR
- AI_GATEWAY_HOST
- CONTROLLER_PORT
- AI_STUDY_PORT
- REDIS_URL
- AI_REDIS_CHANNEL
- LMDB_DIR
- AI_LMDB_MAP_MB

## 说明
- 若未安装 LMDB/redis++/gumbo/poppler/libcurl，将自动降级或禁用相应功能。
- 若要求单文件静态链接，需要确保系统上对应依赖均有静态库版本。
