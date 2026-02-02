#include <drogon/drogon.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include "frontend_server.hpp"
#include <jwt-cpp/jwt.h>
#include <unordered_map>
#include <mutex>

namespace fs = std::filesystem;

namespace {
    std::unordered_map<std::string, std::string> userDatabase = {
        {"admin", "password"} // Example user
    };

    std::mutex dbMutex;
    const std::string jwtSecret = "dev-secret-change-me";
}

std::string getEnv(const std::string &name, const std::string &fallback) {
    const char *value = std::getenv(name.c_str());
    return (value == nullptr || std::string(value).empty()) ? fallback : std::string(value);
}

void setupFrontendServer() {
    const std::string webRoot = getEnv("WEB_ROOT", "./079project_frontend/build");
    const std::string host = getEnv("FRONTEND_HOST", "127.0.0.1");
    const int port = std::stoi(getEnv("FRONTEND_PORT", "5081"));

    if (!fs::exists(webRoot)) {
        std::cerr << "[frontend_server] WEB_ROOT does not exist: " << webRoot << std::endl;
        return;
    }

    drogon::app().registerHandler("/", [webRoot](const drogon::HttpRequestPtr &, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
        auto resp = drogon::HttpResponse::newHttpResponse();
        const std::string indexFile = webRoot + "/index.html";

        if (!fs::exists(indexFile)) {
            resp->setStatusCode(drogon::k404NotFound);
            resp->setBody("index.html not found");
        } else {
            std::ifstream file(indexFile);
            std::stringstream buffer;
            buffer << file.rdbuf();
            resp->setStatusCode(drogon::k200OK);
            resp->setContentTypeString("text/html; charset=utf-8");
            resp->setBody(buffer.str());
        }
        cb(resp);
    });

    drogon::app().registerHandler("/auth/login", [](const drogon::HttpRequestPtr &req, std::function<void(const drogon::HttpResponsePtr &)> &&cb) {
        auto resp = drogon::HttpResponse::newHttpResponse();
        try {
            auto json = req->getJsonObject();
            if (!json || !json->isMember("username") || !json->isMember("password")) {
                resp->setStatusCode(drogon::k400BadRequest);
                resp->setBody("Missing username or password");
                cb(resp);
                return;
            }

            const std::string username = (*json)["username"].asString();
            const std::string password = (*json)["password"].asString();

            std::lock_guard<std::mutex> lock(dbMutex);
            auto it = userDatabase.find(username);
            if (it == userDatabase.end() || it->second != password) {
                resp->setStatusCode(drogon::k401Unauthorized);
                resp->setBody("Invalid credentials");
                cb(resp);
                return;
            }

            auto token = jwt::create()
                .set_issuer("frontend_server")
                .set_type("JWT")
                .set_payload_claim("username", jwt::claim(username))
                .sign(jwt::algorithm::hs256{jwtSecret});

            resp->setStatusCode(drogon::k200OK);
            resp->setContentTypeString("application/json");
            resp->setBody("{\"token\": \"" + token + "\"}");
        } catch (const std::exception &e) {
            resp->setStatusCode(drogon::k500InternalServerError);
            resp->setBody(e.what());
        }
        cb(resp);
    });

    drogon::app().setDocumentRoot(webRoot);
    drogon::app().addListener(host, port);

    std::cout << "[frontend_server] Listening on http://" << host << ":" << port << std::endl;
    std::cout << "[frontend_server] webRoot: " << webRoot << std::endl;
}